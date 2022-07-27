import io
import os
import pyodbc
import keyring
import msal
import O365
from oauthlib.oauth2 import LegacyApplicationClient
from requests_oauthlib import OAuth2Session
from smtplib import SMTP
from email.message import EmailMessage
from socket import gaierror
from requests.exceptions import HTTPError


def send_failure_mail(cfg, body):
    msg = EmailMessage()
    msg.set_content(body)
    msg["Subject"] = "Data pull from track has failed"
    msg["From"] = "tel_track_data@imec.be"
    msg["To"] = cfg["FailureMailRecipients"]
    try:
        with SMTP("smtpserver.imec.be") as smtp:
            smtp.send_message(msg)
    except gaierror:
        print("Cannot connect to imec SMTP server!")
    
    
def connect_to_SQL(cfg):
    connection_str = cfg["SQL_Connection"]
    replacers = {
        "_USER_": cfg["SQL_uname"],
        "_PWD_": keyring.get_password("SQLAzure", cfg["SQL_uname"])
    }
    for key, val in replacers.items():
        connection_str = connection_str.replace(key, val)
    cnxn = pyodbc.connect(connection_str)
    return(cnxn)
    
    
def connect_to_MS_graph(cfg):
    pwd = keyring.get_password("SharePoint", cfg["SharePoint_AccessAccount"])
    
    app = msal.PublicClientApplication(cfg["AD_App_client_id"], authority=cfg["AD_App_authority"])
    uname_pwd_tocken = app.acquire_token_by_username_password(cfg["SharePoint_AccessAccount"], pwd, scopes=["https://graph.microsoft.com/Sites.ReadWrite.All"])
    oauth_session = OAuth2Session(client=LegacyApplicationClient(cfg["AD_App_client_id"]), token=uname_pwd_tocken)
    
    account = O365.Account(("fake_uname", "fake_pwd")) # O365 does not support uname/pwd authentication
    account.con.token_backend.token = uname_pwd_tocken
    account.con.session = oauth_session
    
    if account.is_authenticated:
        return(account)
    else:
        return(None)
    
    
def connect_to_SP_drive(account, cfg):
    site = account.sharepoint().get_site(cfg["SharePoint_BaseSite"], cfg["SharePoint_TargetSite"])
    
    drives = account.storage(resource = "sites/" + site.object_id).get_drives()
    target_drive = None
    for drive in drives:
        if drive.name == cfg["SharePoint_DriveName"]:
            target_drive = drive
            break
    return(target_drive)
    
    
def get_SP_drive(cfg):
    account = connect_to_MS_graph(cfg)
    if account is None:
        return(None) # likely, authentication error
    else:
        drive = connect_to_SP_drive(account, cfg)
        return(drive)

def save_file_to_SP_folder(drive, path, filecontent):
    folder_is_ready = False
    folder_path = os.path.dirname(path)
    while not folder_is_ready:
        if folder_path in ["/", "\\", ""]:
            folder = drive.get_root_folder()
            folder_is_ready = True
        else:
            try:
                folder = drive.get_item_by_path(folder_path)
            except HTTPError: # no such folder
                folder_path = os.path.dirname(folder_path)
            else:
                folder_is_ready = True
                
    if folder_path != os.path.dirname(path): # means that there was no folder:
        missing_part = os.path.relpath(path, start=os.path.commonpath([path, folder_path]))
        missing_folders = os.path.split(os.path.dirname(missing_part))
        for missing_folder in missing_folders:
            if missing_folder == "": # os.split works in a weird way sometimes
                pass
            else:
                folder = folder.create_child_folder(name = missing_folder) # returns newly created folder

    fname = os.path.basename(path)
    
    if type(filecontent) == bytes:
        ftmp = io.BytesIO()
        stream_size = ftmp.write(filecontent)
        ftmp.seek(0)
    else: # assumption is that this is an io.BytesIO object
        ftmp = filecontent
        stream_size = ftmp.getbuffer().nbytes
        ftmp.seek(0)
    
    result = folder.upload_file(item=None, item_name=fname, stream=ftmp, stream_size=stream_size)
    ftmp.close()
    return(result)
