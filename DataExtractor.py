cfg = {
    "TEL_DE_Path": r"C:\Program Files (x86)\Tokyo Electron Limited\ACT-AGC\BIN\DataExtractor.exe",
    "FailureMailRecipients": "bollen98@imec.be,dillem78@imec.be",
    
    "SharePoint_BaseSite": r"imecinternational.sharepoint.com",
    "SharePoint_TargetSite": r"/sites/Team-ToolData",
    "SharePoint_DriveName": r"Documents",
    "SharePoint_TrackPath": r"/TEL_Tracks/",
    "SharePoint_AccessAccount": "appm_con@imecinternational.onmicrosoft.com",
    
    "AD_App_client_id": r"415650c1-1005-494c-b25d-08dc96807886",
    "AD_App_authority": r"https://login.microsoftonline.com/a72d5a72-25ee-40f0-9bd1-067cb5b770d4",
    
    "History_FileBase": "History_of_TR",
    "IdentifiedHistory_FileBase": "Identified_History_of_TR",
    "IdentifiedHistory_FileBase_NewMethod": "Identified_History_NewCount_of_TR",
    "Steps_DB_FileBase": "Steps_DB_of_TR",
    "Year": datetime.date.today().year,
    "Excerpts": "Excerpts_of_TR", # that's the 'base' of the name of the .zip file
    
    "SQL_uname": "fab_prod_reader",
    "SQL_Connection":"Driver={ODBC Driver 17 for SQL Server};"
                     "Server=sqlclusp1_fab.imec.be;"
                     "Database=DWH_PROD;"
                     "UID=_USER_;"
                     "PWD=_PWD_;"
}

cfg["YearSuffix"] = str(cfg["Year"])

tracks = [
    ("3300", "LITHIUS PRO Z"),
    ("3400", "LITHIUS PRO Z 2G")
]

#################################################
try:
    for track in tracks:
        imec_track_name = track[0]
        result = pull_data_from_tel(cfg, track)

except KeyboardInterrupt:
    print("Interrupted.")
except Exception as e:
    error = str(e)
    print("Error happened:\n\n%s" % (error))
else:
    print("Extraction Finished without errors.")
