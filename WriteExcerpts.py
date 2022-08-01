import os
import io
from zipfile import ZipFile, ZIP_DEFLATED
from Support import get_SP_drive, save_file_to_SP_folder, connect_to_SQL
from requests.exceptions import HTTPError
import shutil

def WriteToSharePoint(cfg,track):
    excerpts_file = cfg["SharePoint_TrackPath"] + cfg["YearSuffix"] + "/" + cfg["Excerpts"] + track + "_" + cfg["YearSuffix"] + ".zip"
        
    drive = get_SP_drive(cfg)

    try:
        zip_archive_SP = drive.get_item_by_path(excerpts_file)
    except HTTPError as e:
        status_code = e.response.status_code
        if status_code == 404: # no such file
            zip_archive_io = io.BytesIO()            # need to create the new archive
        else:
            return("Could not fetch %s file. HTTP error %d." % (excerpts_file, status_code))
    else:
        zip_archive_io = io.BytesIO()
        zip_archive_SP.download(output=zip_archive_io)
        zip_archive_io.seek(0)
        
    with ZipFile(zip_archive_io, "a", compression=ZIP_DEFLATED) as zipf:
        files_in_archive = zipf.namelist()
        for entry in os.scandir():
            if entry.name.endswith("(Extract).csv") and entry.name.startswith("Track_" + track + "_History_"):
                if entry.name not in files_in_archive:
                    zipf.write(entry.path, entry.name)
                os.remove(entry.path)
    save_file_to_SP_folder(drive, excerpts_file, zip_archive_io)


def WriteExcerptsToLocal(sourceFolder, folderPath,imec_track_name):
    src_path = sourceFolder
    trg_path = folderPath
        
    file_names = os.listdir(src_path)
    
    for file_name in file_names:
        if file_name.endswith("(Extract).csv") and file_name.startswith("Track_" + imec_track_name + "_History_"):
            if os.path.exists(os.path.join(trg_path, file_name)):
                os.remove(os.path.join(trg_path, file_name))
                shutil.move(os.path.join(src_path, file_name),trg_path)

