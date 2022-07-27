from Support import send_failure_mail
from ExtractTrackExcerpts import pull_data_from_tel
from MergeTrackExcerptsSP import control_merge
from IdentifyProcessedWafersSP import control_identification
from AnotherDurMethod import add_another_duration_calculation
from pandas.core.frame import DataFrame as df_type
import datetime

#################################################
# Configuration

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
        if result is not None:
            send_failure_mail(cfg, "TEL Data extractor failed to extract data for TR%s:\n%s" % (imec_track_name, result))
        else:
        
            print("Merging excerpts for TR%s..." % imec_track_name)
            result = control_merge(cfg, imec_track_name)
            if result is not None:
                if type(result) != df_type:
                    send_failure_mail(cfg, "Merger failed to merge data for TR%s:\n%s" % (imec_track_name, result))
                else:
                    # df_hl is returned in result, this is the most important High-Level summary
                    
                    print("Identifying wafers / lots for data extracted from TR%s..." % imec_track_name)
                    result = control_identification(cfg, imec_track_name, result)
                    if result is not None:
                        if type(result) != df_type:
                            send_failure_mail(cfg, "Identificator failed to identify wafers for TR%s:\n%s" % (imec_track_name, result))
                        else:
                            # df with identified wafers is returned as result
                            
                            print("Computing duration in another way for TR%s..." % imec_track_name)
                            result = add_another_duration_calculation(cfg, imec_track_name, result)
                            if result is not None:
                                send_failure_mail(cfg, "Failed to compute duration with the new method for TR%s:\n%s" % (imec_track_name, result))
                    else:
                        pass # nothing new
            else:
                pass # there were no new excerpts extracted from the TEL software
        print("Done with TR%s.\n" % (imec_track_name))
except KeyboardInterrupt:
    print("Interrupted.")
except Exception as e:
    error = str(e)
    send_failure_mail(cfg, "Weird error happened while processing TR%s:\n\n%s" % (imec_track_name, error))
    print("Error happened:\n\n%s" % (error))
else:
    print("Finished without errors.")
