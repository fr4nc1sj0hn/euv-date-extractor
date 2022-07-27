import io
import saspy
import getpass
import keyring
import pandas as pd
import numpy as np
from fuzzywuzzy import process as fuzzyprocess
from Support import get_SP_drive, save_file_to_SP_folder
from pandas.core.frame import DataFrame as df_type
from requests.exceptions import HTTPError

def find_new_entries(df, final_file, drive):
    try:
        fl_item = drive.get_item_by_path(final_file)
    except HTTPError as e:
        status_code = e.response.status_code
        if status_code == 404: # no such file
            dfa = None
        else:
            return("Could not fetch %s file. HTTP error %d." % (final_file, status_code), None)
    else:    
        times = [
                    "Wafer Start Time",
                    "Wafer End Time",
                    "Lot Start Time",
                    "Lot End Time",
                    "Exposure Start Time",
                    "Exposure End Time",
                    "DT_FOUP",
                    "DT_Lot"
                ]

        ftmp = io.BytesIO()
        fl_item.download(output=ftmp)
        ftmp.seek(0)
        dfa = pd.read_csv(ftmp, dtype=str)
        ftmp.close()

        for time in times:
            dfa[time] = pd.to_datetime(dfa[time])
        dfa.Slot = pd.to_numeric(dfa.Slot)
        
        a_clmns_order = dfa.columns
        dfa = dfa.set_index(["FOUP", "Slot", "Wafer Start Time"])
        
        clmns_order = df.columns
        df = df.set_index(["FOUP", "Slot", "Wafer Start Time"])
        index_dif = df.index.difference(dfa.index)
        
        if index_dif.empty:
            df = pd.DataFrame() # an empty DF
        else:
            df = df.loc[index_dif]
            # print("Adding %d entries" % (len(df)))   # this eases the debug
        
            df = df.reset_index()
            df = df[clmns_order]
        
            dfa = dfa.reset_index()
            dfa = dfa[a_clmns_order]
    
    return(df, dfa)

def control_identification(cfg, track, df):
    
    
    drive = get_SP_drive(cfg)
    if drive is None:
        return("Could not connect to SharePoint")
        
    final_file = cfg["SharePoint_TrackPath"] + cfg["YearSuffix"] + "/" + cfg["IdentifiedHistory_FileBase"] + track + "_" + cfg["YearSuffix"] + ".csv"

    df, dfa = find_new_entries(df, final_file, drive)
    if type(df) != df_type:
        return(df)

    if len(df) > 0:
        uname = getpass.getuser()
        pwd = keyring.get_password("MES", uname)
        sas = saspy.SASsession(cfgname="iom_app1", omruser=uname, omrpw=pwd)

        dfs = df[["FOUP", "Slot", "Lot Start Time"]]
        dfs = dfs.rename(columns={"Lot Start Time": "DT"})

        sas.df2sd(dfs, table="FSI")
        sas.submit("""
            PROC STP
                PROGRAM='/Shared Data/70 SP Public/PT/LWFS_SQL'
                ODSOUT=REPLAY;
                INPUTDATA data=FSI;
                OUTPUTDATA outdata=TEST;
            RUN;
        """)
        out = sas.sd2df("TEST")

        dfm = df.merge(out, how="left",
                    left_on=["Lot Start Time", "FOUP", "Slot"],
                    right_on=["DT_Observation", "FOUP", "Slot"],
                    suffixes=("", "_sas"))
        dfm = dfm.drop(columns=["DT_Observation"])
        
        dfm = dfm.sort_values(["Lot Start Time", "Wafer Start Time"])
        
        dfp = dfm[dfm.WBS.isnull() & ~(dfm["Lot regexp"].isnull() | (dfm["Lot regexp"] == ""))]
        dfp = dfp[["Lot Start Time", "Lot regexp"]]
        dfp = dfp.rename(columns={"Lot regexp": "Lot"})
        
        dfp["DT"] = dfp["Lot Start Time"]###.dt.round("15min") # to reduce table size
        df_to_sas = dfp.drop_duplicates(subset=["DT", "Lot"])
        
        if not df_to_sas.empty:
            sas.df2sd(df_to_sas, table="FSI")
            sas.submit("""
                PROC STP
                    PROGRAM='/Shared Data/70 SP Public/PT/Lot_WBS_SQL'
                    ODSOUT=REPLAY;
                    INPUTDATA data=FSI;
                    OUTPUTDATA outdata=TEST;
                RUN;
            """)
            out = sas.sd2df("TEST")
            
            df_extracted = dfp.merge(out, on=["DT", "Lot"])
            df_extracted = df_extracted.drop(columns=["DT"])
            df_extracted = df_extracted.drop_duplicates()

            dfp["id"] = dfp.index
            dfpa = dfp.merge(df_extracted, on=["Lot Start Time", "Lot"], suffixes=("", "_sas"))
            dfpa = dfpa.set_index(dfpa.id)

            dfm.loc[dfpa.id, "Route"] = dfpa.loc[dfpa.id, "Route"]
            dfm.loc[dfpa.id, "WBS"] = dfpa.loc[dfpa.id, "WBS"]
            
        sas.endsas()
        
        dfm.loc[~dfm.Lot.isnull(), "Lot_Final"] = dfm.Lot
        dfm.loc[dfm.Lot.isnull(), "Lot_Final"] = dfm["Lot regexp"]

        dfm["WBS"] = dfm["WBS"].fillna("").astype(str)
        kp_kd = dfm.WBS.str.split("/", expand=True)
        if len(kp_kd.columns) > 1:
            dfm.loc[dfm.WBS != "", "KD_Final"] = kp_kd[1]
            dfm.loc[dfm.WBS == "", "KD_Final"] = dfm["KD regexp"]
            AR_lots_mask = dfm.Lot_Final.str.startswith("AR") & ~dfm.WBS.isnull() & ~dfm["KD regexp"].isnull()
            dfm.loc[AR_lots_mask, "KD_Final"] = dfm["KD regexp"]
            
            TEL_KDs = dfm.loc[(dfm.KD_Final == "00962") & (~dfm["KD regexp"].isnull()), "KD regexp"]
            match = pd.DataFrame(fuzzyprocess.extract("31021", TEL_KDs, limit=len(TEL_KDs)), columns=["KDR", "Score", "id"])
            good_match = match.loc[match.Score >= 80]
            dfm.loc[good_match.id.values, "KD_Final"] = "31021"
        else:
            dfm["KD_Final"] = dfm["KD regexp"]
            
        dfm.loc[(dfm.KD_Final == "") & (dfm["Recipe class"] == "INPRIA") & (dfm["Lot Name"].str.contains("ML") | dfm["Job Name"].str.contains("ML")), "KD_Final"] = "31247" # special trick for Inpria
        
        if dfa is not None:
            dfr = dfa.append(dfm, sort=False)
        else:
            dfr = dfm
            
        dfr = dfr.reset_index(drop=True)

        dfr = dfr.sort_values(["Wafer End Time", "Wafer Start Time"])
        WaferEndTimeShift = dfr["Wafer End Time"].shift(1)
        new_batch = WaferEndTimeShift <= dfr["Wafer Start Time"]
        dfr["Duration [h]"] = (dfr["Wafer End Time"] - WaferEndTimeShift) / pd.Timedelta("1h")
        dfr.loc[0, "Duration [h]"] = (dfr.loc[0, "Wafer End Time"] - dfr.loc[0, "Wafer Start Time"])/ pd.Timedelta("1h")
        dfr.loc[new_batch, "Duration [h]"] = (dfr["Wafer End Time"] - dfr["Wafer Start Time"])/ pd.Timedelta("1h")

        dfr = dfr.sort_values(["Lot Start Time", "Wafer Start Time"], ascending=False)
        dfr = dfr.reset_index(drop=True)
        
        save_file_to_SP_folder(drive, final_file, dfr.to_csv(index=False).encode())
        
        return(dfr)
    else:
        return(None)
