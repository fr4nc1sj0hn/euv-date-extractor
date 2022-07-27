import io
import pandas as pd
import numpy as np
from Support import get_SP_drive, save_file_to_SP_folder
import sys

def gather_statistics_from_steps_DB(cfg, track, drive):

    def identify_pre_post_exposure(df):
        df["PreExp"] = True
        if "EIF" in df.ModuleName.values:
            eif_index = df.loc[df.ModuleName == "EIF"].index[0]
            df.loc[eif_index:,"PreExp"] = False
        return(df)

    def compute_time(df):
        pre_exp = df.loc[df.PreExp].FullProcessing.sum() / 60 / 60
        post_exp = df.loc[~df.PreExp].FullProcessing.sum() / 60 / 60
        return(pd.Series({"PreExp": pre_exp, "PostExp": post_exp}))
    
    
    steps_db_file_template = cfg["SharePoint_TrackPath"] + "<YEAR>" + "/" + cfg["Steps_DB_FileBase"] + track + "_" + "<YEAR>" + ".csv"
    
    dfs = pd.DataFrame()
    dfwt = pd.DataFrame()
    for year in np.arange(2019, cfg["Year"]+1, 1):
        steps_db_file = steps_db_file_template.replace("<YEAR>", str(year))
        
        fl_item = drive.get_item_by_path(steps_db_file)
        ftmp = io.BytesIO()
        fl_item.download(output=ftmp)
        ftmp.seek(0)
        dfi = pd.read_csv(ftmp, usecols=["Wafer Start Time", "GoodLot", "ModuleName", "Block", "InOutDuration", "Wait", "Processing", "InternalWait_Start", "InternalWait_End", "WIP"])
        ftmp.close()
        
        dfi["Block"] = dfi["Block"].astype("int8")
        dfi["Wafer Start Time"] = pd.to_datetime(dfi["Wafer Start Time"])
        
        df_wip1 = dfi.loc[(dfi.WIP == 1)]
        df_wip1 = df_wip1.loc[df_wip1.GoodLot]
        df_wip1 = df_wip1.loc[df_wip1.ModuleName != "EIF"]
        df_wip1 = df_wip1.drop(columns=["Wafer Start Time", "WIP", "GoodLot"])

        dfs = dfs.append(df_wip1)
        dfwt = dfwt.append(dfi[["Wafer Start Time", "ModuleName", "Processing", "Block"]])
        
    dfs = dfs.reset_index(drop=True)

    grp = dfwt.groupby("Wafer Start Time")
    dfwt = grp.apply(identify_pre_post_exposure)
    dfwt = dfwt.loc[dfwt.ModuleName != "EIF"]
    dfwt = dfwt.reset_index(drop=True)

    # initialized dfs, time to gather some statistics

    start = dfs.loc[dfs.ModuleName == "START", "InOutDuration"].median()
    end = dfs.loc[dfs.ModuleName == "END", "Wait"].median()
    trs_wait = dfs.loc[dfs.ModuleName == "TRS", "Wait"].median()
    trs_move = dfs.loc[dfs.ModuleName == "TRS", ["Block", "InOutDuration"]].groupby("Block", as_index=False).median()

    others = dfs.loc[~dfs.ModuleName.isin(["START", "END", "TRS"])]
    others_wait_start = (others.Wait + others.InternalWait_Start).median()
    others_wait_end = others.groupby("ModuleName", as_index=False).InternalWait_End.median()
    others = others_wait_end.rename(columns={"InternalWait_End": "Wait"})
    others.Wait = others.Wait + others_wait_start

    trs = trs_move.rename(columns={"InOutDuration": "Wait"})
    trs.Wait = trs.Wait + trs_wait

    df_trs = dfwt.loc[dfwt.ModuleName == "TRS", ["Wafer Start Time", "ModuleName", "Block", "PreExp"]].merge(trs, on="Block").drop(columns="Block")
    df_modules = dfwt.loc[dfwt.ModuleName != "TRS", ["Wafer Start Time", "ModuleName", "Processing", "PreExp"]].merge(others, on="ModuleName", how="left")
    df_modules.loc[df_modules.ModuleName == "START", "Wait"] = start
    df_modules.loc[df_modules.ModuleName == "END", "Wait"] = end
    df_all = df_modules.append(df_trs)
    df_all["FullProcessing"] = df_all.Processing.fillna(0) + df_all.Wait.fillna(0)
    df_all = df_all.drop(columns=["ModuleName", "Processing", "Wait"])

    grp = df_all.groupby("Wafer Start Time")
    df_tr_usage = grp.apply(compute_time).reset_index(drop=False) # here we computed pre-exposure and post-exposure TR usage with median waiting times
    
    return(df_tr_usage)


def add_another_duration_calculation(cfg, track, df):
    def compute_kd_overlaps(df):
        dfc = df[["id", "PreExpStart", "PostExpEnd"]].sort_values(["PostExpEnd", "PreExpStart"]).reset_index(drop=True)
        EndShift = dfc.PostExpEnd.shift(1)
        dfc["Duration_NormWait [h]"] = (dfc.PostExpEnd - dfc.PreExpStart)/ pd.Timedelta("1h")
        dfc.loc[dfc.PreExpStart < EndShift, "Duration_NormWait [h]"] = (dfc.PostExpEnd - EndShift) / pd.Timedelta("1h")
        dfc = dfc.drop(columns=["PreExpStart", "PostExpEnd"])
        return(dfc)

    drive = get_SP_drive(cfg)
    if drive is None:
        return("Could not connect to SharePoint")
    
    df_tr_usage = gather_statistics_from_steps_DB(cfg, track, drive) # prepare statistics needed for the new compute method
    
    df_new = df.merge(df_tr_usage, on="Wafer Start Time", how="left")
    dfe = df_new.loc[~df_new["Exposure Start Time"].isnull(), ["Exposure Start Time", "Exposure End Time"]].sort_values(["Exposure Start Time", "Exposure End Time"])
    EETs = dfe["Exposure End Time"].shift(1)
    SC_Processing = (dfe["Exposure End Time"] - dfe["Exposure Start Time"]) 
    SC_Processing.loc[dfe["Exposure Start Time"] < EETs] = (dfe["Exposure End Time"] - EETs)
    dfe["SC Processing [h]"] = SC_Processing / pd.Timedelta("1h")

    dfe = df_new.merge(dfe, on=["Exposure Start Time", "Exposure End Time"], how="left").sort_values(["Wafer Start Time", "Wafer End Time", "Exposure Start Time", "Exposure End Time"])
    dfe["PreExpStart"] = dfe["Exposure End Time"] - SC_Processing - pd.to_timedelta(dfe.PreExp, unit="h") # here I define where TR counting should start...
    dfe["PostExpEnd"] = dfe["Exposure End Time"] + pd.to_timedelta(dfe.PostExp, unit="h")

    dfe.loc[dfe.PreExpStart.isnull(), "PreExpStart"] = dfe["Wafer End Time"] - pd.to_timedelta(dfe.PreExp, unit="h")
    dfe.loc[dfe.PostExpEnd.isnull(), "PostExpEnd"] = dfe["Wafer End Time"]

    dfe = dfe.sort_values(["PreExpStart", "PostExpEnd"])
    dfe = dfe.reset_index(drop=True)
    
    dfeind = dfe.reset_index(drop=False).rename(columns={"index": "id"})
    dfeind.KD_Final = dfeind.KD_Final.fillna("")

    kd_grp = dfeind.groupby("KD_Final")
    tr_ovls = kd_grp.apply(compute_kd_overlaps).reset_index(drop=True)

    dfeindm = dfeind.merge(tr_ovls, on="id", how="left")
    dfeindm = dfeindm.drop(columns=["id", "PreExp", "PostExp", "PreExpStart", "PostExpEnd"])
    dfeindm = dfeindm.sort_values(["Lot Start Time", "Wafer Start Time"], ascending=False)
    dfeindm = dfeindm.reset_index(drop=True)
    
    final_file = cfg["SharePoint_TrackPath"] + cfg["YearSuffix"] + "/" + cfg["IdentifiedHistory_FileBase_NewMethod"] + track + "_" + cfg["YearSuffix"] + ".csv"
    save_file_to_SP_folder(drive, final_file, dfeindm.to_csv(index=False).encode())
    return(None)
    