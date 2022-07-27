import os
import io
import pandas as pd
import numpy as np
from zipfile import ZipFile, ZIP_DEFLATED
from collections import OrderedDict
from Support import get_SP_drive, save_file_to_SP_folder, connect_to_SQL
from pandas.core.frame import DataFrame as df_type
from requests.exceptions import HTTPError

def initialize_df(df):
    recipies = df["Flow Recipe"].str.split("/", expand=True)
    df["Recipe class"] = recipies[2]
    df["Recipe"] = recipies[3]

    foup_wafer = df["SubStrateId"].str.split(".", expand=True)
    df["FOUP"] = foup_wafer[0]
    df["Slot"] = foup_wafer[1].astype("int32")

    df["Wafer Start Time"] = pd.to_datetime(df["Wafer Start Time"], dayfirst=True)
    df["Wafer End Time"] = pd.to_datetime(df["Wafer End Time"], dayfirst=True)
    df["Lot Start Time"] = pd.to_datetime(df["Lot Start Time"], dayfirst=True)
    df["Lot End Time"] = pd.to_datetime(df["Lot End Time"], dayfirst=True)

    df.loc[df["Lot Start Time"] < "2019-11-01", "FOUP"] = df.FOUP.str.replace("E000000000000000", "007760580000IMEC")   # VN fixed the FOUP finally
    
    df = df.drop(columns=["SubStrateId", "Flow Recipe"])
    return(df)

def find_good_lots(df):
    all_good_cr = (df["Wafer Result"] == "2")
    if all_good_cr.all():
        df["GoodLot"] = True
    else:
        df["GoodLot"] = False
    return(df)
def prepare_info_on_steps(df):
    df = df.stack()
    df = df.reset_index(drop=False)
    df = df.drop(columns = ["Tool Name", "Wafer Result", "Lot Name", "Job Name", "Lot Start Time", "Lot End Time", "Recipe class", "Recipe", "FOUP", "Slot"])
    
    for timeclmn in ["InTime", "OutTime", "ProcStartTime", "ProcEndTime"]:
        df[timeclmn] = pd.to_datetime(df[timeclmn], dayfirst=True)
           
    df.loc[df.InTime.isnull(), "InTime"] = df[["Wafer Start Time", "OutTime"]].min(axis=1)
    df.loc[df.OutTime.isnull(), "OutTime"] = df[["Wafer End Time", "InTime"]].max(axis=1)
    df = df.rename(columns={"level_14": "StepNb"})
    df.loc[df.StepNb == 1, "ModuleName"] = "START"
    df.ModuleName = df.ModuleName.replace({"FUST": "END"})
    df.ProcRcp = df.ProcRcp.replace({"///": ""})
    df.BlockModuleNo = df.BlockModuleNo.str.zfill(4)
    df["Block"] = df.BlockModuleNo.str[0:2].astype("int32")
    df["Module"] = df.BlockModuleNo.str[2:].astype("int32")
    df = df.drop(columns = ["BlockModuleNo"])
    return(df)

def process_timings(df):
    # df = df.drop(columns = ["Wafer End Time"])
    df = df.sort_values("StepNb")
    df["InOutDuration"] = (df.OutTime - df.InTime) / pd.Timedelta("1s")
    OutShift = df.OutTime.shift()
    df["Wait"] = (df.InTime - OutShift) / pd.Timedelta("1s")
    df["Processing"] = (df.ProcEndTime - df.ProcStartTime) / pd.Timedelta("1s")
    df["InternalWait_Start"] = (df.ProcStartTime - df.InTime) / pd.Timedelta("1s")
    df["InternalWait_End"] = (df.OutTime - df.ProcEndTime) / pd.Timedelta("1s")
    df = df.drop(columns = ["InTime", "OutTime", "ProcEndTime", "ProcStartTime"])
    df.Wait = df.Wait.fillna(0)
    return(df)

def identify_exposure_step_and_LP(df, all_columns, all_module_name_columns):
    columns_to_drop = [x for x in all_columns if x.startswith("Step")]
    step = None
    
    LP = df["Step1 BlockModuleNo"].str[-1].astype("int")   # this column is always there

    for column in all_module_name_columns:
        no_nan_values = df[column].dropna()
        if len(no_nan_values) > 0:
            if no_nan_values.iloc[0] == "EIF":
                step = column.split()[0]
                break
    else:
        df = df.drop(columns=columns_to_drop)
                                          
                                        
        
    if step is not None:
        start = "%s ProcStartTime" % step
        end = "%s ProcEndTime" % step
        columns_to_drop.remove(start)
        columns_to_drop.remove(end)
        
        df = df.drop(columns=columns_to_drop)
        df = df.rename({start: "Exposure Start Time", end: "Exposure End Time"}, axis="columns")
        df["Exposure Start Time"] = pd.to_datetime(df["Exposure Start Time"], dayfirst=True)
        df["Exposure End Time"] = pd.to_datetime(df["Exposure End Time"], dayfirst=True)
        
        df["Exposure Start Time"] = df["Exposure Start Time"].dt.round("S")
        df["Exposure End Time"] = df["Exposure End Time"].dt.round("S")
    df["Load Port"] = LP
    return(df)

def process_daily_excerpt(df_hl, df_ll, excerpt_path):
    dft = pd.read_csv(excerpt_path, dtype="str")
    if len(dft) > 0:
        dft = initialize_df(dft)

        all_columns = dft.columns.tolist()
        all_module_name_columns = [x for x in all_columns if "ModuleName" in x]
        all_non_steps = [x for x in all_columns if "Step" not in x]
        
        operations = list(OrderedDict.fromkeys([x.split()[1] for x in all_columns if "Step" in x]))
        steps = [int(x.split()[0][4:]) for x in all_module_name_columns]
        col_names = pd.MultiIndex.from_product([operations, steps])

        grp = dft.groupby(["Lot Start Time"], sort=False)
        df_good = grp.apply(find_good_lots)
        if len(df_good) > 0:
            df_good = df_good.reset_index(drop=True)
            df_good = df_good.sort_values("Wafer Start Time")
            grp = df_good.groupby(["Lot Start Time"], sort=False)
            df_good["WaferInLot"] = grp.cumcount() + 1

            all_non_steps.append("WaferInLot")
            all_non_steps.append("GoodLot")
            df_good = df_good.set_index(all_non_steps)
            df_good.columns = col_names
            
            df_steps = prepare_info_on_steps(df_good)
            grp = df_steps.groupby(["Wafer Start Time"], sort=False)
            dfd = grp.apply(process_timings)
        else:
            dfd = pd.DataFrame()
        grp = dft.groupby(["Lot Start Time", "Wafer Result"], sort=False)
        dft = grp.apply(identify_exposure_step_and_LP, all_columns, all_module_name_columns)
        dft = dft.reindex(columns=["Tool Name", "Lot Name", "Job Name", "Wafer Result", "Wafer Start Time", "Wafer End Time", "Lot Start Time", "Lot End Time",
                                    "Recipe class", "Recipe", "Load Port", "FOUP", "Slot", "Exposure Start Time", "Exposure End Time"])

    return(dft, dfd)

def deduct_lot_wbs(dfh, cfg):
    def get_list_of_relevant_kds(cfg):
        cnxn = connect_to_SQL(cfg)
        sql = """
            SELECT DISTINCT
                KD_BK
            FROM
                gen.Activity_Hist_Dim
        """
        df = pd.read_sql(sql,cnxn)
        cnxn.close()
        return(df.KD_BK.values)

    def get_list_of_relevant_lots(cfg):
        cnxn = connect_to_SQL(cfg)
        sql = """
            SELECT DISTINCT
                Lot_Id
            FROM
                fab.W_STATUS_LOT_SECURITY
        """
        df = pd.read_sql(sql,cnxn)
        cnxn.close()
        return(df.Lot_Id.values)

    kds = get_list_of_relevant_kds(cfg)
    all_lots = get_list_of_relevant_lots(cfg)

    dfh = dfh.reset_index(drop=True)
    
    dfh["lot"] = dfh["Lot Name"].str.replace(r"^PJ-.*", "")
    dfh.lot = dfh.lot.str.replace(r"_|\s+", "-")
    dfh.lot = dfh.lot.str.replace(r"=", "-", case=False)
    dfh.lot = dfh.lot.str.replace(r"-(TS?)?\d{1,2}(-|$)", "", case=False)
    dfh.lot = dfh.lot.str.replace(r"-?TS?\d{1,2}$", "", case=False)
    dfh.lot = dfh.lot.str.replace(r"-?TS?-?\d{1,2}to\d{1,2}$", "", case=False)
    dfh.lot = dfh.lot.str.replace(r"-?TEL\d*$", "", case=False)
    dfh.lot = dfh.lot.str.replace(r"-?(D\d{1,8}T?)+$", "", case=False)
    dfh.lot = dfh.lot.str.replace(r"-?S\d{1,2}$", "", case=False)
    dfh.lot = dfh.lot.str.replace(r"-?\d{1,2}WF$", "", case=False)
    dfh.lot = dfh.lot.str.replace(r"-?(D\d{1,8}T?)+$", "", case=False)
    dfh.lot = dfh.lot.str.replace(r"(^|-).{0,4}(-|$)", "-", case=False)
    dfh.lot = dfh.lot.str.replace(r"(^|-)\D+(-|$)", "-", case=False)
    dfh.lot = dfh.lot.str.strip("-")
    
    dfh["sd"] = dfh.lot.str.extract(r"((^|-|F|f)\d{6})", expand=False)[0].str.strip("-")
    dfh.loc[~dfh.sd.isnull(), "fn"] = dfh.loc[~dfh.sd.isnull()].apply(lambda x: x["sd"] in x["FOUP"], axis=1)
    dfh.fn = dfh.fn.replace(True, 1)
    dfh.fn = dfh.fn.replace(False, 0)
    
    dfh.loc[dfh.fn==1, "lot"] = dfh.lot.str.replace(r"((^|-|F)\d{6})", "", case=False)
    temp_lot_ae_names = "AE" + dfh.loc[dfh.fn==0, "lot"]
    valid_lot_ae_names = temp_lot_ae_names.isin(all_lots)
    dfh.loc[(dfh.fn==0) & (valid_lot_ae_names), "lot"] = "AE" + dfh.lot
    
    dfh.lot = dfh.lot.str.replace(r"\D+$", "", case=False)
    dfh.lot = dfh.lot.str.replace(r"TEL.?\d{1,2}$", "", case=False)
    dfh.lot = dfh.lot.str.strip("-")
    dfh.lot = dfh.lot.str.replace(r"-.{0,4}$", "", case=False)
    dfh.lot = dfh.lot.str.replace(r"(^|\W)31199", "", case=False) # that's AP KP
    dfh.lot = dfh.lot.str.strip("-")

    dfh["job"] = dfh["Job Name"].str.replace(r"^CJ-.*", "")
    dfh.job = dfh.job.str.replace(r"_|\s+", "-")
    dfh.job = dfh.job.str.replace(r"=", "-", case=False)
    dfh.job = dfh.job.str.replace(r"-(TS?)?\d{1,2}(-|$)", "", case=False)
    dfh.job = dfh.job.str.replace(r"-?TS?\d{1,2}$", "", case=False)
    dfh.job = dfh.job.str.replace(r"-?TS?-?\d{1,2}to\d{1,2}$", "", case=False)
    dfh.job = dfh.job.str.replace(r"-?TEL\d*$", "", case=False)
    dfh.job = dfh.job.str.replace(r"-?D\d{1,2}$", "", case=False)
    dfh.job = dfh.job.str.replace(r"-?S\d{1,2}$", "", case=False)
    dfh.job = dfh.job.str.replace(r"^-", "", case=False)
    dfh.job = dfh.job.str.replace(r"(^|-).{0,4}(-|$)", "-", case=False)
    dfh.job = dfh.job.str.replace(r"(^|-)\D+(-|$)", "-", case=False)
    dfh.lot = dfh.lot.str.strip("-")
    dfh.job = dfh.job.str.replace(r"(^|\W)31199", "", case=False) # that's AP KP
    dfh.job = dfh.job.str.strip("-")

    dfh = dfh.drop(columns=["sd", "fn"])

    dfh.loc[dfh.lot.str.len() < 5, "lot"] = ""
    dfh.loc[dfh.job.str.len() < 5, "job"] = ""

    lspt = dfh.lot.str.split("-", expand=True)
    try:
        lspt = lspt[[0, 1]]
    except KeyError:
        pass
    else:
        lspt[1] = lspt[1].fillna("")
        dfh.loc[lspt[1].str.match(r"^A\D{1}\d{5,6}(/\d{1,2})?$"), "lot"] = lspt[1]
        dfh.loc[lspt[1].str.match(r"^A\D{1}\d{5,6}(/\d{1,2})?$"), "vl"] = True
        dfh.loc[lspt[1].str.match(r"^\d{5}$"), "kd_l1"] = lspt[1]
    finally:
        dfh.loc[lspt[0].str.match(r"^A\D{1}\d{5,6}(/\d{1,2})?$"), "lot"] = lspt[0]
        dfh.loc[lspt[0].str.match(r"^A\D{1}\d{5,6}(/\d{1,2})?$"), "vl"] = True
        dfh.loc[dfh.vl.isnull(), "lot"] = ""
        dfh.loc[lspt[0].str.match(r"^\d{5}$"), "kd_l0"] = lspt[0]

    jbspt = dfh.job.str.split("-", expand=True)
    try:
        jbspt = jbspt[[0, 1]]
    except KeyError:
        pass
    else:
        jbspt[1] = jbspt[1].fillna("")
        dfh.loc[jbspt[1].str.match(r"^A\D{1}\d{5,6}(/\d{1,2})?$"), "lot"] = jbspt[1]
        dfh.loc[jbspt[1].str.match(r"^\d{5}$"), "kd_j1"] = jbspt[1]
    finally:
        dfh.loc[jbspt[0].str.match(r"^A\D{1}\d{5,6}(/\d{1,2})?$"), "lot"] = jbspt[0]
        dfh.loc[jbspt[0].str.match(r"^\d{5}$"), "kd_j0"] = jbspt[0]
    
    
    l0_candidates = dfh.kd_l0.isin(kds)
    dfh.loc[l0_candidates, "kd"] = dfh.kd_l0
    if "kd_l1" in dfh.columns:
        l1_candidates = dfh.kd_l1.isin(kds)
        dfh.loc[l1_candidates, "kd"] = dfh.kd_l1
        dfh = dfh.drop(columns=["kd_l1"])
        
    j0_candidates = dfh.kd_j0.isin(kds)
    dfh.loc[j0_candidates, "kd"] = dfh.kd_j0
    if "kd_j1" in dfh.columns:
        j1_candidates = dfh.kd_j1.isin(kds)
        dfh.loc[j1_candidates, "kd"] = dfh.kd_j1
        dfh = dfh.drop(columns=["kd_j1"])
    
    dfh.loc[(dfh["Lot Name"].str.contains("TR3300") | dfh["Job Name"].str.contains("TR3300")) & (dfh.lot != ""), "lot"] = dfh.lot + "_TR3300"
    dfh.loc[(dfh["Lot Name"].str.contains("TR3400") | dfh["Job Name"].str.contains("TR3400")) & (dfh.lot != ""), "lot"] = dfh.lot + "_TR3400"
    
    dfh = dfh.drop(columns=["vl", "job", "kd_l0", "kd_j0"])
    LotExists = dfh.lot.isin(all_lots)
    dfh.loc[~LotExists, "lot"] = ""
    dfh = dfh.rename(columns={"lot": "Lot regexp", "kd": "KD regexp"})
    
    return(dfh)

def fill_empty_kd_based_on_batches(df):

    def fill_empty_kd(df):
        s = df["KD regexp"].value_counts(dropna=False)
        lunidentified = s.loc[s.index.isnull()]
        if len(lunidentified) > 0:
            lunidentified = lunidentified.values[0]

            if (lunidentified > 0) & (lunidentified < len(df)):
                next_max = s.index.drop(np.nan)[0]
                df.loc[df["KD regexp"].isnull(), "KD regexp"] = next_max
        return(df)

    FOUPs = df.FOUP.drop_duplicates()
    FOUPs1 = FOUPs.shift(1)
    FOUPs2 = FOUPs.shift(2)
    FOUPs1.name = "FOUPs1"
    FOUPs2.name = "FOUPs2"

    df = df.join(FOUPs1)
    df = df.join(FOUPs2)
    df["FOUPs1t"] = df.FOUP.shift(1)
    df.FOUPs1 = df.FOUPs1.fillna(method="ffill")
    df.FOUPs2 = df.FOUPs2.fillna(method="ffill")

    df["batch_flag"] = (df.FOUP == df.FOUPs1t) | (df.FOUP == df.FOUPs1) | (df.FOUP == df.FOUPs2)
    df = df.reset_index(drop=False)
    df = df.rename(columns={"index": "id"})

    batch_start = df.loc[~df["batch_flag"]]
    batch_start = batch_start.reset_index(drop=True)
    batch_start = batch_start.reset_index(drop=False)
    batch_start = batch_start.rename(columns={"index": "batch"})
    batch_start = batch_start[["batch", "id"]]

    df = df.merge(batch_start, on="id", how="left")
    df.batch = df.batch.fillna(method="ffill")
    df = df.drop(columns=["FOUPs1t", "FOUPs1", "FOUPs2", "batch_flag", "id"])
    df = df.groupby(["batch", "FOUP"], sort=False).apply(fill_empty_kd)
    df = df.drop(columns=["batch"])
    return(df)
    
def merge_with_regular_historical_file(df, history_file, drive):
    try:
        fl_item = drive.get_item_by_path(history_file)
    except HTTPError as e:
        status_code = e.response.status_code
        if status_code == 404: # no such file
            dfh = pd.DataFrame()
        else:
            return("Could not fetch %s file. HTTP error %d." % (history_file, status_code))
    else:
        ftmp = io.BytesIO()
        fl_item.download(output=ftmp)
        ftmp.seek(0)
        dfh = pd.read_csv(ftmp, dtype=str)
        ftmp.close()
        times = ["Wafer Start Time", "Wafer End Time", "Lot Start Time", "Lot End Time", "Exposure Start Time", "Exposure End Time"]
        for time in times:
            dfh[time] = pd.to_datetime(dfh[time])
        dfh["Slot"] = dfh["Slot"].astype("int32")
        
    dfh = df.append(dfh, sort=False)
    dfh = dfh.drop_duplicates(subset=["FOUP", "Slot", "Wafer Start Time"])
    dfh = dfh.reset_index(drop=True)
    dfh = dfh.sort_values(["Lot Start Time", "Wafer Start Time"], ascending=False)
    dfh = dfh.reset_index(drop=True)
    return(dfh)

def merge_with_detailed_historical_file(df, steps_db_file, drive):
    try:
        fl_item = drive.get_item_by_path(steps_db_file)
    except HTTPError as e:
        status_code = e.response.status_code
        if status_code == 404: # no such file
            dfh = pd.DataFrame()
        else:
            return("Could not fetch %s file. HTTP error %d." % (steps_db_file, status_code))
    else:
        ftmp = io.BytesIO()
        fl_item.download(output=ftmp)
        ftmp.seek(0)
        dfh = pd.read_csv(ftmp, dtype=str)
        ftmp.close()
        dfh["Wafer Start Time"] = pd.to_datetime(dfh["Wafer Start Time"])
        dfh["StepNb"] = dfh["StepNb"].astype("int32")
    
    df = df.reset_index(drop=True)
    df = df.sort_values(["Wafer Start Time", "StepNb"])
    df = df.reset_index(drop=True)
        
    dfh = dfh.append(df, sort=False)
    dfh = dfh.drop_duplicates(subset=["Wafer Start Time", "ModuleName", "StepNb"])
    return(dfh)

def add_wip_info(df, dft):
    def find_wip(df):
        wst, wse = df.name
        dfss = (dft["Wafer End Time"] >= wst) & (dft["Wafer End Time"] <= wse)
        df["WIP"] = dfss.sum()
        return(df)
    
    dfo = df[df["Wafer End Time"].isnull()]
    dfn = df[~df["Wafer End Time"].isnull()]
    
    grp = dfn.groupby(["Wafer Start Time", "Wafer End Time"], sort=False)
    dfn = grp.apply(find_wip)
    dfn = dfn.reset_index(drop=True)
    
    df = dfo.append(dfn, sort=False)
    df = df.drop(columns=["Wafer End Time"])
    return(df)

def control_merge(cfg, track):
    df_hl = pd.DataFrame()  # high-level DF (on wafer level)
    df_ll = pd.DataFrame()  # step-level DF

    for entry in os.scandir():
        if entry.name.endswith("(Extract).csv") and entry.name.startswith("Track_" + track + "_History_"):
            df_wafer_level, df_step_details = process_daily_excerpt(df_hl, df_ll, entry.path)
            df_hl = df_hl.append(df_wafer_level, sort=False)
            df_ll = df_ll.append(df_step_details, sort=False)
            
    if (len(df_hl) > 0) or (len(df_ll) > 0):
        history_file = cfg["SharePoint_TrackPath"] + cfg["YearSuffix"] + "/" + cfg["History_FileBase"] + track + "_" + cfg["YearSuffix"] + ".csv"
        steps_db_file = cfg["SharePoint_TrackPath"] + cfg["YearSuffix"] + "/" + cfg["Steps_DB_FileBase"] + track + "_" + cfg["YearSuffix"] + ".csv"
        excerpts_file = cfg["SharePoint_TrackPath"] + cfg["YearSuffix"] + "/" + cfg["Excerpts"] + track + "_" + cfg["YearSuffix"] + ".zip"
        
        drive = get_SP_drive(cfg)
        if drive is None:
            return("Could not connect to SharePoint")
        
        if len(df_hl) > 0:
            df_hl = deduct_lot_wbs(df_hl, cfg)
            df_hl = merge_with_regular_historical_file(df_hl, history_file, drive)
            if type(df_hl) != df_type:
                return(df_hl)
            df_hl = fill_empty_kd_based_on_batches(df_hl)
            save_file_to_SP_folder(drive, history_file, df_hl.to_csv(index=False).encode())
        
        if len(df_ll) > 0:
            df_ll = merge_with_detailed_historical_file(df_ll, steps_db_file, drive)
            df_ll = add_wip_info(df_ll, df_hl)
            if type(df_ll) != df_type:
                return(df_ll)
            save_file_to_SP_folder(drive, steps_db_file, df_ll.to_csv(index=False).encode())
        
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
        
        
    if len(df_hl) > 0:
        return(df_hl)
    else:
        return(None)
