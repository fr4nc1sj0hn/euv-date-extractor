import datetime
import subprocess

def pull_data_from_tel(cfg, track, start=None, end=None):
    TEL_DE_Path = cfg["TEL_DE_Path"]
    imec_track_name = track[0]
    tel_track_name = track[1]
    start_limit = datetime.date(cfg["Year"], 1, 1)
    
    if end is None:
        end = datetime.date.today()
    if start is None:
        start = end - datetime.timedelta(days=4)
        # start = end - datetime.timedelta(days=1)
    
    start = max(start, start_limit) # this is to ensure that data from the previous year does not go into the next year
    
    step = datetime.timedelta(days=1)

    with open('extractionLog.txt', 'w') as f:
        while start < end:
            start_ingenio_format = start.strftime("%Y/%m/%d")
            end_ingenio_format = (start + datetime.timedelta(days=1)).strftime("%Y/%m/%d")
            print("Current extraction: %s for %s track" % (start_ingenio_format, imec_track_name))
            
            start_filename_format = start.strftime("%Y_%m_%d")
            filename = "Track_%s_History_%s_" % (imec_track_name, start_filename_format)
            
            process = subprocess.Popen(
                [
                   TEL_DE_Path,
                   "-u=IMEC",
                   "-p=IC73",
                   "-SearchDirect",
                   "-CollectMax=1000",
                   "-ToolName=%s" % tel_track_name,
                   "-CollectStart=%s" % start_ingenio_format,
                   "-CollectEnd=%s" % end_ingenio_format,
                   "-e=Dmitry_Exposure",
                   "-f=%s" % filename
                ],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT
            )

            f.write(filename)
            f.write('\n')

            result = process.communicate()[0].decode()

            if not(result.startswith("The target data could not be found") or result.startswith("DataExtractor normal end")):
                return(result)

            start += step
        else:
            return(None)