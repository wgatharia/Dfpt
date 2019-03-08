import os
import json
import sys
from strategy.afcarsstate import AfcarsState
from strategy.afcarsnational import AfcarsNational
from strategy.ncandsstate import NcandsState
from strategy.ncandsnational import NcandsNational

#returns jursidcition id from state code. reads from json file
def get_state_jurisdiction_id_from_state_code(state_code):
    with open("Data\\jurisdictions.json", 'r') as f:
        jurisdictions = json.load(f)["Jurisdictions"]
    f.close()    
    for jurisdiction in jurisdictions:
        if jurisdiction["JurisdictionCode"] == state_code:
            return jurisdiction["JurisdictionId"]
    return "00000"

#creates file metadata object
def GetFileMetaData(fileToProcess):
    try:
        file = fileToProcess.split('_')
        if len(file) > 5:
            file = file[:5]
            file_meta_data = { \
                    "DataSourceType": file[0], \
                    "StateNationalCode": file[1], \
                    "StateCode": file[2][-2:], \
                    "FiscalYear": file[3], \
                    "PeriodCode": file[4], \
                    "StateJurisdictionID": int(get_state_jurisdiction_id_from_state_code(file[2][-2:])[:2]) \
                }
            if file[0] == 'AF' and file[1] == 'S':
                file_meta_data['FileType'] = "AfcarsState"
            elif file[0] == 'AF' and file[1] == 'N':
                file_meta_data['FileType'] = "AfcarsNational"
            elif file[0] == 'NC' and file[1] == 'S':
                file_meta_data['FileType'] = "NcandsState"
            elif file[0] == 'NC' and file[1] == 'N':
                file_meta_data['FileType'] = "NcandsNational"
            else:
                raise Exception("Invalid file.{0}".format(fileToProcess))
            return file_meta_data
        else:
            raise Exception("Invalid file.{0}".format(fileToProcess))
    except Exception as e:
         print(e)

#scan folder and process file
def main(sourcePath):
    #source and export folder file to process
    sourceFolder = sourcePath + "\\RawDataFile\\"
    exportFolder = sourcePath + "\\Exported\\Python\\"

    if os.path.exists(sourceFolder):   
        files = os.listdir(sourceFolder)
        for file in files:
            fileToProcess = file.split('_')
            if len(fileToProcess) == 6 and ( len(file.split('.')[0]) == 16 or len(file.split('.')[0]) == 22):
                file_meta_data = GetFileMetaData(file)
                file_meta_data['SourceFolder'] = sourceFolder
                file_meta_data['ExportFolder'] = exportFolder

                if file_meta_data['FileType'] == "AfcarsState":
                    afcars = AfcarsState(**file_meta_data)
                    afcars.ProcessFile(file)
                elif file_meta_data['FileType'] == "AfcarsNational":
                    afcars = AfcarsNational(**file_meta_data)
                    afcars.ProcessFile(file)
                elif file_meta_data['FileType'] == "NcandsState":
                    ncands = NcandsState(**file_meta_data)
                    ncands.ProcessFile(file)
                elif file_meta_data['FileType'] == "NcandsNational":
                    ncands = NcandsNational(**file_meta_data)
                    ncands.ProcessFile(file)
                else:
                     raise Exception("Unsupported file type.{0}".format(file_meta_data['FileType']))
#program entry point
if __name__ == "__main__":
    if len(sys.argv) == 2:
        main(sys.argv[1])
    else:
        print("""
            Usage: python processFiles.py \"D:\\Casey\\RawFileToolGroup\"
            where \"D:\\Casey\\RawFileToolGroup\" is path with files to process.
        """)