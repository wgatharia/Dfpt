from strategy.ncandsbase import NcandsBase
import csv
import json

class NcandsNational(NcandsBase):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        #config for input map
        with open("inputmap\\NcandsNationalConfig.json", 'r') as f:
            try:
                self.InputMap = json.load(f)["InputMap"]
            except:
                pass 
    def ProcessFile(self, file):
        print('Processing: %s' % (file))
        file_data = {}
        with open(self.concatString(self.FileMetaData.get('SourceFolder', None), file), mode="rt") as csv_file:
            output_line = 0
            csv_reader = csv.DictReader(csv_file, delimiter=',')
            for line in csv_reader:
                d = self.ProcessLine(line)
                output_line+= 1
                file_data[output_line] = d
        #transform data
        transformed = self.TransformData(file_data, "outputmap\\NcandsNationalConfig.json")
        #write deduped file
        self.WriteDetailFile(transformed)
        #write summary
        self.WriteSummaryFile(transformed, "statistics\\ncandsstatistics.json")
        #build and write column distributions
        self.BuildColumnDistributions(transformed)       

    def DictionaryGetCaseInsensitiveField(self, line, name):
        for key in line.keys():
            if key.upper() == name.upper():
                return line[key]
        return None

    def  ProcessLine(self, line):
        d = {}
        for fld in self.InputMap:
            #read field data.
            fieldData = self.DictionaryGetCaseInsensitiveField(line, fld["Name"])
            #validate field data.
            if fieldData is not None and len(fieldData) > 0:
                if fld["Type"] == "datetime":
                    d[fld["Name"]] = self.ParseNcandsDate(fieldData)
                elif fld["Type"] == "int":
                    if "DefaultValue" in fld:
                        d[fld["Name"]] = self.ParseInt(fieldData)
                    else:
                        d[fld["Name"]] = self.ParseNullableInt(fieldData)
                else:      
                    d[fld["Name"]] = self.ParseString(fieldData)
            else:
                if fld["Type"] == "datetime":
                    d[fld["Name"]] = ""
                elif fld["Type"] == "int":
                    if "DefaultValue" in fld:
                        d[fld["Name"]] = int(fld["DefaultValue"])
                    else:
                        d[fld["Name"]] = fld["Value"]
                else:
                    d[fld["Name"]] = fld["Value"]

        #add DQ fields
        for field in self.Ncands_DQ_Columns:
            d[field] = self.GetNcandsDQ(field, d)
        #add  missing
            d["ChBdate"] = None
            d["IncidDt"] = None
            d["RptDispDt"] = None
            d["SuprvID"] = None
            d["WrkrID"] = None
            d["RptCnty"] = self.GetRptCnty(line["RptCnty"])
        return d