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
        with open(self.concatString(self.FileMetaData.get('SourceFolder', None), file), mode="rt", encoding=self.DefaultEncoding) as csv_file:
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
        #partitioned data
        partitioned_data = self.BuildPartitionedData(transformed)
        transformed.clear()      
        #write summary
        self.WriteSummaryFile(partitioned_data, "statistics\\ncandsstatistics.json")
        #build and write column distributions
        self.BuildColumnDistributions(partitioned_data)       
        #build and write trend data
        self.BuildTrendData(partitioned_data)
        
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
                        fieldValue = self.ParseInt(fieldData)
                        d[fld["Name"]] =  self.ApplyDefaultIntegerRule(fld, fieldValue)
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
        for field in ['ChBdate', 'IncidDt', 'RptDispDt', 'SuprvID', 'WrkrID']:
            if d.get(field, None) is None:
                d[field] = None    
        d["RptCnty"] = self.GetRptCnty(line["RptCnty"])
        #add RptVictim
        d["RptVictim"] =  1 if d.get("MalDeath", None) == 1 or d.get("Mal1Lev", None) in [1, 2, 3] or d.get("Mal2Lev") in [1, 2, 3]or d.get("Mal3Lev") in [1, 2, 3] or d.get("Mal4Lev") in [1, 2, 3] else 0
        return d