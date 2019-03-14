from strategy.ncandsbase import NcandsBase
import json

class NcandsState(NcandsBase):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        #config for input map
        with open("inputmap\\NcandsStateConfig.json", 'r') as f:
            try:
                self.InputMap = json.load(f)["InputMap"]
            except:
                pass 

    def ProcessLine(self, line):
        start = 0
        end = 0
        d = {}
        for fld in self.InputMap:
            start = end
            end += fld["Width"]
            if fld["Type"] == "datetime":
                d[fld["Name"]] = self.ParseNcandsDate(line[start:end])
            elif fld["Type"] == "int":
                if "DefaultValue" in fld:
                    fieldValue = self.ParseInt(line[start:end])
                    d[fld["Name"]] =  self.ApplyDefaultIntegerRule(fld, fieldValue)
                else:
                    d[fld["Name"]] = self.ParseNullableInt(line[start:end])
            else:      
                d[fld["Name"]] = self.ParseString(line[start:end])
                
        #add DQ fields
        for field in self.Ncands_DQ_Columns:
            if field == 'DQ_NCANDS_MissingAge':
                d[field] = 0 if d.get('ChAge', None) <= 21 or d.get('ChAge', None) == 77 else 1
            elif field == 'DQ_NCANDS_HasID':
                d[field] = 1 if d.get('ChID', None) is not None else 0
            else:
                d[field] = 0
        #add RptVictim
        d["RptVictim"] =  1 if d.get("MalDeath", None) == 1 or d.get("Mal1Lev", None) in [1, 2, 3] or d.get("Mal2Lev") in [1, 2, 3]or d.get("Mal3Lev") in [1, 2, 3] or d.get("Mal4Lev") in [1, 2, 3] else 0
        return d
    
    def ProcessFile(self, file):
        print('Processing: %s' % (file))
        
        file_data = {}
        #read source file
        with open(self.concatString(self.FileMetaData.get('SourceFolder', None), file), mode="rt") as f:
            pos = 0
            output_line = 0
            for line in f:
                pos+= 1
                if len(line) != 0:
                    d = self.ProcessLine(line)
                    output_line+= 1
                    file_data[output_line] = d

        #transform data
        transformed = self.TransformData(file_data)
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

