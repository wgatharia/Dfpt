from strategy.afcarsbase import AfcarsBase
import json

class AfcarsState(AfcarsBase):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        #config for input map
        with open("inputmap\\AfcarsStateConfig.json", 'r') as f:
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
                d[fld["Name"]] = self.ParseDate(line[start:end])
            elif fld["Type"] == "int":
                if "DefaultValue" in fld:
                    d[fld["Name"]] = self.ParseInt(line[start:end])
                else:
                    d[fld["Name"]] = self.ParseNullableInt(line[start:end])
            else:      
                d[fld["Name"]] = self.ParseString(line[start:end])
        #add age - Must be added before DQ fields
        d["DOBAge"] =self.GetAge(d)

        #add DQ fields
        for field in self.Afcarss_DQ_Columns:
            d[field] = self.GetAfcarsDQ(field, d)
        #add other fields
        
        return d

    def ProcessFile(self, file):
        print('Processing: %s' % (file))
        try:
            file_data = {}
            #read source file
            with open(self.concatString(self.FileMetaData.get('SourceFolder', None), file), mode="rt", encoding="ANSI") as f:
                pos = 0
                output_line = 0
                for line in f:
                    pos+= 1
                    if len(line) != 0 and pos > 2:
                        if line.startswith("$"):
                            break
                        d = self.ProcessLine(line)
                        output_line+= 1
                        file_data[output_line] = d
            #dedupe file_data
            deduped_data = {}
            id_track = {}
            output_line = 0
            for fd in file_data:
                record_id = file_data[fd]['RECNUMBR']
                if record_id not in id_track:
                    output_line += 1
                    deduped_data[output_line] = file_data[fd]
                    id_track[record_id] = None
            #transform data
            transformed = self.TransformData(deduped_data)
            #write deduped file
            self.WriteDetailFile(transformed)
            #partitioned data
            partitioned_data = self.BuildPartitionedData(transformed)
            transformed.clear()      
            #write unduped file
            #WriteDetailFile(file_meta_data)
            self.WriteSummaryFile(partitioned_data, "statistics\\afcarsstatistics.json")
            #build and write column distributions
            self.BuildColumnDistributions(partitioned_data)
            #build and write trend data
            self.BuildTrendData(partitioned_data)
        except Exception as e:
            print(e)     
