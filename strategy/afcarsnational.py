from strategy.afcarsbase import AfcarsBase
import csv
import json

class AfcarsNational(AfcarsBase):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
            #config for input map
        with open("inputmap\\AfcarsNationalConfig.json", 'r') as f:
            try:
                self.InputMap = json.load(f)["InputMap"]
            except Exception as e:
                print(e)  
    def ProcessFile(self, file):
        file_data = {}
        with open(self.concatString(self.FileMetaData.get('SourceFolder', None), file), mode="rt") as csv_file:
            output_line = 0
            csv_reader = csv.DictReader(csv_file, delimiter=',')
            for line in csv_reader:
                d = self.ProcessLine(line)
                output_line+= 1
                file_data[output_line] = d
        #write A and B files
        period_a_data = {}
        period_b_data = {}

        period_a_line = 0
        period_b_line = 0
        fiscalyear = self.FileMetaData.get('FiscalYear', None)
        if fiscalyear != None:
            fiscalyear = int(fiscalyear)        
        for key in file_data.keys():
            month = file_data.get(key, None).get("REPDATMO", None)
            year = file_data.get(key, None).get("REPDATYR", None)

            if month == 3 and fiscalyear == year:
                period_a_line += 1
                period_a_data[period_a_line] = file_data.get(key, None)
            else:
                period_b_line += 1
                period_b_data[period_b_line] = file_data.get(key, None)
        """
            Period A
        """        
        self.FileMetaData['PeriodCode']  = 'A'
        transformed_a = self.TransformData(period_a_data)
        self.WriteDetailFile(transformed_a)
        #write stats
        self.WriteSummaryFile(transformed_a, "statistics\\afcarsstatistics.json")
        #build and write column distributions
        self.BuildColumnDistributions(transformed_a)
        #build and write period a trend data
        self.BuildTrendData(transformed_a)
        
        """
            Period B
        """
        self.FileMetaData['PeriodCode']  = 'B'
        transformed_b = self.TransformData(period_b_data)
        self.WriteDetailFile(transformed_b)
        #write stats
        self.WriteSummaryFile(transformed_b, "statistics\\afcarsstatistics.json")
        #build and write column distributions
        self.BuildColumnDistributions(transformed_b)
        #build and write period b trend data
        self.BuildTrendData(transformed_b)

    def DictionaryGetCaseInsensitiveField(self, line, name):
        for key in line.keys():
            if key.upper() == name.upper():
                return line[key]
        return None

    def ProcessLine(self, line):
        d = {}
        for fld in self.InputMap:
            #read field data.
            fieldData = self.DictionaryGetCaseInsensitiveField(line, fld["Name"])
            #validate field data.
            if fieldData is not None and len(fieldData) > 0:
                if fld["Type"] == "datetime":
                    d[fld["Name"]] = self.ParseDate(fieldData)
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
                    d[fld["Name"]] = fld["Value"]
                else:
                    d[fld["Name"]] = fld["Value"]
        #add age - Must be added before DQ fields
        d["DOBAge"] =self.GetAge(d)

        #add DQ fields
        for field in self.Afcarss_DQ_Columns:
            d[field] = self.GetAfcarsDQ(field, d)
    
        return d