from abc import ABC, abstractmethod, abstractproperty
from datetime import datetime
from datetime import timedelta 
import json
import csv
import os
import math

class FileBase(ABC):
    def __init__(self, **kwargs):
        self.FileMetaData = kwargs
        #create export folder if it does not exists
        if not os.path.exists(self.FileMetaData['ExportFolder']):
            os.makedirs(self.FileMetaData['ExportFolder'])

    @abstractmethod
    def ProcessFile(self, file):
        print('process from base')
        pass

    @property
    def FileMetaData(self):
        return self._fileMetaData

    @FileMetaData.setter
    def FileMetaData(self, fileMetaData):
        self._fileMetaData = fileMetaData
    
    @property
    def ReportPeriodEndDate(self):
        periodCode = self.FileMetaData.get('PeriodCode', None)
        fiscalyear = self.FileMetaData.get('FiscalYear', None)

        if periodCode == 'A':
            return "03/31/" + fiscalyear
        return "09/30/" + fiscalyear
    
    @property
    def Transforms(self):
        with open("statistics\\transforms.json", 'r') as f:
            self._transforms = json.load(f)
        f.close()
        return self._transforms

    #This method builds files statistics
    def BuildStatistics(self, source_data, statistics_config_file):
        file_statistics = {}
        field_names = []
        statistics = {}

        with open(statistics_config_file, 'r') as f:
            statisticsMap = json.load(f)["OutputMap"]
        f.close()
        #DQ Fields
        for fld in statisticsMap:
            if fld["Map"].startswith("DQ"):
                statistics[fld["Map"]] = 0
        for key in source_data:
            data = source_data[key]
            for stat in statistics:
                map = stat.replace('Pct', '')
                if map in data:
                    statistics[stat] += data[map]
        totals = len(source_data)
        
        for fld in statisticsMap:
            field_names.append(fld["Map"])
            if fld["Name"] in self.FileMetaData:
                file_statistics[fld["Map"]] = self.FileMetaData[fld["Name"]]
            if fld["Map"] in statistics:
                file_statistics[fld["Map"]] = 0 if statistics[fld["Map"]] == 0 else "{:.2f}".format(round(statistics[fld["Map"]]/totals * 100, 2))

        return  { 'FieldNames': field_names, 'FileStatistics':file_statistics }   
    
    #calculate distribitions
    def CalculateColumnDistributions(self, output, sourceType):
        results = {}
        distributions = {}
        for column in sourceType:
            results[column] = {}
            for line in output:
                data = output[line][column]
                if data is None:
                    data = -2
                if data not in results[column]:
                      results[column][data] = 1
                else:
                    results[column][data] += 1
            
        row = 0
        for key in results:
            result = results[key]
            distributions_for_key = {}

            for r in result:
                _value = self.TransformFieldValues(key, r)
                distributions_for_key[r] = {
                    'DataSourceType': self.FileMetaData['DataSourceType'],
                    'StateNationalCode': self.FileMetaData['StateNationalCode'],
                    'StateCode': self.FileMetaData['StateCode'],
                    'FiscalYear': self.FileMetaData['FiscalYear'],
                    'PeriodCode': self.FileMetaData['PeriodCode'],
                    'PeriodEndDate': self.ReportPeriodEndDate,
                    'Name': key, 
                    'Value': self.TransformFieldValues(key, r), 
                    'Count': result[r], 
                    'Percent': ("%.2f" % round(100*result[r]/len(output), 2))
                }
            #add sorting to dictionary
            for kr in sorted(distributions_for_key.keys()):
                row +=1
                distributions[row] = distributions_for_key[kr] 
        return distributions

    #Write detail file
    def WriteDetailFile(self, transformed):
        
        output = transformed["Output"]
        fieldnames = transformed["FieldNames"]

        #write output
        outputFile = "{0}_{1}_{2}_{3}_{4}_D.csv".format(\
                self.FileMetaData["DataSourceType"],\
                self.FileMetaData["StateNationalCode"],\
                self.FileMetaData["StateCode"],\
                self.FileMetaData["FiscalYear"],\
                self.FileMetaData["PeriodCode"])

        print("Writing detail file ..", outputFile)
        outputFile = self.concatString(self.FileMetaData['ExportFolder'] , outputFile)

        csvfile = open(outputFile, "w", newline='', encoding="utf-8")
        with csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames, extrasaction='ignore')
            writer.writeheader()
            for i in output:
                writer.writerow(output[i])
        
        csvfile.close()
        
    
    #write summary file
    def WriteSummaryFile(self, transformed, statistics_config_file):
        print("Building Statistics ..")
        statistics = self.BuildStatistics(transformed["Output"], statistics_config_file)

        field_names = statistics['FieldNames']
        file_statistics = statistics['FileStatistics']

        print("Writing summary file ..")
        outputFile = "{0}_{1}_{2}_{3}_{4}_S.csv".format(\
            self.FileMetaData["DataSourceType"],\
            self.FileMetaData["StateNationalCode"],\
            self.FileMetaData["StateCode"],\
            self.FileMetaData["FiscalYear"],\
            self.FileMetaData["PeriodCode"])

        outputFile = self.concatString(self.FileMetaData['ExportFolder'] , outputFile)

        statistics_file = open(outputFile, "w", newline='', encoding="utf-8")
        with statistics_file:
            writer = csv.DictWriter(statistics_file, fieldnames=field_names)
            writer.writeheader()
            writer.writerow(file_statistics)
        statistics_file.close()

    #write store measures
    def WriteStoreMeasures(self):
        print('write store measures from base')
        pass

    #write distributions
    def WriteColumnDistibutionsFile(self, distributions, type):    
        field_names = ['DataSourceType', 'StateNationalCode', 'StateCode', 'FiscalYear', 'PeriodCode', 'PeriodEndDate', 'Name', 'Value', 'Count', 'Percent']
        outputFile = "{0}_{1}_{2}_{3}_{4}_{5}.csv".format(\
            self.FileMetaData["DataSourceType"],\
            self.FileMetaData["StateNationalCode"],\
            self.FileMetaData["StateCode"],\
            self.FileMetaData["FiscalYear"],\
            self.FileMetaData["PeriodCode"],
            type)

        print('Writing column distributions file ..', outputFile)
        outputFile = self.concatString(self.FileMetaData['ExportFolder'] , outputFile)
        
        distributions_file = open(outputFile, "w", newline='', encoding="utf-8")
        with distributions_file:
            writer = csv.DictWriter(distributions_file, fieldnames=field_names)
            #writer.writeheader()
            for i in distributions:
                writer.writerow(distributions[i])
        distributions_file.close()
        
   
    #other functions
    def concatString(self, x, y): 
        z = "%s%s" % (x, y) 
        return z
    #function for parsing int
    def ParseInt(self, i):
        try:
            i = int(i)
        except:
            return -2
        return i
    #function for parsing nullable int
    def ParseNullableInt(self, i):
        try:
            i = int(i)
        except:
            return None
        return i
    #function for parsing strings
    def ParseString(self, i):
        if len(i.replace(' ', '')) == 0:
            return None
        return i

    #function to transform statistics
    def TransformFieldValues(self, field, value):

        data = self.Transforms.get(field, None)
        if data is not None:
            transform = data.get(str(value), None)
            if transform is not None:
                return transform
        return value