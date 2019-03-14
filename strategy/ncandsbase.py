from strategy.filebase import FileBase
from datetime import datetime
from datetime import timedelta
import json
import csv
import os


class NcandsBase(FileBase):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    @property
    def Ncands_DQ_Columns(self):
        return ['DQ_NCANDS_MissingAge', 'DQ_NCANDS_HasID', 'DQ_NCANDS_AFCARSMatch', 'DQ_NCANDS_IDMatch', 'DQ_NCANDS_IDMatchChildDiff']

    #rules for data quality
    def GetNcandsDQ(self, fld, line):
        if fld == "DQ_NCANDS_MissingAge":
            return 1 if line.get("ChAge", '') is '' or  line.get("ChAge", None) == 99 or  line.get("ChAge", None) == -2 else 0
        elif fld == "DQ_NCANDS_HasID":
            return 0 if line.get("ChID", '') is '' else 1
        else:
            return 0

    def GetRptCnty(self, rptCnty):
        state = self.StateJurisdictionID[:2]
        if rptCnty == "-1":
            return state + "xxx"
        elif rptCnty == "99":
            return state + "099"
        elif len(rptCnty) == 2:
            return '0' + rptCnty
        elif len(rptCnty) == 1:
            return '00' + rptCnty
        else:
            return rptCnty
            
    def GetAdjustedPerAge(self, age):
        if age is None:
            return -2
        if age > 18 and age < 70:
            return age
        elif age > 0 and age <= 18:
            return 18
        elif age >= 70 and age < 75:
            return 70
        elif age >= 75 and age < 99:
            return 75
        elif age <= 0 or age >= 99:
            return -2

    def ApplyDefaultIntegerRule(self, fld, fieldValue):
        return -2 if ( fieldValue == 99 and fld["Name"] not in ['ChAge', 'Per1Rel', 'Per2Rel'] ) or ( fieldValue == 9 and \
                            fld["Name"] not in [ 'ChSex', 'ChAge', 'Per1Age', 'Per2Age', 'Per3Age', 'RptSrc', 'Notifs', 'CEthn','ChLvng', 
                                                'Per1Rel', 'Per2Rel', 'ChRacAI', 'ChRacAs', 'ChRacBl',  'ChRacNH', 'ChRacWh', 'ChRacUd',
                                                'CdAlc', 'CdDrug', 'CdRtrd', 'CdEmotnl', 'CdVisual', 'CdLearn', 'CdPhys', 'CdBehav', 'CdMedicl' ] ) else fieldValue       
    # trasform data to output format
    def TransformData(self, deduped_data, output_map_file = "OutputMap\\NcandsStateConfig.json"):
        transformed = {}
        output = {}
        fieldnames = []
        # read output map
        with open(output_map_file, 'r') as f:
            outputMap = json.load(f)["OutputMap"]
        f.close()

        # output map field names
        for fld in outputMap:
            if not "Exclude" in fld:
                fieldnames.append(fld["Map"])

        # build output
        for i in deduped_data:
            data = {}
            line = deduped_data[i]
            for fld in outputMap:
                if fld["Map"] in ["ChildGenderCode", "Per1SexID", "Per2SexID", "Per3SexID"]:
                    data[fld["Map"]] = self.GetGenderCode(fld["Name"], line)
                elif fld["Map"] == "AgencyJurisdictionID":
                    data[fld["Map"]] = line[fld["Name"]] if len(line[fld["Name"]]) == 5 else self.StateJurisdictionID[:2] + line[fld["Name"]]
                elif fld["Map"] in ["Per1AgeID", "Per2AgeID", "Per3AgeID", "Per1Age", "Per2Age", "Per3Age"]:
                    data[fld["Map"]] = self.GetAdjustedPerAge(
                        line[fld["Name"]])
                elif fld["Map"] in ["Per1RelID", "Per2RelID", "Per3RelID"]:
                    data[fld["Map"]] = -2 if line[fld["Name"]] == 99 else line[fld["Name"]]
                elif fld["Map"] == "StateJurisdictionID":
                    data[fld["Map"]] = self.StateJurisdictionID
                elif fld["Map"] == "ChAgeID":
                    data[fld["Map"]] = -1 if line[fld["Name"]] == 77 else -2 if line[fld["Name"]] > 21 else line[fld["Name"]]
                elif fld["Map"] in ["CdAlcID", "CdBehavID", "CdDrugID", "CdEmotnlID", "CdLearnID", "CdMediclID", "CdPhysID", "CdRtrdID", "CdVisualID",
                                    "ChMilID", "ChPriorID", "CoChRepID", "EducatnID", "EmployID", "FamPlanID", "FamPresID", "FamSupID", "FCAlcID", "FCDrugID", "FCEmotnlID",
                                    "FCHouseID", "FCLearnID", "FCMediclID", "FCMoneyID", "FCPhysID", "FCPublicID", "FCRtrdID", "FCViolID", "FCVisualID",
                                    "FosterCrID", "HealthID", "HomebaseID", "HousingID", "InfoRefID", "JuvPetID", "LegalID", "MalDeathID", "MentHlthID",
                                    "OtherSvID", "P2RacUdID", "P3RacUdID", "Per1CrID", "Per1EthnID", "Per1MilID", "Per1PiorID", "Per2CrID", "Per2EthnID", "Per2MilID",
                                    "Per2PiorID", "Per3CrID", "Per3EthnID", "Per3MilID", "Per3PiorID", "PostServID", "RespiteID", "SSDelinqID", "SSDisablID", "SubAbuseID",
                                    "TransLivID", "TransprtID"]:
                    data[fld["Map"]] = 0 if line[fld["Name"]] == 2 else - \
                        1 if line[fld["Name"]
                                  ] == 3 else 1 if line[fld["Name"]] == 1 else -2
                elif fld["Map"] in ["ChRacAIID", "ChRacAsID", "ChRacBlID", "ChRacNHID", "ChRacUdID", "ChRacWhID",
                                    "P1RacAIID", "P1RacAsID", "P1RacBlID", "P1RacNHID", "P1RacUdID", "P1RacWhID",
                                    "P2RacAIID", "P2RacAsID", "P2RacBlID", "P2RacNHID", "P2RacUdID", "P2RacWhID",
                                    "P3RacAIID", "P3RacAsID", "P3RacBlID", "P3RacNHID", "P3RacUdID", "P3RacWhID"]:
                    data[fld["Map"]] = 0 if line[fld["Name"]
                                                 ] == 2 else 1 if line[fld["Name"]] == 1 else -2
                elif fld["Map"] in ["CEthnID"]:
                    data[fld["Map"]] = -2 if line[fld["Name"]
                                                  ] == 9 else line[fld["Name"]]
                elif fld["Map"] in ["Per1Mal1ID", "Per1Mal2ID", "Per1Mal3ID", "Per1Mal4ID",
                                    "Per2Mal1ID", "Per2Mal2ID", "Per2Mal3ID", "Per2Mal4ID",
                                    "Per3Mal1ID", "Per3Mal2ID", "Per3Mal3ID", "Per3Mal4ID"]:
                    data[fld["Map"]] = 0 if line[fld["Name"]
                                                 ] == 2 else line[fld["Name"]]
                else:
                    data[fld["Map"]] = line[fld["Name"]]

            output[i] = data

        transformed["Output"] = output
        transformed["FieldNames"] = fieldnames
        return transformed

    # changes gender code

    def GetGenderCode(self, fld, line):
        gender = line[fld]
        if gender == 1:
            return "M"
        if gender == 2:
            return "F"
        return "U"

    # function for parsing dates
    def ParseNcandsDate(self, dt):
        try:

            if len(dt) == 8:
                dt = '%02d%02d%04d' % (
                    int(dt[0:2]), int(dt[2:4]), int(dt[4:8]))
                datetimeobject = datetime.strptime(dt, '%m%d%Y')
                return datetimeobject.strftime('%m/%d/%Y')
            if len(dt) == 10:
                dt = '%04d%02d%02d' % (
                    int(dt[0:4]), int(dt[5:7]), int(dt[8:10]))
                datetimeobject = datetime.strptime(dt, '%Y%m%d')
                return datetimeobject.strftime('%m/%d/%Y')
            return dt
        except:
            return ''

    # build distributions
    def BuildColumnDistributions(self, partitioned):
        # calculate null int distributions
        nullint_distributions = self.CalculateColumnDistributions(
            partitioned, self.NcandsNullIntType)
        self.WriteColumnDistibutionsFile(nullint_distributions, "TNI")

        # calculate null int distributions
        int_distributions = self.CalculateColumnDistributions(
            partitioned, self.NcandsIntType)
        self.WriteColumnDistibutionsFile(int_distributions, "TI")

        # calculate null int distributions
        string_distributions = self.CalculateColumnDistributions(
            partitioned, self.NcandsStringType)
        self.WriteColumnDistibutionsFile(string_distributions, "TS")

    def BuildPartitionedData(self, transformed):
        output = transformed["Output"]
        trend_source = {}
        for key in output:
            data = output[key]
            trend_source[key] = {
                'DataSourceType': data.get("DataSourceType", None),
                'StateJurisdictionID': data.get("StateJurisdictionID", None),
                'StateNationalCode': data.get("StateNationalCode", None),
                'FiscalYear': data.get("FiscalYear", None),
                'PeriodCode': data.get("PeriodCode", None),
                'DQ_NCANDS_MissingAge': data.get("DQ_NCANDS_MissingAge", None),
                'DQ_NCANDS_MissingAgePct': data.get("DQ_NCANDS_MissingAgePct", None),
                'DQ_NCANDS_HasID': data.get("DQ_NCANDS_HasID", None),
                'DQ_NCANDS_HasIDPct': data.get("DQ_NCANDS_HasIDPct", None),
                'ChID': data.get("ChID", None),
                'RptVictim': data.get("RptVictim", None),
                'ChSex': data.get("ChSex", None),
                'SubYr': data.get("SubYr", None),
                'RptSrc': data.get("RptSrc", None),
                'RptDisp': data.get("RptDisp", None),
                'Notifs': data.get("Notifs", None),
                'CEthn': data.get("CEthn", None),
                'ChLvng': data.get("ChLvng", None),
                'ChMal1': data.get("ChMal1", None),
                'ChMal2': data.get("ChMal2", None),
                'Per1Rel': data.get("Per1Rel", None),
                'Per2Rel': data.get("Per2Rel", None),
                'Per1Prnt': data.get("Per1Prnt", None),
                'Per1Age': data.get("Per1Age", None),
                'ChAge': data.get("ChAge", None),
                'ChRacAI': data.get("ChRacAI", None),
                'ChRacAs': data.get("ChRacAs", None),
                'ChRacBl': data.get("ChRacBl", None),
                'ChRacNH': data.get("ChRacNH", None),
                'ChRacWh': data.get("ChRacWh", None),
                'ChRacUd': data.get("ChRacUd", None),
                'CdAlc': data.get("CdAlc", None),
                'CdDrug': data.get("CdDrug", None),
                'CdRtrd': data.get("CdRtrd", None),
                'CdEmotnl': data.get("CdEmotnl", None),
                'CdVisual': data.get("CdVisual", None),
                'CdLearn': data.get("CdLearn", None),
                'CdPhys': data.get("CdPhys", None),
                'CdBehav': data.get("CdBehav", None),
                'CdMedicl': data.get("CdMedicl", None),
                'RptCnty': data.get("RptCnty", None)
            }
        return trend_source

    def BuildTrendData(self, trend_source):
        trend_data = {}
        #calculate chid distributions
        chid_distributions = self.CalculateColumnDistributions(trend_source, ['ChID'])
        unique_child = len(chid_distributions)
        total_child = len(trend_source)
        #calculate Substantiated,Unique victims
        victims = {}
        for i in range(total_child, 0, -1):
            data = trend_source[i]
            chid = data.get('ChID', None)
            if data.get('RptVictim') == 1:
                victims[chid] = chid
                trend_source.pop(i)

        #add trend data.
        trend_data[0] = {
                    'DataSourceType': self.FileMetaData.get('DataSourceType', None),
                    'StateNationalCode': self.FileMetaData.get('StateNationalCode', None),
                    'StateCode': self.FileMetaData.get('StateCode', None),
                    'FiscalYear': self.FileMetaData.get('FiscalYear', None),
                    'PeriodCode': self.FileMetaData.get('PeriodCode', None),
                    'PeriodEndDate': self.ReportPeriodEndDate,
                    'Name': 'Child,Total', 
                    'Value': total_child
        }

        trend_data[1] = {
                    'DataSourceType': self.FileMetaData.get('DataSourceType', None),
                    'StateNationalCode': self.FileMetaData.get('StateNationalCode', None),
                    'StateCode': self.FileMetaData.get('StateCode', None),
                    'FiscalYear': self.FileMetaData.get('FiscalYear', None),
                    'PeriodCode': self.FileMetaData.get('PeriodCode', None),
                    'PeriodEndDate': self.ReportPeriodEndDate,
                    'Name': 'Child,Unique', 
                    'Value': unique_child
        }

        trend_data[2] = {
                    'DataSourceType': self.FileMetaData.get('DataSourceType', None),
                    'StateNationalCode': self.FileMetaData.get('StateNationalCode', None),
                    'StateCode': self.FileMetaData.get('StateCode', None),
                    'FiscalYear': self.FileMetaData.get('FiscalYear', None),
                    'PeriodCode': self.FileMetaData.get('PeriodCode', None),
                    'PeriodEndDate': self.ReportPeriodEndDate,
                    'Name': 'Substantiated,Unique', 
                    'Value': len(victims)
        }
        #write
        self.WriteStoreMeasures(trend_data, '12M')

    @property
    def NcandsNullIntType(self):
        return ['ChSex', 'RptVictim', 'SubYr']

    @property
    def NcandsIntType(self):
        return ['DQ_NCANDS_MissingAge', 'DQ_NCANDS_HasID', 'RptSrc', 'RptDisp', 'Notifs', 'CEthn', 'ChLvng',
        'ChMal1', 'ChMal2', 'Per1Rel', 'Per2Rel', 'Per1Prnt', 'Per1Age', 'ChAge', 'ChRacAI', 'ChRacAs', 'ChRacBl',
        'ChRacNH', 'ChRacWh', 'ChRacUd', 'CdAlc', 'CdDrug', 'CdRtrd', 'CdEmotnl', 'CdVisual', 'CdLearn', 'CdPhys', 'CdBehav', 'CdMedicl']

    @property
    def NcandsStringType(self):
        return ['RptCnty']
