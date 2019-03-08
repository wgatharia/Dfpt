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
            return 1 if line.get("ChAge", '') is '' or  line.get("ChAge", None) == 99 else 0
        elif fld == "DQ_NCANDS_HasID":
            return 0 if line.get("ChID", '') is '' else 1
        else:
            return 0

    def GetRptCnty(self, stateTer):
        pass

    def GetAdjustedPerAge(self, age):
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

    # trasform data to output format
    def TransformData(self, deduped_data):
        transformed = {}
        output = {}
        fieldnames = []
        # read output map
        with open("OutputMap\\NcandsStateConfig.json", 'r') as f:
            outputMap = json.load(f)["OutputMap"]
        f.close()

        # output map field names
        for fld in outputMap:
            fieldnames.append(fld["Map"])

        # build output
        for i in deduped_data:
            data = {}
            line = deduped_data[i]
            for fld in outputMap:
                if fld["Map"] in ["ChildGenderCode", "Per1SexID", "Per2SexID", "Per3SexID"]:
                    data[fld["Map"]] = self.GetGenderCode(fld["Name"], line)
                elif fld["Map"] == "AgencyJurisdictionID":
                    data[fld["Map"]] = self.GetStateJurisdictionID(
                        line['StaTerr'])[:2] + line[fld["Name"]]
                elif fld["Map"] in ["Per1AgeID", "Per2AgeID", "Per3AgeID", "Per1Age", "Per2Age", "Per3Age"]:
                    data[fld["Map"]] = self.GetAdjustedPerAge(
                        line[fld["Name"]])
                elif fld["Map"] in ["Per1RelID", "Per2RelID", "Per3RelID"]:
                    data[fld["Map"]] = -2 if line[fld["Name"]] == 99 else line[fld["Name"]]
                elif fld["Map"] == "StateJurisdictionID":
                    data[fld["Map"]] = self.GetStateJurisdictionID(
                        line[fld["Name"]])
                elif fld["Map"] == "ChAgeID" or fld["Map"] == "ChAge":
                    data[fld["Map"]] = -2 if line[fld["Name"]] > 21 and line[fld["Name"]] != 77 and line[fld["Name"]] != 99 else line[fld["Name"]]
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

    # returns jursidcition id from state code. reads from json file
    def GetStateJurisdictionID(self, state_code):
        with open("Data\\jurisdictions.json", 'r') as f:
            jurisdictions = json.load(f)["Jurisdictions"]
        f.close()
        for jurisdiction in jurisdictions:
            if jurisdiction["JurisdictionCode"] == state_code:
                return jurisdiction["JurisdictionId"]
        return "00000"

    # build distributions
    def BuildColumnDistributions(self, transformed):
        output = transformed["Output"]
        # calculate null int distributions
        nullint_distributions = self.CalculateColumnDistributions(
            output, self.NcandsNullIntType)
        self.WriteColumnDistibutionsFile(nullint_distributions, "TNI")

        # calculate null int distributions
        int_distributions = self.CalculateColumnDistributions(
            output, self.NcandsIntType)
        self.WriteColumnDistibutionsFile(int_distributions, "TI")

        # calculate null int distributions
        string_distributions = self.CalculateColumnDistributions(
            output, self.NcandsStringType)
        self.WriteColumnDistibutionsFile(string_distributions, "TS")

    @property
    def NcandsNullIntType(self):
        return ['ChSex', 'RptVictim', 'SubYear']

    @property
    def NcandsIntType(self):
        return ['DQ_NCANDS_MissingAge', 'DQ_NCANDS_HasID', 'RptSrc', 'RptDisp', 'Notifs', 'CEthn', 'ChLvng',
        'ChMal1', 'ChMal2', 'Per1Rel', 'Per2Rel', 'Per1Prnt', 'Per1Age', 'ChAge', 'ChRacAI', 'ChRacAs', 'ChRacBl',
        'ChRacNH', 'ChRacWh', 'ChRacUd', 'CdAlc', 'CdDrug', 'CdRtrd', 'CdEmotnl', 'CdVisual', 'CdLearn', 'CdPhys', 'CdBehav', 'CdMedicl']

    @property
    def NcandsStringType(self):
        return ['RptCnty']
