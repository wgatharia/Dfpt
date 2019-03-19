from strategy.filebase import FileBase
from datetime import datetime
from datetime import timedelta 
import json
import csv
import os

class AfcarsBase(FileBase):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    @property
    def Afcarss_DQ_Columns(self):
        return ['DQ_DOBgtDtDisch', 'DQ_DOBgtDtLatRem', 'DQ_DtDischEqDtLatRem', 'DQ_DtDischLtDtLatRem', 'DQ_gt21DOBtoDtDisch', 'DQ_gt21DOBtoDtLatRem', 'DQ_gt21DtDischtoDtLatRem', 
                'DQ_missDisreasn', 'DQ_missDOB', 'DQ_missDtLatRem', 'DQ_missNumPlep', 'DQ_totalrem1', 'DQ_missDoDFCDt', 'DQ_ExitDISREASN', 'DQ_InCareU18CASEGOAL', 'DQ_InCareU18CURPLSET', 'DQ_Dropped', 'DQ_IDNoMatchNext6Mo' ]
    #rules for data quality
    def GetAfcarsDQ(self, fld, line):
        if fld == "DQ_DOBgtDtDisch":
            return 0 if line["DoDFCDt"] == "" or line["DOB"] == "" else 1 if datetime.strptime(line["DOB"], "%m/%d/%Y") > datetime.strptime(line["DoDFCDt"], "%m/%d/%Y") else 0
        elif fld == "DQ_DOBgtDtLatRem":
            return 0 if line["LatRemDt"] == "" or line["DOB"] == "" else 1 if datetime.strptime(line["DOB"], "%m/%d/%Y") > datetime.strptime(line["LatRemDt"], "%m/%d/%Y") else 0
        elif fld == "DQ_DtDischEqDtLatRem":
            return 0 if line["DoDFCDt"] == "" or line["LatRemDt"] == "" else 1 if datetime.strptime(line["DoDFCDt"], "%m/%d/%Y") == datetime.strptime(line["LatRemDt"], "%m/%d/%Y") else 0
        elif fld == "DQ_DtDischLtDtLatRem":
            return 0 if line["DoDFCDt"] == "" or line["LatRemDt"] == "" else 1 if datetime.strptime(line["DoDFCDt"], "%m/%d/%Y") < datetime.strptime(line["LatRemDt"], "%m/%d/%Y") else 0
        elif fld == "DQ_gt21DOBtoDtDisch":
            return 0 if line["DoDFCDt"] == "" or line["DOB"] == "" else 1 if datetime.strptime(line["DoDFCDt"], "%m/%d/%Y") > datetime.strptime(line["DOB"], "%m/%d/%Y") + timedelta(days=21*365) else 0
        elif fld == "DQ_gt21DOBtoDtLatRem":
            return 0 if line["LatRemDt"] == "" or line["DOB"] == "" else 1 if datetime.strptime(line["LatRemDt"], "%m/%d/%Y") > datetime.strptime(line["DOB"], "%m/%d/%Y") + timedelta(days=21*365) else 0
        elif fld == "DQ_gt21DtDischtoDtLatRem":
            return 0 if line["DoDFCDt"] == "" or line["LatRemDt"] == "" else 1 if datetime.strptime(line["DoDFCDt"], "%m/%d/%Y") > datetime.strptime(line["LatRemDt"], "%m/%d/%Y") + timedelta(days=21*365) else 0
        elif fld == "DQ_missDisreasn":
            return 1 if line["DISREASN"] == "" and line["DoDFCDt"] != "" else 0 if line["DISREASN"] >= 0 and line["DISREASN"] <= 8 else 1 if line["DISREASN"] == -2 and line["DoDFCDt"] != "" else 0 
        elif fld == "DQ_missDOB":
            return 1 if line["DOB"] == "" else 0
        elif fld == "DQ_missDtLatRem":
            return 1 if line["LatRemDt"] == "" else 0
        elif fld == "DQ_missNumPlep":
            return 1 if line["NUMPLEP"] is None else 0 if line["NUMPLEP"] > 0 else 1
        elif fld == "DQ_totalrem1":
            return 1 if line["TOTALREM"] == 1 else 0
        elif fld == "DQ_missDoDFCDt":
            return 1 if line["DoDFCDt"] == "" else 0
        elif fld == "DQ_ExitDISREASN":
            return line["DISREASN"] if line["DoDFCDt"] != "" else -2
        elif fld == "DQ_InCareU18CASEGOAL":
            return line["CASEGOAL"] if line["DQ_missDOB"] == 0 and line["DQ_missDoDFCDt"] == 0 and line["DOBAge"] < 18 else -2
        elif fld == "DQ_InCareU18CURPLSET":
            return line["CURPLSET"] if line["DQ_missDOB"] == 0 and line["DQ_missDoDFCDt"] == 0 and line["DOBAge"] < 18 else -2
        return 0 
    
    def TransformData(self, deduped_data):
        transformed = {}
        output = {}
        fieldnames = []  
        #read output map
        with open("OutputMap\\AfcarsStateConfig.json", 'r') as f:
            outputMap = json.load(f)["OutputMap"]
        f.close()

        for fld in outputMap:
            if not "Exclude" in fld:
                fieldnames.append(fld["Map"])

        for i in deduped_data:        
            data = {}
            line = deduped_data[i]
            for fld in outputMap:            
                # if fld["Map"].startswith("DQ"):
                #     data[fld["Map"]] = self.GetAfcarsDQ(fld["Name"], line)
                if fld["Map"] == "AgencyJurisdictionID":
                    data[fld["Map"]] = self.GetFipsCode(fld["Name"], line)
                elif fld["Map"] == "IsEverAdoptedID":
                    data[fld["Map"]] = 0 if line[fld["Name"]] == 2 else 3 if line[fld["Name"]] == 0 else 2 if line[fld["Name"]] == 3 else line[fld["Name"]]
                elif fld["Map"] == "IsFCT1HisOrginID":
                    data[fld["Map"]] = 0 if line[fld["Name"]] == 2 else 3 if line[fld["Name"]] == 0 else 2 if line[fld["Name"]] == 3 else line[fld["Name"]]
                elif fld["Map"] == "IsFCT2HisOrginID":
                    data[fld["Map"]] = 0 if line[fld["Name"]] == 2 else 3 if line[fld["Name"]] == 0 else 2 if line[fld["Name"]] == 3 else line[fld["Name"]]
                elif fld["Map"] == "IsHisOrginID":
                    data[fld["Map"]] = 0 if line[fld["Name"]] == 2 else 3 if line[fld["Name"]] == 0 else 2 if line[fld["Name"]] == 3 else line[fld["Name"]]
                elif fld["Map"] == "IsPlacementOutOfStateID":
                    data[fld["Map"]] = 0 if line[fld["Name"]] == 2 else 3 if line[fld["Name"]] == 0 else 2 if line[fld["Name"]] == 3 else line[fld["Name"]]
                elif fld["Map"] == "DiagnosedDisabilityID":
                    data[fld["Map"]] = 0 if line[fld["Name"]] == 2 else 2 if line[fld["Name"]] == 3 else line[fld["Name"]]
                elif fld["Map"] == "GenderCode":
                    data[fld["Map"]] = self.GetGenderCode(fld["Name"], line)
                elif fld["Map"] == "ReportPeriodEndDate":
                    data[fld["Map"]] = self.GetReportPeriodEndDate(line)
                elif fld["Map"] == "StateJurisdictionID":
                    data[fld["Map"]] = self.StateJurisdictionID
                elif fld["Map"] == "DischargeReasonID":
                    data[fld["Map"]] = -2 if line[fld["Name"]] == 99 else line[fld["Name"]]
                elif fld["Map"] == "IsEnteredFosterCareDuringFiscalID" \
                    or fld["Map"] == "IsExitedFosterCareDuringFiscalID" \
                    or fld["Map"] == "IsInFosterCareAtFiscalEndID" \
                    or fld["Map"] == "IsInFosterCareAtFiscalStartID" \
                    or fld["Map"] == "IsParentalRightsTermID" \
                    or fld["Map"] == "IsServedDuringFiscalID" \
                    or fld["Map"] == "IsWaitingForAdoptID":
                    if line.get(fld["Name"], None) is not None:
                        if line[fld["Name"]] == 2:
                            data[fld["Map"]] = None
                        else:
                            data[fld["Map"]] =line[fld["Name"]]
                elif fld["Name"] == "FIPSCODE":
                    data[fld["Map"]] =self.GetFipsCode(fld["Name"], line)
                else:
                    data[fld["Map"]] =line.get(fld["Name"], None)

            output[i] = data
        transformed["Output"] = output
        transformed["FieldNames"] = fieldnames
        return transformed
    
    #gets correct fipscode. adds xxx for missing fips. adds xx8 where fips is 8
    def GetFipsCode(self, fld, line):
        m_state = line["STATE"]
        state = str(m_state)
        if m_state < 10:
            state = "0" + state
        if line[fld] == None:
            return state + "xxx"
        fips_code = line[fld].strip()

        if(fips_code == "8" or fips_code == "00008"):
            return state + "xx8"
        if len(fips_code) == 5:
            return fips_code
        return state + "xxx"
    #changes gender code
    def GetGenderCode(self, fld, line):
        gender = line[fld]
        if gender == 1:
            return "M"
        if gender == 2:
            return "F"
        return "N"
    #creates reporting end date from month and year
    def GetReportPeriodEndDate(self, line):
        month = line["REPDATMO"]
        year = line["REPDATYR"]
        fiscalyear = self.FileMetaData.get('FiscalYear', None)
        if fiscalyear != None:
            fiscalyear = int(fiscalyear)
        if month == 3 and fiscalyear == year:
            return "03/31/" + str(year)
        if month == 3 and fiscalyear < year:
            return "09/30/" + str(fiscalyear)
        return "09/30/" + str(year)

    #function for parsing dates
    def ParseDate(self, dt):
        try:
            
            if len(dt) == 8:
                dt = '%04d%02d%02d' % ( int(dt[0:4]), int(dt[4:6]), int(dt[6:8]) )
                datetimeobject = datetime.strptime(dt,'%Y%m%d')
                return datetimeobject.strftime('%m/%d/%Y')
            if len(dt) == 10:
                dt = '%04d%02d%02d' % ( int(dt[0:4]), int(dt[5:7]), int(dt[8:10]) )
                datetimeobject = datetime.strptime(dt,'%Y%m%d')
                return datetimeobject.strftime('%m/%d/%Y')
            return dt
        except:
            return ''
    #get dobage
    def GetAge(self, line):
        if line["DOB"] != "":
            if line["REPDATMO"] == 3:
                end = datetime.strptime("3/31/" + str(line["REPDATYR"]), "%m/%d/%Y").date() 
            else:
                end = datetime.strptime("9/30/" + str(line["REPDATYR"]), "%m/%d/%Y").date()

            start = datetime.strptime(line["DOB"], "%m/%d/%Y").date()
            _days = (end - start).days
            return round(_days/365)
        return 0
    #set MonthsSinceLatRmDt
    def SetMonthsSinceLatRmDt(self, line):
        if line["DoDFCDt"] != "" and line["LatRemDt"] != "":
            start = datetime.strptime(line["LatRemDt"], "%m/%d/%Y").date()
            end = datetime.strptime(line["DoDFCDt"], "%m/%d/%Y").date()
            return int((end - start).days / 30)
        elif line["DoDFCDt"] != "":
            start = datetime.strptime(line["DoDFCDt"], "%m/%d/%Y").date()
            end = datetime.strptime(self.GetReportPeriodEndDate(line), "%m/%d/%Y").date()
            return int((end - start).days / 30)
        else:
            return 0
    #build distributions
    def BuildColumnDistributions(self, partitioned):
        #calculate null int distributions
        nullint_distributions = self.CalculateColumnDistributions(partitioned, self.AfcarsNullIntType)
        self.WriteColumnDistibutionsFile(nullint_distributions, "TNI")

        #calculate null int distributions
        int_distributions = self.CalculateColumnDistributions(partitioned, self.AfcarsIntType)
        self.WriteColumnDistibutionsFile(int_distributions, "TI")
        
        #calculate null int distributions
        string_distributions = self.CalculateColumnDistributions(partitioned, self.AfcarsStringType)
        self.WriteColumnDistibutionsFile(string_distributions, "TS")

    def BuildPartitionedData(self, transformed):
        output = transformed["Output"]
        trend_source = {}

        for key in output:
            data = output.get(key, None)
            if data is not None:
                trend_source[key] = {
                    'RecordID': data.get("RecordID", None),
                    'DataSourceType': data.get("DataSourceType", None),
                    'StateJurisdictionID': data.get("StateJurisdictionID", None),
                    'StateNationalCode': data.get("StateNationalCode", None),
                    'FiscalYear': data.get("FiscalYear", None),
                    'PeriodCode': data.get("PeriodCode", None),
                    'REPDATYR': data.get("REPDATYR", None),
                    'REPDATMO': data.get("REPDATMO", None),
                    'FIPSCODE': data.get("FIPSCODE", None),
                    'STATE': data.get("STATE", None),
                    'NUMPLEP': data.get("NUMPLEP", None),
                    'DQ_DOBgtDtLatRem': data.get("DQ_DOBgtDtLatRem", None),
                    'DQ_missDOB': data.get("DQ_missDOB", None),
                    'DQ_DOBgtDtDisch': data.get("DQ_DOBgtDtDisch", None),
                    'DQ_DtDischEqDtLatRem': data.get("DQ_DtDischEqDtLatRem", None),
                    'DQ_DtDischLtDtLatRem': data.get("DQ_DtDischLtDtLatRem", None),
                    'DQ_gt21DOBtoDtDisch': data.get("DQ_gt21DOBtoDtDisch", None),
                    'DQ_gt21DOBtoDtLatRem': data.get("DQ_gt21DOBtoDtLatRem", None),
                    'DQ_missDisreasn': data.get("DQ_missDisreasn", None),
                    'DQ_missDoDFCDt': data.get("DQ_missDoDFCDt", None),
                    'DQ_missNumPlep': data.get("DQ_missNumPlep", None),
                    'DQ_totalrem1': data.get("DQ_totalrem1", None),
                    'DQ_missDtLatRem': data.get("DQ_missDtLatRem", None),
                    'DQ_gt21DtDischtoDtLatRem': data.get("DQ_gt21DtDischtoDtLatRem", None),
                    'DQ_ExitDISREASN': data.get("DQ_ExitDISREASN", None),
                    'DQ_InCareU18CASEGOAL': data.get("DQ_InCareU18CASEGOAL", None),
                    'DQ_InCareU18CURPLSET': data.get("DQ_InCareU18CURPLSET", None),
                    'DOBAge': data.get("DOBAge", None),
                    'SEX': data.get("SEX", None),
                    'EVERADPT': data.get("EVERADPT", None),
                    'DISREASN': data.get("DISREASN", None),
                    'CURPLSET': data.get("CURPLSET", None),
                    'PLACEOUT': data.get("PLACEOUT", None),
                    'CASEGOAL': data.get("CASEGOAL", None),
                    'FOSFAMST': data.get("FOSFAMST", None),
                    'AGEADOPT': data.get("AGEADOPT", None),
                    'AMIAKN': data.get("AMIAKN", None),
                    'ASIAN': data.get("ASIAN", None),
                    'BLKAFRAM': data.get("BLKAFRAM", None),
                    'HAWAIIPI': data.get("HAWAIIPI", None),
                    'WHITE': data.get("WHITE", None),
                    'HISORGIN': data.get("HISORGIN", None),
                    'UNTODETM': data.get("UNTODETM", None),
                    'CLINDIS': data.get("CLINDIS", None),
                    'MR': data.get("MR", None),
                    'VISHEAR': data.get("VISHEAR", None),
                    'PHYDIS': data.get("PHYDIS", None),
                    'DSMIII': data.get("DSMIII", None),
                    'OTHERMED': data.get("OTHERMED", None),
                    'MonthsSinceRem1Dt': data.get("MonthsSinceRem1Dt", None),
                    'MonthsInFosterCare': data.get("MonthsInFosterCare", None),
                    'MonthsSinceLatRmDt': data.get("MonthsSinceLatRmDt", None),
                    'MonthsSinceCurSetDt': data.get("MonthsSinceCurSetDt", None),
                }
        return trend_source

    def CalculateInCareCounts(self, partitioned, age=None):
        #define locals to use for calculating counts
        report_periodend_date = datetime.strptime(self.ReportPeriodEndDate, "%m/%d/%Y")
        default_date = datetime.strptime('12/31/9999', "%m/%d/%Y")

        incare = {}

        for key in partitioned:
            data = partitioned[key]
            record_id =  data.get('RecordID', None)

            discharge_date = data.get('DischargeDate', None)
            discharge_date = default_date if discharge_date is None or len(discharge_date) == 0 else datetime.strptime(discharge_date, "%m/%d/%Y")
            
            dob = data.get('BirthDate', None)
            dob = default_date if dob is None or len(dob) == 0 else datetime.strptime(dob, "%m/%d/%Y")

            latest_removal_date = data.get('LatestRemovalDate', None)
            latest_removal_date = default_date if latest_removal_date is None or len(latest_removal_date) == 0 else datetime.strptime(latest_removal_date, "%m/%d/%Y")

            if age == 18 and latest_removal_date <= report_periodend_date and \
                dob + timedelta(days=18*365.25) > report_periodend_date and \
                discharge_date > report_periodend_date:
                    incare[record_id] = record_id
            if age is None and latest_removal_date <= report_periodend_date and discharge_date > report_periodend_date:
                incare[record_id] = record_id
        return len(incare)

    def BuildTrendData(self, trend_source):
        trend_data = {}
        #calculate record number distributions
        recordid_distributions = self.CalculateColumnDistributions(trend_source, ['RecordID'])
        dupes = 0
        for key in recordid_distributions:
            if recordid_distributions[key].get('Count') > 1:
                dupes += 1
        
        trend_data[0] = {
                    'DataSourceType': self.FileMetaData.get('DataSourceType', None),
                    'StateNationalCode': self.FileMetaData.get('StateNationalCode', None),
                    'StateCode': self.FileMetaData.get('StateCode', None),
                    'FiscalYear': self.FileMetaData.get('FiscalYear', None),
                    'PeriodCode': self.FileMetaData.get('PeriodCode', None),
                    'PeriodEndDate': self.ReportPeriodEndDate,
                    'Name': 'Check,Duplicate', 
                    'Value': dupes
        }

        trend_data[1] = {
                    'DataSourceType': self.FileMetaData.get('DataSourceType', None),
                    'StateNationalCode': self.FileMetaData.get('StateNationalCode', None),
                    'StateCode': self.FileMetaData.get('StateCode', None),
                    'FiscalYear': self.FileMetaData.get('FiscalYear', None),
                    'PeriodCode': self.FileMetaData.get('PeriodCode', None),
                    'PeriodEndDate': self.ReportPeriodEndDate,
                    'Name': 'Check,Unique', 
                    'Value': len(recordid_distributions)
        }

        trend_data[2] = {
                    'DataSourceType': self.FileMetaData.get('DataSourceType', None),
                    'StateNationalCode': self.FileMetaData.get('StateNationalCode', None),
                    'StateCode': self.FileMetaData.get('StateCode', None),
                    'FiscalYear': self.FileMetaData.get('FiscalYear', None),
                    'PeriodCode': self.FileMetaData.get('PeriodCode', None),
                    'PeriodEndDate': self.ReportPeriodEndDate,
                    'Name': 'In Care,Under Age 18', 
                    'Value': self.CalculateInCareCounts(trend_source, 18)
        }

        trend_data[3] = {
                    'DataSourceType': self.FileMetaData.get('DataSourceType', None),
                    'StateNationalCode': self.FileMetaData.get('StateNationalCode', None),
                    'StateCode': self.FileMetaData.get('StateCode', None),
                    'FiscalYear': self.FileMetaData.get('FiscalYear', None),
                    'PeriodCode': self.FileMetaData.get('PeriodCode', None),
                    'PeriodEndDate': self.ReportPeriodEndDate,
                    'Name': 'In Care,All Ages', 
                    'Value': self.CalculateInCareCounts(trend_source)
        }

        #write
        self.WriteStoreMeasures(trend_data, '6M')

    @property
    def AfcarsNullIntType(self):
        return ['REPDATYR', 'REPDATMO', 'STATE', 'NUMPLEP' ]

    @property
    def AfcarsIntType(self):
        return ['DQ_DOBgtDtLatRem', 'DQ_missDOB', 'DQ_DOBgtDtDisch', 'DQ_DtDischEqDtLatRem', 'DQ_DtDischLtDtLatRem', 'DQ_gt21DOBtoDtDisch', 
        'DQ_gt21DOBtoDtLatRem', 'DQ_missDisreasn', 'DQ_missDoDFCDt','DQ_missNumPlep', 'DQ_totalrem1', 'DQ_missDtLatRem',
        'DQ_gt21DtDischtoDtLatRem', 'DQ_ExitDISREASN', 'DQ_InCareU18CASEGOAL', 'DQ_InCareU18CURPLSET', 'DOBAge', 'SEX', 'EVERADPT', 'DISREASN',
        'CURPLSET', 'PLACEOUT', 'CASEGOAL', 'FOSFAMST', 'AGEADOPT', 'AMIAKN', 'ASIAN', 'BLKAFRAM', 'HAWAIIPI', 'WHITE', 'HISORGIN', 'UNTODETM',
        'CLINDIS', 'MR', 'VISHEAR', 'PHYDIS', 'DSMIII', 'OTHERMED', 'MonthsSinceRem1Dt', 'MonthsInFosterCare', 'MonthsSinceLatRmDt', 'MonthsSinceCurSetDt' ]

    @property
    def AfcarsStringType(self):
        return ['FIPSCODE']