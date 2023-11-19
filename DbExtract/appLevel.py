
baseDateTimeStampString = None
appConfig = None 
log_file_name = None
mongo_db = None
provider_table = 'SnapshotExtract'
db_columns = [
    'HPID','SSN','DegreeTitle','NPI','PrecedingTitle','FirstName','MiddleName','LastName','BirthDate','SucceedingTitle','AdditionalSucceedingTitle','Gender','DebarredEffecticeDate','DebarredCancelDate','MaintenanceUser','MaintenanceTimestamp','SpotSyncIndicator','PriorIdentifier','PriorTypeCode','PriorBusinessName','PriorEffectiveDaete','TaxId','TypeCode','PracticeName','EffectiveDate','CancelDate','MaintenanceUserPractice','MaintenanceTimestampPractice','Address1','Address2','City','StateCode','Zip','County','OtherIdNumber','HippaTaxonomyCode','SpecialtyBoardStatus','NetworkName','BillingAddress1','BillingAddress2','BillingCity','BillingState','BillingZip','DEALicenseNumber','OfficeHours'
    ]
db_columns1 = ['Id'
      ,'UserId'
      ,'SpecialtyTypeId'
      ,'ProviderTypeId'
      ,'ProviderSubTypeId'
      ,'DegreeTitleId'
      ,'Gender'
      ,'IsDummyNPI'
      ,'NPI'
      ,'SSN'
      ,'TaxId'
      ,'FirstName'
      ,'MiddleName'
      ,'LastName'
      ,'PreferedName'
      ,'MaidenName'
      ,'OtherKnownName'
      ,'SpouseName'
      ,'Suffix'
      ,'Prefix'
      ,'BirthDate'
      ,'Email'
      ,'Phone'
      ,'Fax'
      ,'Pager'
      ,'BirthCountryId'
      ,'BirthStateId'
      ,'BirthCity'
      ,'Ethinicity']

mergeSchema = {
            "properties": {
                "ProviderSpeciality": {
                    "mergeStrategy": "append"
                }
            }
        }
