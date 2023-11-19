baseDateTimeStampString = None
currentDateTimeStampString = None
appConfig = None 
 
systemRuleConfig = None
ruleConfig = None
customRuleConfig = None

templateJsonData = None
inputJsonData = None
massagedJsonData = None
extract_output_file_object = []
output_batch_files = []
log_file_name = None
mongo_db = None
extract_snapshot_table = 'SnapshotExtract'
extract_snapshot_table_cols = "AffiliationLineId,AffiliationGUID,ProviderID,ProviderGUID,PracticeOfficeLocationId,PPLID,PPLGUID,ProviderSpecialityId,AffPracticeId,SubNetworkId,PracticeAddressId,ProviderCategory,ProviderType,ProviderType_FF,DegreeTitle,DegreeTitle_FF,Gender,Gender_FF,NPI,NPI_FF,IsDummyNPI,SSN,FirstName,FirstName_FF,MiddleName,MiddleName_FF,LastName,LastName_FF,Prefix,Prefix_FF,Suffix,Suffix_FF,MaidenName,MaidenName_FF,BirthDate,BirthDate_FF,ProviderEmail,Email_FF,Ethnicity,Ethinicity_FF,BirthCity,BirthCity_FF,BirthCountry,BirthCountry_FF,BirthState,BirthState_FF,CitizenshipCountry,CitizenshipCountry_FF,MaritalStatus,MaritalStatus_FF,SpouseName,SpouseName_FF,ProviderPhone,Phone_FF,ProviderCellPhone,ProviderCellPhone_FF,DBAName,DbaOwner_FF,ProviderFax,Fax_FF,SpecialtyType,SpecialtyType_FF,TaxonomyCode,TaxonomyCode_FF,TypeLevel1,TypeLevel1_FF,ClassificationLevel2,ClassificationLevel2_FF,SpecializationLevel3,SpecializationLevel3_FF,BoardName,BoardName_FF,BoardStatus,BoardStatus_FF,CertificationNumber,CertificationNumber_FF,CertificationDate,CertificationDate_FF,ExpirationDate,ExpirationDate_FF,ReCertificationDate,ReCertificationDate_FF,ExamDate,ExamDate_FF,IsPursuing,IsPursuing_FF,Explanation,Explanation_FF,PracticeName,PracticeName_FF,TaxId,TaxId_FF,PracticeEffectiveDate,PracticeEffectiveDate_FF,PracticeTerminateDate,PracticeTerminateDate_FF,PracticeTerminationReason,PracticeTerminationReason_FF,LocationName,LocationName_FF,Address1,Address1_FF,Address2,Address2_FF,City,City_FF,StateCode,StateCode_FF,Zip,Zip_FF,County,County_FF,CountryCode,CountryCode_FF,CountryName,CountryName_FF,Latitutde,Longitude,ProviderPracticeLocationEffectiveDate,ProviderPracticeLocationEffectiveDate_FF,ProviderPracticeLocationTerminationDate,ProviderPracticeLocationTerminationDate_FF,ProviderPracticeLocationTerminationReason,ProviderPracticeLocationTerminationReason_FF,NetworkName,NetworkName_FF,SubNetworkName,SubNetworkName_FF,AffiliationEffectiveDate,AffiliationEffectiveDate_FF,AffiliationTerminationDate,AffiliationTerminationDate_FF,AffiliationTerminationReason,AffiliationTerminationReason_FF,EmployeeType,EmployeeType_FF,PositionName,PositionName_FF,JobType,JobType_FF,ConfidentialHire,ConfidentialHire_FF,ContactTitle,ContactTitle_FF,ContactFirstName,ContactFirstName_FF,ContactLastName,ContactLastName_FF,ContactEmail,ContactEmail_FF,ContactPhone,ContactPhone_FF,ContactPhoneExtension,ContactPhoneExtension_FF,ContactFax,ContactFax_FF,ContactFaxExtension,ContactFaxExtension_FF,ContactPager,ContactPager_FF,DirectorName,DirectorName_FF,RecruiterName,RecruiterName_FF,WeekendHours,WeekendHours_FF,EveningHours,EveningHours_FF,UndefinedHours,UndefinedHours_FF,DateOfHiring,DateOfHiring_FF,OffficeDetailsEffectiveDate,OffficeDetailsEffectiveDate_FF,ProvCreatedDate,ProvTermDate,AffCreatedDate,AffTermDate,AffiliationBillingLocationName,AffiliationBillingLocationName_FF,AffiliationBillingAddress1,AffiliationBillingAddress1_FF,AffiliationBillingAddress2,AffiliationBillingAddress2_FF,AffiliationBillingCity,AffiliationBillingCity_FF,AffiliationBillingState,AffiliationBillingState_FF,AffiliationBillingZip,AffiliationBillingZip_FF,AffiliationBillingCounty,AffiliationBillingCounty_FF,AffiliationBillingCountyCode,AffiliationBillingCountyCode_FF,AffiliationBillingCountryName,AffiliationBillingCountryName_FF,AffiliationBillingCountryCode,AffiliationBillingCountryCode_FF,AffiliationBillingEffectiveDate,AffiliationBillingEffectiveDate_FF,AffiliationBillingTerminationDate,AffiliationBillingTerminationDate_FF,AffiliationBillingTerminationReason,AffiliationBillingTerminationReason_FF,AffiliationMailingLocationName,AffiliationMailingLocationName_FF,AffiliationMailingAddress1,AffiliationMailingAddress1_FF,AffiliationMailingAddress2,AffiliationMailingAddress2_FF,AffiliationMailingCity,AffiliationMailingCity_FF,AffiliationMailingState,AffiliationMailingState_FF,AffiliationMailingZip,AffiliationMailingZip_FF,AffiliationMailingCounty,AffiliationMailingCounty_FF,AffiliationMailingCountyCode,AffiliationMailingCountyCode_FF,AffiliationMailingCountryName,AffiliationMailingCountryName_FF,AffiliationMailingCountryCode,AffiliationMailingCountryCode_FF,AffiliationMailingEffectiveDate,AffiliationMailingEffectiveDate_FF,AffiliationMailingTerminationDate,AffiliationMailingTerminationDate_FF,AffiliationMailingTerminationReason,AffiliationMailingTerminationReason_FF,FeeScheduleName,FeeScheduleName_FF,FeeScheduleCode,FeeScheduleCode_FF,AffiliationFSEffectiveDate,AffiliationFSEffectiveDate_FF,AffiliationFSTerminationDate,AffiliationFSTerminationDate_FF,AffiliationFSTerminationReason,AffiliationFSTerminationReason_FF,ContractName,ContractName_FF,ContractStartDate,ContractStartDate_FF,ContractEndDate,ContractEndDate_FF,CurrentDate,ProviderWorkHours__json,ProviderLanguages__json,HospitalPrivileges__json,ProviderEducationTraining__json,ProviderLicenses__json,ProviderLicensesFederal__json,AdditionalSpecialties__json,PracticeAddressContactDetails__json,Exclusion__json,ProviderSanctions__JSON,PracticeTaxAddress__json,ProviderGroupNPI__json,ProviderOtherIds__json,PracticeLocationOtherID__json,AffiliationOtherID__json,ProviderStatus,OfficeLocationAlias,OfficeLocationAlias_FF,OfficeLocationAliasPurpose,OfficeLocationAliasPurpose_FF,ProPracLocOtherIDs__json,PracticeLocationSiteService__json,AddOnSiteService__json,RecordLevelFlag"
mergeSchema = {
            "properties": {
				"ProviderOtherIdType":{
                    "mergeStrategy": "append"
                },
				"ProviderEducationTraining": {
                    "mergeStrategy": "append"
                },
				"ProviderSanction": {
                    "mergeStrategy": "append"
                },
				"HospitalPrivileges": {
                    "mergeStrategy": "append"
                },
				"ProviderSpecialty": {
                    "mergeStrategy": "append"
                },
				"AdditionalSpecialties": {
                    "mergeStrategy": "append"
                },
				"ProviderLicense": {
                    "mergeStrategy": "append"
                },
				"ProviderLicensesFederal": {
                    "mergeStrategy": "append"
                },
				"ProviderLanguages": {
                    "mergeStrategy": "append"
                },
				"ProviderGroupNPI": {
                    "mergeStrategy": "append"
                },
				"PracticeLocation": {
                    "mergeStrategy": "append"
                },                
                "PracticeAddressContactDetails": {
                    "mergeStrategy": "append"
                },
                "ProviderPracticeLocationWorkHours": {
                    "mergeStrategy": "append"
                },
                "PracticeLocationSiteService": {
                    "mergeStrategy": "append"
                },
                "PracticeLocationOtherID": {
                    "mergeStrategy": "append"
                },
                "PracticeTaxAddress": {
                    "mergeStrategy": "append"
                },
                "ProviderPracticeLocationOtherId": {
                    "mergeStrategy": "append"
                },
                "AffiliationLine": {
                    "mergeStrategy": "append"
                },
                "AffiliationLineOtherID": {
                    "mergeStrategy": "append"
                },
                "AddOnSiteService": {
                    "mergeStrategy": "append"
                },
                "AffiliationLineFeeSchedule": {
                    "mergeStrategy": "append"
                },
                "Exclusion": {
                    "mergeStrategy": "append"
                }
                }
            }
