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
extract_snapshot_table_cols = "AffiliationLineId,ProviderID,ProviderCategory,ProviderType,DegreeTitle,Gender,NPIPrior,NPI,IsDummyNPI,SSN,FirstName,MiddleName,LastName,Prefix,Suffix,MaidenName,BirthDate,ProviderEmail,Ethnicity,BirthCity,BirthCountry,BirthState,CitizenshipCountry,MaritalStatus,SpouseName,ProviderPhone,ProviderCellPhone,DBAName,ProviderFax,SanctionType,RegistrationNumber,SanctionState,SanctionBoard,SanctionStartDate,SanctionEndDate,SanctionReason,SanctionModifiedBy,SanctionModifiedDate,SpecialityType,SpecialityLevelNamePrior,TaxonomyCode,TypeLevel1,ClassificationLevel2,SpecializationLevel3,BoardName,BoardStatus,CertificationNumber,CertificationDate,ExpirationDate,ReCertificationDate,ExamDate,IsPursuing,Explanation,TaxonomyCodePrior,SpecialtyBoardStatus,PriorLicenseTypeName,PriorLicenseNumber,PracticeNamePrior,PracticeName,TaxIdPrior,TaxId,PracticeEffectiveDate,PracticeTerminateDate,PracticeTerminationReason,PracticeModifiedBy,PracticeModifiedDate,LocationName,OfficeLocationAlias,OfficeLocationAliasPurpose,OfficeAliasModifiedBy,OfficeAliasModifiedDate,Address1Prior,Address1,Address2Prior,Address2,CityPrior,City,StateCodePrior,StateCode,ZipPrior,Zip,CountyPrior,County,PriorCountryCode,CountryCode,PriorCountryName,CountryName,PriorLatitude,Latitutde,PriorLongitude,Longitude,ProviderPracticeLocationEffectiveDate,ProviderPracticeLocationTerminationDate,ProviderPracticeLocationTerminationReason,LocationEMail1,PPLID,AffiliationBillingLocationName,AffiliationBillingAddress1,AffiliationBillingAddress2,AffiliationBillingCity,AffiliationBillingState,AffiliationBillingZip,AffiliationBillingCounty,AffiliationBillingCountyCode,AffiliationBillingCountryName,AffiliationBillingCountryCode,AffiliationBillingEffectiveDate,AffiliationBillingTerminationDate,AffiliationBillingTerminationReason,AffiliationMailingLocationName,AffiliationMailingAddress1,AffiliationMailingAddress2,AffiliationMailingCity,AffiliationMailingState,AffiliationMailingZip,AffiliationMailingCounty,AffiliationMailingCountyCode,AffiliationMailingCountryName,AffiliationMailingCountryCode,AffiliationMailingEffectiveDate,AffiliationMailingTerminationDate,AffiliationMailingTerminationReason,PracticeLocationModifiedBy,PracticeLocationModifiedDate,BillingAddress1Prior,BillingAddress2Prior,BillingCityPrior,BillingStatePrior,BillingZipPrior,BillingCountyPrior,BillingCountryNamePrior,BillingCountryCodePrior,NetworkName,SubNetworkName,AffiliationEffectiveDate,AffiliationTerminationDate,AffiliationTerminationReason,FeeScheduleName,FeeScheduleCode,AffiliationFSEffectiveDate,AffiliationFSTerminationDate,AffiliationFSTerminationReason,AffiliationFSModifiedBy,AffiliationFSModifiedDate,ContractName,ContractStartDate,ContractEndDate,ProviderStatus,EmployeeType,PositionName,JobType,ConfidentialHire,ContactTitle,ContactFirstName,ContactLastName,ContactEmail,ContactPhone,ContactPhoneExtension,ContactFax,ContactFaxExtension,ContactPager,DirectorName,RecruiterName,WeekendHours,EveningHours,UndefinedHours,DateOfHiring,OffficeDetailsEffectiveDate,ProviderOtherIds__json,AdditionalSpecialties__json,ProPracLocOtherIDs__json,PracticeTaxAddress__json,AffiliationOtherID__json,ProviderLanguages__json,PracticeLocationSiteService__json,ProviderLicenses__json,ProviderLicensesFederal__json,PracticeLocationOtherID__json,ProviderGroupNPI__json,AddOnSiteService__json,PracticeAddressContactDetails__json,ProviderWorkHours__json,ProviderEducationTraining__json"

mergeSchema = {
            "properties": {
                "Practice": {
                    "mergeStrategy": "append"
                },
                "PracticeLocation": {
                    "mergeStrategy": "append"
                },
                "ProviderAlias": {
                    "mergeStrategy": "append"
                },
                "ProviderSpeciality": {
                    "mergeStrategy": "append"
                },
                "ProviderLicense": {
                    "mergeStrategy": "append"
                },
                "ProviderLicenseStatusLog": {
                    "mergeStrategy": "append"
                },
                "ProviderAppointmentAssociation": {
                    "mergeStrategy": "append"
                },
                "ProviderCertification": {
                    "mergeStrategy": "append"
                },
                "ProviderCMECredit": {
                    "mergeStrategy": "append"
                },
                "ProviderCoveringCollaborating": {
                    "mergeStrategy": "append"
                },
                "ProviderEducationTraining": {
                    "mergeStrategy": "append"
                },
                "ProviderHealth": {
                    "mergeStrategy": "append"
                },
                "ProviderHospitalPrivelege": {
                    "mergeStrategy": "append"
                },
                "ProviderLanguages": {
                    "mergeStrategy": "append"
                },
                "ProviderMalPractice": {
                    "mergeStrategy": "append"
                },
                "ProviderOtherIdType": {
                    "mergeStrategy": "append"
                },
                "ProviderPractice": {
                    "mergeStrategy": "append"
                },
                "ProviderPracticeLocation": {
                    "mergeStrategy": "append"
                },
                "ProviderContractLine": {
                    "mergeStrategy": "append"
                },
                "ContractLineFeeSchedule": {
                    "mergeStrategy": "append"
                },
                "ProviderProfessionalLiability": {
                    "mergeStrategy": "append"
                },
                "ProviderReference": {
                    "mergeStrategy": "append"
                },
                "ProviderSubNpi": {
                    "mergeStrategy": "append"
                },
                "SubNetworkPractice": {
                    "mergeStrategy": "append"
                },
                "ProviderEmploymentHistory": {
                    "mergeStrategy": "append"
                },
                "ProviderOtherIdType":{
                    "mergeStrategy": "append"
                }
            }
        }
