
baseDateTimeStampString = None
appConfig = None 
ruleConfig = None 
systemRuleConfig = None 
templateJsonData = None
inputJsonData = None
massagedJsonData = None
currentDateTimeStampString = None
output_batch_files = []

log_file_name = None

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
                "Exclusion": {
                    "mergeStrategy": "append"
                },
                "PracticeLocationSiteService": {
                    "mergeStrategy": "append"
                },
                "AddOns": {
                    "mergeStrategy": "append"
                }
            }
        }
