from DIRAC.AccountingSystem.private.Policies.JobPolicy import JobPolicy as myJobPolicy

gPoliciesList = {"Job": myJobPolicy(), "WMSHistory": myJobPolicy(), "Pilot": myJobPolicy(), "Null": False}
