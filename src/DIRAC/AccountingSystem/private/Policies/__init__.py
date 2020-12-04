from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from DIRAC.AccountingSystem.private.Policies.JobPolicy import JobPolicy as myJobPolicy

gPoliciesList = {
    'Job': myJobPolicy(),
    'WMSHistory': myJobPolicy(),
    'Pilot': myJobPolicy(),
    'Null': False
}
