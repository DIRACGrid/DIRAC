"""
Provides Accounting functionality to DIRAC
It includes 2 different Services:

* DataStore: where new records are inserted
* ReportGenerator: that produce reports using the inserted records

and the associated Clients:
* DataStoreClient
* ReportsClient

DIRAC Accounting uses a number of predefined Types that must include:
* Accounting keys (text) to classify the records
* Accounting fields (numeric) to included the accounted data
* bucket definition to set the granularity of the reports

"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
__package__ = 'DIRAC.AccountingSystem'
