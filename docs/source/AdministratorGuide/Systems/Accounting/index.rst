=================
Accounting System
=================

.. contents:: Table of contents
   :depth: 3
   
Multi-DB accounting
======================
Since v6r12 each accounting type can be stored in a different DB. By default all accounting types data will be stored in the database defined under **/Systems/Accounting/_Instance_/Databases/AccountingDB**. To store a type data in a different database (say WMSHistory) define the data base location under the databases directory. Then define **/Systems/Accounting/_Instance_/Databases/MultiDB** and set an option with the type name and value pointing to the database to use. For instance::


    Systems
    {
      Accounting
      {
        Development
        {
          AccountingDB
          {
            Host = localhost
            User = dirac
            Password = dirac
            DBName = accounting
          }
          Acc2
          {
            Host = somewhere.internet.net
            User = dirac
            Password = dirac
            DBName = infernus
          }
          MultiDB
          {
            WMSHistory = Acc2
          }
        }
      }
    }
    
With the previous configuration all accounting data will be stored and retrieved from the usual database except for the _WMSHistory_ type that will be stored and retrieved from the _Acc2_ database.

DataStore Helpers
======================
From DIRAC v8r17p14 we are able to run multiple services. The master will creates the proper buckets and the helpers only insert the records to the 'in' table.
When you install the DataStore helper service you have to set RunBucketing parameter False.
For example:
install service Accounting DataStoreHelper -m DataStore -p RunBucketing=True -p Port=1966
In the CS you have to define DataStoreMaster. For example::
     
	URLs
	{
        DataStore = dips://lbvobox105.cern.ch:9133/Accounting/DataStore
        DataStore += dips://lbvobox105.cern.ch:9166/Accounting/DataStoreHelper
        DataStore += dips://lbvobox102.cern.ch:9166/Accounting/DataStoreHelper
        ReportGenerator = dips://lbvobox106.cern.ch:9134/Accounting/ReportGenerator
        DataStoreHelper = dips://lbvobox105.cern.ch:9166/Accounting/DataStoreHelper
        DataStoreHelper += dips://lbvobox102.cern.ch:9166/Accounting/DataStoreHelper
        DataStoreMaster = dips://lbvobox105.cern.ch:9133/Accounting/DataStore
       }
