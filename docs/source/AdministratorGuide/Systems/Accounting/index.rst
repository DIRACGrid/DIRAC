=================
Accounting System
=================

.. contents:: Table of contents
   :depth: 3
   

The Accounting system is responsible to collect and store data regarding to the activities: data transfers, pilot jobs. It is designed for store
historical data by creating time buckets. 
The data stored with properties, which are used to classify the records: user, site and also properties which can be measured: memory, CPU.

The data can be accessible through the DIRAC web framework using the Accounting application. The records are stored in the AccountingDB, 
in "two" different formats:

  - raw records
  - time buckets: this is displayed to the users

The system consists of the following accounting types:
   - Job:  for creating reports of the activity on the computing resources such as Grid, Cloud, etc. 
   - Pilot: for creating reports for pilot jobs running on different computing elements such as ARC CE, CREAM, VAC, etc.
   - Data operation: for creating reports about data activities: transfers, replication, removal, etc.
   - WMS History: This it used for monitoring the DIRAC Workload Management system. This type is replaced by the
     WMS monitoring which is part of the Monitoring system. It is replaced, because the WMS History type is for real
     time monitoring and MySQL is not for storing time series with high resolution.


AccountingDB
============

It is based on MySQL. It stores the raw records and the time buckets and provides the functionalities for creating the accounting reports.
According to the computing activities (for example running jobs) and the size of the DIRAC system the size of the db can be small: a single
MySQL server or it can be a multiple instance.
The system can allow to store the accounting types in different database instances using Multi-DB accounting.
    
 
Multi-DB accounting
======================
Accounting types can be stored in a different DB. By default all accounting types data will be stored in the database 
defined under **/Systems/Accounting/_Instance_/Databases/AccountingDB**. 
To store a type data in a different database (say WMSHistory) define the data base location under the databases directory. 
Then define **/Systems/Accounting/_Instance_/Databases/MultiDB** and set an option with the type name and value pointing to the database to use. 
For instance::


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


.. _datastorehelpers:

DataStore Helpers
======================
From DIRAC v6r17p14 there is the possibility to run multiple 'DataStore' services, where one
needs to be called 'DataStoreMaster', while all the others may be called anything else. The master
will create the proper buckets and the helpers only insert the records to the 'in' table.  For
example::

  install service Accounting DataStoreHelper -m DataStore -p RunBucketing=False -p Port=9166

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
      
 
Report generator
================
It is used for creating the accounting reports. Note: the report generator is caching the plots using the local file system. It is very important for 
running a service in a hardware which are having very good disk. 
 

Installation
==============
In order to use the system, it requires to install the following components: AccountingDB, DataStore, ReportGenerator, for the WMSMonitoring the StatesAccountingAgent.
The simplest is by using the SystemAdministrator CLI::

  install db AccountingDB
  install service Accounting DataStore
  install service Accounting ReportGenerator
  install agent WorkloadManagement StatesAccountingAgent

Accounting user interface
=========================

The Accounting web application can be used for creating the reports. If you do not have WebAppDIRAC, please install it following :ref:`installwebappdirac` instructions.
