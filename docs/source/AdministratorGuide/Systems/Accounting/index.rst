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
