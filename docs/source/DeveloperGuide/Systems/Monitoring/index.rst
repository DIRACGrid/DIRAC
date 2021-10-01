.. contents:: Table of contents
   :depth: 3

=================
Monitoring System
=================

------------
Overview
------------

The system is storing monitoring information. It means the data stored in the database is time series. It can be used to monitor:
   -computing task (for example: Grid jobs, etc.)
   -computing infrastructures (for example: machines, etc.)
   -data movement (for example: Data operation etc.)

This system is based on ElasticSearch, RabbitMQ and DIRAC plotting facilities. It allows to introduce new monitoring types by adding
minimal code.

------------
Architecture
------------

It is based on layered architecture and is based on DIRAC architecture:

* **Services**

  * MonitoringHandler:
    DISET request handler base class for the MonitroingDB

* **DB**

  * MonitoringDB:
    It is a based on ElasticSearch database and provides all the methods which needed to create the reports. Currently, it supports only
    one type of query: It creates a dynamic buckets which will be used to retrieve the data points. The query used to retrieve the data points
    is retrieveBucketedData. As you can see it uses the ElasticSearch QueryDSL language. Before you modify this method please learn this language.

   * private:
      - Plotters: It contains all Plotters used to create the plots. More information will be provided later.
      - DBUtils: It provides utilities used to manipulate the data.
      - MainReporter: It contains all available plotters and it has a reference to the database. It uses the db to retrieve the data and the Plotter to create the plot.
      - TypeLoader: It loads all Monitoring types.


* **Clients**
   * MonitoringClient is used to interact withe the Monitoring service.
   * Types contains all Monitoring types.

-------------------------------
How to add new monitoring type?
-------------------------------
A new monitoring type can be added:
   - You have to define the monitoring values and the conditions. For example: cond1, cond2, monitoring value id ex1
     Monitoring/Client/Types/Example.py For more information please have a look WMSHistory.py

      self.setKeyFields( ['cond1', 'cond2'] )
      self.setMonitoringFields( [ 'ex1' ] )
   - create the plotter: MonitoringSystem/Client/private/Plotters/ExamplePlotter.py
     Note: The file name must ends with Plotter word.
     You have to implement two functions:

         def _reportExample( self, reportRequest ):
         def _plotExample( self, reportRequest, plotInfo, filename ):

     In the Monitoring page you will see and Example. But if you want to rename it:
         _reportExample = 'Test1'
         def _reportExample( self, reportRequest ):

     More information: WMSHistoryPlotter.py

   - Add the new monitoring to the WebAppDIRAC Monitoring application.
