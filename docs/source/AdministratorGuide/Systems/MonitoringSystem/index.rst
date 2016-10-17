=================
Monitoring System
=================

.. contents:: Table of contents
   :depth: 3
   
Overview
=========

The Monitoring system is used to monitor various components of DIRAC. Currently, we have to monitoring types:

	WMSHistory: is used to monitor the DIRAC WMS
	Component Monitoring: is used to monitor DIRAC components such as services, agents, etc.
	
It is based on Elasticsearch distributed search and analytics NoSQL database. If you wan to use it, you have to install the Monitoring service and 
elasticsearch db. You can use a single node, if you do not have to store lot of data, otherwise you need a cluster (more than one node).

Install Elasticsearch
======================

You can found in https://www.elastic.co official web site. I propose to use standard tools to install for example: yum, rpm, etc. otherwise 
you encounter some problems. If you are not familiar with managing linux packages, you have to ask your college or read some relevant documents.

Configure the MonitoringSystem
===============================

You can run your El cluster without authentication or using User name and password. You have to add the following parameters:
	
	User
	Password
	Host
	Port

The User name and Password must be added to the local cfg file while the other can be added to the CS using the Configuration web application.
You have to handle the EL secret information similar way than MySQL.

For example:

::
	Systems
	{
	  NoSQLDatabases
	  {
	    User = test
	    Password = password
	  }
	  
	}


Enable WMSHistory monitoring
============================

You have to install the WorkloadManagemet/StatesMonitoringAgent.

Enable Component monitoring
===========================

You have to set DynamicMonitoring=True in the CS:

::
	Systems
	{
		Framework
		{
			 SystemAdministrator
		     {
		        ...
		        DynamicMonitoring = True
		      }
		   }
       }
       

.. image:: cs.png
   :align: center

Accessing the Monitoring information
=====================================

After you installed and configured the Monitoring system, you can use the Monitoring web application.
 
