.. _monitoring_system:

=================
Monitoring System
=================

.. contents:: Table of contents
   :depth: 3

Overview
=========

The Monitoring system is used to monitor various components of DIRAC. Currently, we have two monitoring types:

  - WMSHistory: for monitoring the DIRAC WMS
  - Component Monitoring: for monitoring  DIRAC components such as services, agents, etc (in preview mode, it will be fully working starting from DIRAC v7r2).

It is based on Elasticsearch distributed search and analytics NoSQL database. If you want to use it, you have to install the Monitoring service and
elasticsearch db. You can use a single node, if you do not have to store lot of data, otherwise you need a cluster (more than one node).


Enable WMSHistory monitoring
============================

You have to install the WorkloadManagemet/StatesMonitoringAgent. This agent is used to collect information using the JobDB and send it to the Elasticsearch database.
If you install this agent, you can stop the StatesAccounting agent.

Note: You can use RabbitMQ for failover. This is optional as the agent already has a failover mechanism. You can configure RabbitMQ in the local dirac.cfg file
where the agent is running::

   Resources
   {
     MQServices
     {
       hostname (for example lbvobox10.cern.ch)
       {
         MQType = Stomp
         Port = 61613
         User = monitoring
         Password = seecret
         Queues
         {
           WMSHistory
           {
             Acknowledgement = True
           }
         }
       }
     }
   }



Enable Component monitoring
===========================

You have to set DynamicMonitoring=True in the CS::

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
