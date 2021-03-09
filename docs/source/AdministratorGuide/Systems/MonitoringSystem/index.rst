.. _monitoring_system:

=================
Monitoring System
=================

.. contents:: Table of contents
   :depth: 3

Overview
=========

The Monitoring system is used to monitor various components of DIRAC. Currently, we have three monitoring types:

  - WMSHistory: for monitoring the DIRAC WMS
  - Component Monitoring: for monitoring DIRAC components such as services, agents, etc.
  - RMS Monitoring: for monitoring the DIRAC RequestManagement System (mostly the Request Executing Agent).

It is based on Elasticsearch distributed search and analytics NoSQL database.
If you want to use it, you have to install the Monitoring service, and of course connect to a ElasticSearch instance.

Enable WMSHistory monitoring
============================

You have to add ``Monitoring`` to the ``Backends`` option of WorkloadManagemet/StatesAccountingAgent.
If you do so, this agent will collect information using the JobDB and send it to the Elasticsearch database.
This same agent can also report to the MySQL backend of the Accounting system (which is in fact the default).

Optionally, you can use an MQ system (like RabbitMQ) for failover, even though the agent already has a simple failover mechanism.
You can configure the MQ in the local dirac.cfg file where the agent is running::

   Resources
   {
     MQServices
     {
       hostname.some.where
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

You have to set ``EnableActivityMonitoring=True`` in the CS.
It can be done globally, the ``Operations`` section, or per single component.



Enable RMS Monitoring
=====================

In order to enable RMSMonitoring we need to set value of ``EnableRMSMonitoring`` flag to yes/true in the CS::


   Systems
   {
     RequestManagement
     {
       <instance>
       {
         Agents
         {
           RequestExecutingAgent
           {
             ...
             EnableRMSMonitoring = True
           }
         }
       }
     }
   }



Accessing the Monitoring information
=====================================

After you installed and configured the Monitoring system, you can use the Monitoring web application (from the Accounting WebApp).
