.. contents:: Table of contents
   :depth: 3

=======================================================================================
<PREVIEW_NOT_IN_PROD> <PREVIEW_NOT_IN_PROD> <PREVIEW_NOT_IN_PROD> <PREVIEW_NOT_IN_PROD>
=======================================================================================

==============================
Pilots Logging system overview
==============================

Pilots Loggins system is designed to allow logging of pilot state on every stage of lifecycle, including before installing
DIRAC client and starting pilot process.

Each logging entry includes:

- current status of the Pilot - has to be one of predefined list of possible states,
- additional information about status,
- timestamp of logging the status - if there is no timestamp of actual event provided, time of adding entry to database will be used,
- source of the logging message to distinguish updates from Pilot itself and other services.

.. image:: PilotsLoggingDiagram.png
   :alt: PilotsLogging system
   :align: center


Server side
================================

Server elements of Pilots Logging system is build using five elements:

- message queue (RabbitMQ) server,
- message queue consumer,
- DIRAC Client,
- DIRAC Service,
- database.

Message queue
--------------------------------

Message works as a interface between Pilot and Pilots Logging service. Pilot puts status related messages into queue then
messages are handled by message queue consumer.

Message queue consumer
--------------------------------

Consumer registers itself into message queue. When new messages arrive they are handled by callback function. In consumer
messages are processed and passed to DIRAC Service using DIRAC Client.

DIRAC Client
--------------------------------

Client handles RPC communication with Service. This is 'thin-client', all business logic is in Service.

DIRAC Service
--------------------------------

Service exports functions to be called by Clients. It handles all operations on databases. All server side logic of
Pilots Logging system is defined here. Two databases are accessed to gather all required information.

Database
--------------------------------

Database class handles operation on the database. Object-relational mapping is done using SQLAlchemy. Single table stores
record for every status reported by Pilot:

.. image:: PilotsLoggingDB.png
   :alt: PilotsLogging database schema
   :align: center

Pilot side
================================

TBD
