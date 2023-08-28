=====
Usage
=====

.. contents:: Table of contents
   :depth: 3

-------
scripts
-------

There are two main scripts to get and set statuses on RSS:

* *dirac-rss-list-status*
* *dirac-rss-set-status*

dirac-rss-list-status
=====================

This command can be issued by everyone in possession of a valid proxy.

dirac-rss-set-status
====================

This command CANNOT be issued by everyone. You need the SiteManager property to
use it.

Appart from setting a new status, it will set the token owner for the elements
modified to the owner of the proxy used for a duration of 24 hours.

-----------------
interactive shell
-----------------

This is a quick reference of the basic usage of RSS from the python interactive shell.

There are two main components that can be used to extract information :

* the client : ResourceStatusSystem
* the helper : SiteStatus, ResourceStatus, NodeStatus

The second is a simplification of the client with an internal cache. Unless you
want to access not-only status information, please use the second. Nevertheless,
bear in mind that both require a valid proxy.

Helper
======

Let's get some statuses.

.. code-block:: python

   from DIRAC.ResourceStatusSystem.Client.ResourceStatus import ResourceStatus
   helper = ResourceStatus()

   # Request all status types of CERN-USER SE
   helper.getStorageElementStatus( 'CERN-USER' )[ 'Value' ]
   {'CERN-USER': {'ReadAccess': 'Active', 'RemoveAccess': 'Active', 'WriteAccess': 'Active', 'CheckAccess': 'Active'}}

   # Request ReadAccess status type of CERN-USER SE
   helper.getStorageElementStatus( 'CERN-USER', statusType = 'ReadAccess' )[ 'Value' ]
   {'CERN-USER': {'ReadAccess': 'Active'}}

   # Request ReadAccess & WriteAccess status types of CERN-USER SE
   helper.getStorageElementStatus( 'CERN-USER', statusType = [ 'ReadAccess', 'WriteAccess' ] )[ 'Value' ]
   {'CERN-USER': {'ReadAccess': 'Active', 'WriteAccess': 'Active'}}

   # Request ReadAccess status type of CERN-USER and PIC-USER SEs
   helper.getStorageElementStatus( [ 'CERN-USER', 'PIC-USER' ], statusType = 'ReadAccess' )[ 'Value' ]
   {'CERN-USER': {'ReadAccess': 'Active'}, 'PIC-USER': {'ReadAccess': 'Active'}}

   # Request unknown status type for PIC-USER SE
   helper.getStorageElementStatus( 'PIC-USER', statusType = 'UnknownAccess' )
   Cache misses: [('PIC-USER', 'UnknownAccess')]
   {'Message': "Cache misses: [('PIC-USER', 'UnknownAccess')]", 'OK': False}

   # Request unknown and a valid status type for PIC-USER SE
   helper.getStorageElementStatus( 'PIC-USER', statusType = [ 'UnknownAccess', 'ReadAccess' ] )
   Cache misses: [('PIC-USER', 'UnknownAccess')]
   {'Message': "Cache misses: [('PIC-USER', 'UnknownAccess')]", 'OK': False}


Similarly, let's set some statuses.

.. code-block:: python
   :emphasize-lines: 13

   from DIRAC.ResourceStatusSystem.Client.ResourceStatus import ResourceStatus
   helper = ResourceStatus()

   # Are you sure you have a proxy with SiteManager property ? If not, this is what you will see.
   helper.setStorageElementStatus( 'PIC-USER', 'ReadAccess', 'Active', reason = 'test' )[ 'Message' ]
   'Unautorized query'

   # Let's try again with the right proxy
   _ = helper.setStorageElementStatus( 'PIC-USER', 'ReadAccess', 'Bad', reason = 'test' )
   helper.getStorageElementStatus( 'PIC-USER', 'ReadAccess' )
   {'OK': True, 'Value': {'PIC-USER': {'ReadAccess': 'Bad'}}}

   # Or banning all SE. For the time being, we have to do it one by one !
   helper.setStorageElementStatus( 'PIC-USER', [ 'ReadAccess', 'WriteAccess' ], 'Bad', reason = 'test' )[ 'OK' ]
   False
