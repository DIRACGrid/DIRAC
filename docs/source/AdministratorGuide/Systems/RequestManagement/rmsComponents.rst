.. _rmsComponents:

--------------
RMS Components
--------------

All the components described here MUST be installed in order to have a working RMS. The exception is the ReqProxy, which is optional.

.. _requestDB:

RequestDB
---------

This DB hosts the various :ref:`rmsObjects`. No special configuration


.. _reqManager:

ReqManager
----------

This is the service in front of the DB. It has the following special configuration options:

* ``ConstantRequestDelay``: (default 0 minutes) if not 0, this is the constant retry delay we add when putting a Request back to the DB

.. _RequestExecutinAgent:

RequestExecutingAgent
---------------------


The RequestExecutingAgent (REA) is in charge of executing the Requests.It will fetch requests from the database, and process them in parallel (using :py:mod:`~DIRAC.Core.Utilities.ProcessPool`), using the proxy of the user that created the Request (this means the machine on which the REA runs must have enough privileges).

A Request will be fetched from the DB, and all its operation executed in turns. The execution stops either because everything is done, or because there is an error, or because we delegated the work to FTS.

At the end of the execution, if the Request comes from a job, we set the job to (`Done`, `Request Done`), providing its previous status was (`Completed`, `Pending Request`). If the request fails, the job will stay in this status (uncool...).

The RequestExecutingAgent is one of the few that can be duplicated. There are protections to make sure that a Request is only processed by one REA at the time.

Configuration options are described :mod:`here <DIRAC.RequestManagementSystem.Agent.RequestExecutingAgent>`.

==============
Retry strategy
==============

Operations are normally retried several times in case they fail. There is a delay between each execution, depending on the case:

* If the option ``ConstantRequestDelay`` is set in the :ref:`ReqManager`, then we apply that one
* If one of the StorageElement (source or target) is banned, then we wait 1 hour (except if the SE is always banned, then we fail the Operation)
* Otherwise the delay increases following a logarithmic scale with the number of attempts


.. _CleanReqDBAgent:

CleanReqDBAgent
---------------

This agent cleans the DB from old Requests in final state. Special configuration options are

* `DeleteGraceDays`: (default 60)  Delay after which Requests are removed
* `DeleteLimit`: (default 100)  Maximum number of Requests to remove per cycle
* `DeleteFailed`: (default False)  Whether to delete also Failed request
* `KickGraceHours`: (default 1)  After how long we should kick the Requests in `Assigned`
* `KickLimit`: (default 10000)  Maximum number of requests kicked by cycle

.. _reqProxy:

ReqProxy
--------

The ReqProxy service is used as a failover for the ReqManager. A client will first attempt to send a Request to the ReqManager, but if it fails for whatever reason (service or DB down), it will send it to one of the ReqProxy. The ReqProxy will then store the Request on the local disk of the machine, and will periodically attempt to forward the Request to the ReqManager until it succeeds.

It is not mandatory to have ReqProxy, but highly recommended.

The only specific configuration option is for the URLs section, where it should be `ReqProxyURLs`, instead of `ReqProxy`
