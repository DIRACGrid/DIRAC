.. _framework_proxymanager:

=======================
The ProxyManager system
=======================

The ProxyManager(PM) system provides users proxies management. This system allows one to upload, delete and download proxies to a DIRAC database.
It keeps uploaded long-living proxies in order to provide them for asynchronous operations performed on the user's behalf.
This system is vital, as the proxies stored here are used to run users jobs and pilots, as well as to retrieve information from VOMS.
The system also contains the logic of notification (look :ref:`Notification system <framework_notification>` of the expiration of the proxy.
It is also important to mention that the PM manages proxy extensions, both DIRAC and VOMS. The class :mod:`~DIRAC.Core.Security.VOMS` is used to add a VOMS extension.
It is also possible to obtain a limited user proxy using tokens.

Structure
=========

The system consists of a client part :mod:`~DIRAC.FrameworkSystem.Client.ProxyManagerClient` that contains client access to
the production service :mod:`~DIRAC.FrameworkSystem.Service.ProxyManagerHandler` running on the server side,
which in turn communicates with the database :mod:`~DIRAC.FrameworkSystem.DB.ProxyDB`.
There are also commands and a web portal(:ref:`read how to install it <installwebappdirac>`) interface for interaction with the user.

Scripts
-------

You can be use :ref:`DIRAC commands <proxymanager_cmd>` to interact with this system to manage proxies.
