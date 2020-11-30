.. _framework_proxymanager:

=======================
The ProxyManager system
=======================

The ProxyManager(PM) system provides users proxies management. This system allows to upload, delete and download proxies to a DIRAC database.
This system is vital, as the proxies stored here are used to run users jobs and pilots, as well as to retrieve information from VOMS.
The system also contains the logic of notification (look :ref:`Notification system <framework_notification>` of the expiration of the proxy.
It is also important to mention that the PM manages proxy extensions, both DIRAC and VOMS. It is also possible to obtain a limited use proxy using tokens.

Structure
=========

The system consists of a client part :mod:`~DIRAC.FrameworkSystem.Client.ProxyManagerClient` that contains client access to
the production service :mod:`~DIRAC.FrameworkSystem.Service.ProxyManagerHandler` running on the server side,
which in turn communicates with the database :mod:`~DIRAC.FrameworkSystem.DB.ProxyDB`.
There are also commands and a web portal(:ref:`read how to install it <installwebappdirac>`) interface for interaction with the user.
Consider each separately.

Scripts
-------

The following commands can be used to interact with this system to manage proxies:
  * :ref:`dirac-admin-get-proxy <admin_dirac-admin-get-proxy>`
  * :ref:`dirac-proxy-init <admin_dirac-proxy-init>`
  * :ref:`dirac-proxy-info <admin_dirac-proxy-info>`
  * :ref:`dirac-proxy-get-uploaded-info <admin_dirac-proxy-get-uploaded-info>`
  * :ref:`dirac-admin-users-with-proxy <admin_dirac-admin-users-with-proxy>`
  * dirac-admin-upload-proxy
  * dirac-proxy-destroy
