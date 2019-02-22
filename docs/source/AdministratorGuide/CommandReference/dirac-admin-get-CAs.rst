.. _admin_dirac-admin-get-CAs:

===================
dirac-admin-get-CAs
===================

Refresh the local copy of the CA certificates and revocation lists.

Connects to the BundleDelivery service to obtain the tar balls. Needed when proxies appear to be
invalid.

Usage::

  dirac-admin-get-CAs (<options>|<cfgFile>)*

Example::

  $ dirac-admin-get-CAs
