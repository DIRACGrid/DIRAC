.. _resourcesIdProvider:

=================
Identity Provider
=================

This type of resource describes the interaction with third-party IdP.

An identity provider (IdP) is a system entity that creates, maintains, and manages identity information for principals and also provides authentication services to relying applications within a federation or distributed network.
IdP offer user authentication as a service.

These resources are used by the following DIRAC instances:

- Framework/TokenManager service
- Framework/Auth endpoint
- web portal

.. note:: With the start of IdP use, the bulk of user management relies on VO managers through the selected IdP.

.. note:: Since not all DIRAC services support token authorization, users still need to keep a fresh proxy in ProxyManager.

The following IdP are presented here: ``OAuth2``, ``CheckIn``, ``IAM``.

------------------------
OAuth2 identity provider
------------------------

This is the base class for describing IdP that use OAuth2/OIDC, for example ``CheckIn`` and ``IAM``. It is based on the ``authlib`` library.

What is required to register OAuth2 IdP?

- first of all it is necessary to register there a confidential client, the registration process may be different for different IdPs, but in most cases it is sufficient to use the user interface on the IdP site.
  After successful registration you will receive client credentinals: ``client_id`` and ``client_secret``. They need to be added to the local configuration of the server ``dirac.cfg``.
- give token exchange (https://tools.ietf.org/html/rfc8693) permission for this client. This is necessary so that DIRAC can receive new tokens to run asynchronous user tasks without his participation.
- give refresh token permission for this client. This is necessary so that DIRAC can receive new access tokens without user participation.

-------------------------
CheckIn identity provider
-------------------------

EGI Check-in is a proxy service that operates as a central hub to connect federated Identity Providers (IdPs) with EGI service providers.

.. literalinclude:: /dirac.cfg
  :start-after: ## EGI Checkin type:
  :end-before: ##
  :dedent: 2
  :caption: /Resources/IdProviders section

---------------------
IAM identity provider
---------------------

WLCG IAM is an INDIGO identity and access management service.

.. literalinclude:: /dirac.cfg
  :start-after: ## WLCG IAM type:
  :end-before: ##
  :dedent: 2
  :caption: /Resources/IdProviders section
