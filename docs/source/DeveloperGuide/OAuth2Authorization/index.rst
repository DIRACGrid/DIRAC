.. _oauth2_authorization:

####################
OAuth2 authorization
####################

`OAuth2 <https://oauth.net/2/>`_ authorization is being implemented in DIRAC for transitioning to using access tokens to manage jobs and data, and for simplifying the authorization process for users.

.. contents::

*************
The main goal
*************

The main goal is to expand the capabilities of DIRAC in terms of interoperability with third-party systems that support OAuth 2 authorization.
Enabling users to access DIRAC resources not only through a proxy certificate, but also with access tokens obtained through Identity Providers.

*******************
OAuth 2.0 framework
*******************

The OAuth 2.0 authorization framework is a protocol that allows a user to grant a third-party web site or application access to the user's protected resources, without necessarily revealing their long-term credentials or even their identity.
There are already many articles to familiarize yourself with this framefork, for example `Auth0 Docs <https://auth0.com/docs/authorization/protocols/protocol-oauth2>`_ or `RFCs <https://oauth.net/>`_

.. image:: /_static/Systems/FS/OAuth2/OAuth2Roles.png
   :alt: OAuth 2.0 roles in DIRAC.

OAuth 2.0 roles
===============

An OAuth 2.0 flow has the following `roles <https://datatracker.ietf.org/doc/html/rfc6749#section-1.1>`_:

 - **Resource Owner** - Entity that can grant access to a protected resource. In the context of DIRAC, these are DIRAC users.
 - **Resource Server** - Server hosting the protected resources. In the context of DIRAC, this is DIRAC backend components like a DIRAC services.
 - **Client** - Application requesting access to a protected resource on behalf of the *Resource Owner*. In the context of DIRAC, these are DIRAC client installations.
 - **Authorization Server** - Server that authenticates the *Resource Owner* and issues access tokens after getting proper authorization. In the context of DIRAC, this is DIRAC Authorization Server.

OAuth 2.0 grants
================

OAuth 2.0 defines flows to get an access token, called `grant types <https://datatracker.ietf.org/doc/html/rfc6749#section-1.3>`_. We use the following flows:

 - `Device Flow <https://datatracker.ietf.org/doc/html/rfc8628>`_ to authorize with DIRAC client installation.
 - `Authorization Code Flow <https://tools.ietf.org/html/rfc6749#section-1.3.1>`_ to authorize with browser.
 - `Client Credentials <https://tools.ietf.org/html/rfc6749#section-4.4>`_ to authorize Web portal and to interact with third party authorization services.
 - `Refresh Token <https://tools.ietf.org/html/rfc6749#section-1.5>`_ to implement long sessions for DIRAC clients and to refresh users access tokens.
 - `Token Exchange <https://datatracker.ietf.org/doc/html/rfc8693>`_ to get access tokens from third party Identity Providers with scopes needed for a particular case.

*******************
Involved components
*******************

 - DIRAC **Authorization Server** (AS) - acts as an authorization server for DIRAC clients by providing users with access tokens and proxies
 - command line interface (CLI) - commands for creating a DIRAC work session, see :ref:`dirac-login`, :ref:`dirac-logout`, :ref:`dirac-configure`.
 - **Authorization API** endpoints - OAuth2 endpoints for authorization, receiving a response from Identity Provider, obtaining an access token, etc., see :py:class:`~DIRAC.FrameworkSystem.API.AuthHandler.AuthHandler`.
 - **Token Manager** (TM) service - Service that takes care of storing, updating and obtaining new user access tokens. Similar to the ProxyManager service, but differs in the specifics of working with tokens.
 - **Identity Provider** (IdP) - a type of DIRAC resource that allows you to describe the interaction with third-party services that manage user accounts.
 - also the **tornado framework** containing the logic of authorizing client requests to DIRAC components, which in turn act as a resource.


.. _dirac_as:

DIRAC Authorization Server
==========================

This component is based on the popular `authlib <https://docs.authlib.org/en/latest/oauth/2/index.html>`_ python3 library.
The necessary components for DIRAC Authorization Server to work are collected in a :py:mod:`~DIRAC.FrameworkSystem.private.authorization` subpackage.

.. image:: /_static/Systems/FS/OAuth2/AuthorizationServerPackage.png
   :alt: DIRAC Authorization Server structure in a subpackage.

Components
----------

 - :py:class:`~DIRAC.FrameworkSystem.private.authorization.grants` contains helper classes with descriptions of the flows to get and revoke an access token.
 - :py:class:`~DIRAC.FrameworkSystem.private.authorization.utils` contains helper classes with main OAuth2 object descriptions and helper methods.
 - :py:class:`~DIRAC.FrameworkSystem.private.authorization.AuthServer` inherit `authlib.oauth2.AuthorizationServer` and simulates the operation of OAuth 2 authorization server.


Configuration
-------------

*Authorization Server metadata*:

  DIRAC AS should contain a `metadata <https://datatracker.ietf.org/doc/html/rfc8414>`_ that an OAuth client can use to obtain the information needed to interact with DIRAC AS, including its endpoint locations and authorization server capabilities.
  But you don't have to worry about that, just define the `/DIRAC/Security/Authorization/issuer` option in the DIRAC configuration, and everything else will be determined for you by the :py:meth:`~DIRAC.FrameworkSystem.private.authorization.utils.Utilities.collectMetadata` method.

*Authorization clients*:

  OAuth defines two types of `clients <https://tools.ietf.org/html/rfc6749#section-2.1>`_:

   - confidential clients
   - public clients

  DIRAC AS takes both into account and already has a default *public client* (see :py:class:`~DIRAC.FrameworkSystem.private.authorization.utils.Clients`) configured to authorize DIRAC client installations via the device code authorization flow mentioned earlier.
  The new `authorization client metadata <https://datatracker.ietf.org/doc/html/rfc7591#section-2>`_ can be described in the `/DIRAC/Security/Authorization/Clients` section in format::

      CLIENT_NAME
      {
        client_id=MY_CLIENT_ID
        client_secret=MY_CLIENT_SECRET
        scope=supported scopes separated by a space
        response_types=device,
        grant_types=refresh_token,
      }

*Supported scopes*:

  For DIRAC-specific authorization, support for the following scopes is implemented:

    - `g:<DIRAC group name>` this parametric scope allows you to notify which group the user selects when logging in.
    - `proxy` scope informs that the user expects to receive a proxy certificate instead of a token after successful authorization.
    - `lifetime:<proxy life time in a seconds>` scope informs how long the proxy should be.


Commands
========

Two commands were created for interaction with DIRAC AS:

 - :ref:`dirac-login`
 - :ref:`dirac-logout`

Also added the ability to authorize without a certificate while configuring the DIRAC client with the :ref:`dirac-configure` command and a special ``--login`` flag.


Authorization API
=================

With a new system component - :ref:`APIs <apis>`, was created Authorization API for =Framework= system (see :py:class:`~DIRAC.FrameworkSystem.API.AuthHandler`) which provides the necessary endpoints for interaction with DIRAC AS.


Token Manager
=============

The TokenManager service aims to capture access tokens and refresh user tokens upon successful authorization and manage them, issue access tokens upon request of DIRAC services or user-owners.


Identity Provider
=================

Since DIRAC is not going to perform the function of user account management, it delegates this function as much as possible to third parties services where VOs should be registered and where there are VO administrators who will deal with it.
Such resources are described as `IdProviders`, see :ref:`idps`.


Tornado Framework
=================

The framework has also been modified, adding the ability to access DIRAC services using access tokens, see :py:class:`~DIRAC.Core.Tornado.Client.private.TornadoBaseClient.TornadoBaseClient` and :py:class:`~DIRAC.Core.Tornado.Server.private.BaseRequestHandler.BaseRequestHandler`.

.. note:: to use the received access token to access DIRAC services, you need to add ``/DIRAC/Security/UseTokens=true`` or ``export DIRAC_USE_ACCESS_TOKEN=true``.
