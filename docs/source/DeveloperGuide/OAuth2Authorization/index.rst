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
There are already many articles to familiarize yourself with this framework, for example `Auth0 Docs <https://auth0.com/docs/authorization/protocols/protocol-oauth2>`_ or `RFCs <https://oauth.net/>`_

The following diagram shows the main OAuth 2.0 roles in DIRAC.

.. image:: /_static/Systems/FS/OAuth2/OAuth2Roles.png
   :alt: OAuth 2.0 roles in DIRAC (source https://github.com/TaykYoku/DIRACIMGS/raw/main/OAuth2_Roles.ai)

A feature of DIRAC is the ability to perform user tasks asynchronously on behalf of the user, i.e. using their access token or proxy certificate.

.. image:: /_static/Systems/FS/OAuth2/DIRACComponentsInteractionRoles.png
   :alt: OAuth 2.0 roles in context of the DIRAC components interation (source https://github.com/TaykYoku/DIRACIMGS/raw/main/OAuth2_Roles_ServiceAsClient.ai)

As shown in the figure, DIRAC server components, such as service or agent, may have sufficient privileges to request a user access token (or proxy). Upon receiving it, the component can access the protected resource on behalf of the user.

.. warning:: The OAuth 2.0 scheme does not involve the use of X509 certificates, but since their usage is still mandatory in DIRAC, the scheme is more complicated:
             the protected resource request may contain the X509 proxy user certificate instead of the user access token.

OAuth 2.0 roles
===============

An OAuth 2.0 flow has the following `roles <https://datatracker.ietf.org/doc/html/rfc6749#section-1.1>`_:

 - **Resource Owner** - Entity that can grant access to a protected resource. In the context of DIRAC, these are DIRAC users.
 - **Resource Server** - Server hosting the protected resources. In the context of DIRAC, this is DIRAC backend components like a DIRAC services.
 - **Client** - Application requesting access to a protected resource on behalf of the *Resource Owner*. In the context of DIRAC, these are DIRAC client installations. The client may also be a DIRAC component, such as a service or agent, that uses a user access token to access DIRAC services.
 - **Authorization Server** - Server that authenticates the *Resource Owner* and issues access tokens after getting proper authorization. In the context of DIRAC, this is DIRAC Authorization Server.

OAuth 2.0 grants
================

OAuth 2.0 defines flows to get an access token, called `grant types <https://datatracker.ietf.org/doc/html/rfc6749#section-1.3>`_. We use the following flows:

 - `Device Flow <https://datatracker.ietf.org/doc/html/rfc8628>`_ to authorize with DIRAC client installation.
 - `Authorization Code Flow <https://tools.ietf.org/html/rfc6749#section-1.3.1>`_ to authorize with browser.
 - `Client Credentials <https://tools.ietf.org/html/rfc6749#section-4.4>`_ to authorize Web portal and to interact with third party authorization services.
 - `Refresh Token <https://tools.ietf.org/html/rfc6749#section-1.5>`_ to implement long sessions for DIRAC clients and to refresh users access tokens.
 - `Token Exchange <https://datatracker.ietf.org/doc/html/rfc8693>`_ to get access tokens from third party Identity Providers with scope needed for a particular case.

.. warning:: DIRAC components can use the host certificate as Client Credentials, which goes beyond the OAuth 2.0 scheme.

*******************
Involved components
*******************

 - DIRAC **Authorization Server** (AS) - acts as an authorization server for DIRAC clients by providing users with access tokens and proxies
 - command line interface (CLI) - commands for creating a DIRAC work session, see :ref:`dirac-login`, :ref:`dirac-logout`, :ref:`dirac-configure`.
 - **Authorization API** endpoints - OAuth2 endpoints for authorization, receiving a response from Identity Provider, obtaining an access token, etc., see :py:class:`~DIRAC.FrameworkSystem.API.AuthHandler.AuthHandler`.
 - **Token Manager** (TM) service - Service that takes care of storing, updating and obtaining new user access tokens. Similar to the Proxy Manager service, but differs in the specifics of working with tokens.
 - **Identity Provider** (IdP) - a type of DIRAC resource that allows you to describe the interaction with third-party services that manage user accounts.
 - also the **tornado framework** containing the logic of authorizing client requests to DIRAC components, which in turn act as a resource.


.. _dirac_as:

DIRAC Authorization Server
==========================

This component is based on the popular `authlib <https://docs.authlib.org/en/latest/oauth/2/index.html>`_ python3 library.
The necessary components for DIRAC Authorization Server to work are collected in a :py:mod:`~DIRAC.FrameworkSystem.private.authorization` subpackage.

.. image:: /_static/Systems/FS/OAuth2/AuthorizationServerPackage.png
   :alt: DIRAC Authorization Server structure in a subpackage (source https://github.com/TaykYoku/DIRACIMGS/raw/main/Authorization_server_structure.ai)

Components
----------

 - :py:class:`~DIRAC.FrameworkSystem.private.authorization.grants` contains helper classes with descriptions of the flows to get and revoke an access token.
 - :py:class:`~DIRAC.FrameworkSystem.private.authorization.utils` contains helper classes with main OAuth2 object descriptions and helper methods.
 - :py:class:`~DIRAC.FrameworkSystem.private.authorization.AuthServer` inherits from `authlib.oauth2.AuthorizationServer` and simulates the operation of OAuth 2 authorization server.


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

With a new system component - :ref:`APIs <apis>`, was created Authorization API for *Framework* system (see :py:class:`~DIRAC.FrameworkSystem.API.AuthHandler`) which provides the necessary endpoints for interaction with DIRAC AS.


Token Manager
=============

The Token Manager service aims to capture access tokens and refresh user tokens upon successful authorization and manage them, issue access tokens upon request of DIRAC services or user-owners.


Identity Provider
=================

Since DIRAC is not going to perform the function of user account management, it delegates this function as much as possible to third parties services where VOs should be registered and where there are VO administrators who will deal with it.
Such resources are described as `IdProviders`, see :ref:`resourcesIdProvider`.


Tornado Framework
=================

The framework has also been modified, adding the ability to access DIRAC services using access tokens, see :py:class:`~DIRAC.Core.Tornado.Client.private.TornadoBaseClient.TornadoBaseClient` and :py:class:`~DIRAC.Core.Tornado.Server.private.BaseRequestHandler.BaseRequestHandler`.

.. note:: to use the received access token to access DIRAC services, you need to add ``/DIRAC/Security/UseTokens=true`` or ``export DIRAC_USE_ACCESS_TOKEN=true``.


**********
Logging in
**********

Consider process by which an user gains access to a DIRAC resources by identifying and authenticating themselves.

DIRAC CLI
=========

The ``dirac-login`` command will help us with this. There are three main ways to authorize:

- using a local user certificate to obtain a proxy certificate
- logging in with DIRAC AS to obtain a proxy certificate
- logging in with DIRAC AS to obtain an access token


Using ``dirac-login my_group --use-certificate``:

.. image:: /_static/Systems/FS/OAuth2/certificateFlow.png
   :alt: DIRAC CLI login with certificate flow (source https://raw.githubusercontent.com/TaykYoku/DIRACIMGS/main/component_schema_flows.drawio)

Using the local certificate ``dirac-login`` makes a similar algorithm as :ref:`dirac-proxy-init`:
  1) Generate a proxy certificate locally on the user's machine from a locally installed user certificate.
  #) Try to connect to the DIRAC Configuration Server (CS) with this proxy certificate.
  #) If the connection was successful, a command generate a proxy certificate with the required extensions.
  #) A proxy certificate without extensions upload to :py:class:`~DIRAC.FrameworkSystem.DB.ProxyDB.ProxyDB` using :py:class:`~DIRAC.FrameworkSystem.Service.ProxyManagerHandler.ProxyManagerHandler`.

Using ``dirac-login my_group --use-diracas --token``:

.. image:: /_static/Systems/FS/OAuth2/diracasTokenFlow.png
   :alt: DIRAC CLI login DIRAC AS flow and obtaining an access token (source https://raw.githubusercontent.com/TaykYoku/DIRACIMGS/main/component_schema_flows.drawio)

User do not need to have a locally installed certificate if logging in through DIRAC AS.

  1) ``dirac-login`` initializes **OAuth 2.0 Device flow** by passing DIRAC client ID to DIRAC AS.
  #) DIRAC AS responds with a ``device_code``, ``user_code``, ``verification_uri``, ``verification_uri_complete``, ``expires_in`` (lifetime in seconds for device_code and user_code), and polling ``interval``.
  #) The command asks the user to log in using a device that has a browser(e.g.: their computer, smartphone) or if the device running ``dirac-login`` has a browser installed, a new tab with the received URL will open automatically.

    a) The command begins polling DIRAC AS for an access token sending requests to token endpoint until either the user completes the browser flow path or the user code expires.

  4) After receiving this request from the browser, DIRAC AS will initialize **OAuth 2.0 Authorization Code flow** with choosed IdP. If several IdPs are registered in DIRAC and it is not clear from the requested group which one to choose, DIRAC AS will ask the user to choose one.
  #) DIRAC AS prepare authorization URL for the corresponding IdP and redirects the user to the login and authorization prompt.
  #) When the user has successfully logged in, IdP redirects him back to the DIRAC AS with an authorization code.
  #) DIRAC AS sends this code to the IdP along with the client credentials and recieve an ID token, access token and refresh token.
  #) DIRAC AS try to parse received tokens to get the user profile and its ID.
  #) Check whether the ID is registered in the DIRAC CS Registry, if not then the authorization process is interrupted and administrators receive a message about an unregistered user.

    a) If the user is registered, :py:class:`~DIRAC.FrameworkSystem.Service.TokenManagerHandler.TokenManagerHandler` stores tokens in :py:class:`~DIRAC.FrameworkSystem.DB.TokenDB.TokenDB`.
    #) If ``TokenDB`` already contains tokens for the user, then the extra tokens are revoked (just one refresh token in Token Manager for the user is enough).

  10) DIRAC AS update authorization session status.
  11) Here we **back to OAuth 2.0 Device flow**. Upon receipt of a request for an access token, DIRAC AS requests :py:class:`~DIRAC.FrameworkSystem.Service.TokenManagerHandler.TokenManagerHandler` to provide a fresh access token to the requested user and group.

    a) Token Manager forms a scope that corresponds to the selected group.
    #) After that Token Manager makes aexchange token request to get new access and refresh tokens.
    #) DIRAC AS encrypts the refresh token and stores it in :py:class:`~DIRAC.FrameworkSystem.DB.AuthDB.AuthDB`.
    #) DIRAC AS responds with an access and encripted refresh token.

Using ``dirac-login my_group --use-diracas --proxy``:

.. image:: /_static/Systems/FS/OAuth2/diracasProxyFlow.png
   :alt: DIRAC CLI login DIRAC AS flow and obtaining a proxy (source https://raw.githubusercontent.com/TaykYoku/DIRACIMGS/main/component_schema_flows.drawio)

In this case, the process differs only in that when the user successfully completes the browser flow path, DIRAC AS responds with a proxy:
  11) Upon receipt of a request for a proxy, DIRAC AS requests :py:class:`~DIRAC.FrameworkSystem.Service.ProxyManagerHandler.ProxyManagerHandler` to provide a proxy to the requested user and group.

    a) Proxy Manager see if you need a VOMS extension for the selected group.
    #) Proxy Manager makes ``voms-proxy-init`` with the required flags if a VOMS extension is required and add DIRAC group extension.
    #) DIRAC AS responds with a proxy.

Web portal
==========

.. image:: /_static/Systems/FS/OAuth2/WebAppLoginFlow.png
   :alt: DIRAC web login flow (source https://raw.githubusercontent.com/TaykYoku/DIRACIMGS/main/component_schema_flows.drawio)

The diagram shows the following steps:
  1) The user selects an identity provider for authorization in the web portal.
  #) After receiving this request from the browser, the web server creates an authorization session, and redirects the user to DIRAC AS by initiating the **OAuth 2.0 Authorization Code flow**.
  #) DIRAC AS will initialize **OAuth 2.0 Authorization Code flow** with choosed IdP.
  #) When the user has successfully logged in, DIRAC AS redirects him back to the web server with an authorization code.
  #) Web server sends this code to the DIRAC AS along with the client credentials and recieve an access and refresh tokens.
  #) The web server creates an http only secure cookie with the received tokens and store an access token in sessionStorage (see https://www.w3schools.com/jsref/prop_win_sessionstorage.asp for more details). This token can be used by JS code from the user's browser (currently not used).

This scheme is being revised to simplify it.

***********
Logging out
***********

Consider process by which an user end work session with DIRAC.

DIRAC CLI
=========

Using ``dirac-logout``:

.. image:: /_static/Systems/FS/OAuth2/revokeToken.png
   :alt: DIRAC logout flow (source https://raw.githubusercontent.com/TaykYoku/DIRACIMGS/main/component_schema_flows.drawio)

If it is a long session, i.e. with a refresh token, which allows you to update the access token and thus continue the working session, then to end the session it is necessary to revoke the refresh token:
  1) :ref:`dirac-logout` sends a revoke request to DIRAC AS.

    a) DIRAC AS decrypts the refresh token and reads to whom it belongs.
    #) DIRAC AS makes a revoke request to the appropriate IdP.
    #) DIRAC AS removes the record about this refresh token from the ``AuthDB`` database.

  2) Delete the token file.

Web portal
==========

Click on the username to select "Log out".

.. image:: /_static/Systems/FS/OAuth2/revokeTokenWeb.png
   :alt: DIRAC web logout flow (source https://raw.githubusercontent.com/TaykYoku/DIRACIMGS/main/component_schema_flows.drawio)

The web server receives a request from the user's browser to end the session and made revoke refresh token request to DIRAC AS. After that cleans cookies.
