.. set highlighting to console input/output
.. highlight:: console

==================================
Getting User Identity
==================================

To start working with the Grid in general and with DIRAC in particular, the user should join some
grid Virtual Organization and obtain a Grid Certificate. The procedure to obtain the Grid Certificate
depends on the user's national Certification Authority (CA). The certificate is usually obtained via a
Web interface and is downloaded into the user's Web Browser. To be used with the Grid client software,
the certificate should be exported from the Browser into a file in p12 format. After that the certificate
should be converted into the pem format and stored in the user home directory. If the DIRAC client software
is available, the conversion can be done with the following DIRAC command::

  $ dirac-cert-convert <cert_file.p12>

The user will be prompted for the password used while exporting the certificate and for the pass phrase
to be used with the user's private key. Do not forget it !

Registration with DIRAC
-----------------------

Users are always working in the Grid as members of some User Community. Therefore, every user must be registered
with the Community DIRAC instance. You should ask the DIRAC administrators to do that, the procedure can
be different for different communities.

Once registered, a user becomes a member of one of the DIRAC user groups. The membership in the group
determines the user rights for various Grid operations. Each DIRAC installation defines a default user
group to which the users are attributed when the group is not explicitly specified.

Proxy initialization
--------------------

Users authenticate with DIRAC services, and therefore with the Grid services that DIRAC expose via "proxies",
which you can regard as a product of personal certificates.

There are two major differences between certificates and proxies:

- certificates are signed by a CA, while proxies can be signed by a certificate and/or by another proxy
- proxies can have extra token embedded (like macaroon of Google)

DIRAC uses *RFC* proxies, following an RFC standard (https://www.ietf.org/rfc/rfc3820.txt).

Before a user can work with DIRAC, the user's certificate proxy should be initialized and
uploaded to the DIRAC ProxyManager Service. This is achieved with a simple command::

  $ dirac-proxy-init

In this case the user proxy with the default DIRAC group will be generated and uploaded.
If another non-default user group is needed, the command becomes::

  $ dirac-proxy-init -g <user_group>

where ``user_group`` is the desired DIRAC group name for which the user is entitled.

Token authorization
-------------------

Starting with the 8.0 version of DIRAC, it is possible to authorize users through third party Identity Providers (IdP),
such as EGI Checkin [https://www.egi.eu/services/check-in/] or WLCG IAM (https://indigo-iam.github.io/v/current/).
To do this, you do not need to have a certificate if you use a terminal, the main thing is that you must be registered in one of the supported IdP. The registration process is different for each IdP.

Once your account is created, you will be able to register with DIRAC using the `dirac-login` command that will return tokens that will be used to access the services::

  dirac-login -g <user_group>

But since not all services currently support tokens, you can get a proxy if you use the *--proxy* key::

  dirac-login -g <user_group> --proxy

Note that to get a proxy you must first put it in DIRAC, see "Proxy initialization".
