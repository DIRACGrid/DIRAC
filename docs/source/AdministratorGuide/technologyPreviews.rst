===================
Technology Previews
===================


When new technologies are introduced within DIRAC, there are cases when we allow this technology not to be used.
The reason might be that it is not completely mature yet, or that it can be disturbing. These technologies are toggled by either CS flags or environment variables.
They are meant to stay optional for a couple of releases, and then they become the default.
This page keeps a list of such technologies.

M2Crypto
========

We aim at replacing the home made wrapper of openssl pyGSI with the standard M2Crypto library. It is by default disabled.
You can enable it by setting the environment variable ``DIRAC_USE_M2CRYPTO`` to ``Yes``.
When answering a call, the service main thread delegates the work and the SSL handshake to a task thread. This is how it should be for high performance. However, this behavior was added a bit late with respect to testing, so if you want to SSL handshake to happen in the main thread, you can set `DIRAC_M2CRYPTO_SPLIT_HANDSHAKE=No`. Note that this possibility will disappear soon.

Possible issues
---------------

M2Crypto (or any standard tool that respects TLS..) will be stricter than PyGSI. So you may need to adapt your environment a bit. Here are a few hints:

* SAN in your certificates: if you are contacting a machine using its aliases, make sure that all the aliases are in the SubjectAlternativeName (SAN) field of the certificates
* FQDN in the configuration: SAN normally contains only FQDN, so make sure you use the FQDN in the CS as well (e.g. ``mymachine.cern.ch`` and not ``mymachine``)
* ComponentInstaller screwed: like any change you do on your hosts, the ComponentInstaller will duplicate the entry. So if you change the CS to put FQDN, the machine will appear twice. 

