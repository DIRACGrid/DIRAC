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
You can enable it by setting the environment variable `DIRAC_USE_M2CRYPTO` to `Yes`.

Possible issues
---------------

M2Crypto (or any standard tool that respects TLS..) will be stricter than PyGSI. So you may need to adapt your environment a bit. Here are a few hints:

* SAN in your certificates: if you are contacting a machine using its aliases, make sure that all the aliases are in the SubjectAlternativeName (SAN) field of the certificates
* FQDN in the configuration: SAN normally contains only FQDN, so make sure you use the FQDN in the CS as well (e.g. `mymachine.cern.ch` and not `mymachine`)
* ComponentInstaller screwed: like any change you do on your hosts, the ComponentInstaller will duplicate the entry. So if you change the CS to put FQDN, the machine will appear twice. 


.. _jsonSerialization:

JSON Serialization
==================

We aim at replacing the DEncode serialization over the network with json serialization. In order to be smooth, the transition needs to happen in several steps:
* DISET -> DISET: as it is now. We encode DISET, and decode DISET
* DISET -> DISET, JSON: we send DISET, try to decode with DISET, fallback to JSON if it fails. This step is to make sure all clients will be ready to understand JSON whenever it comes.
* JSON -> DISET, JSON: we send JSON, attempt to decode with DISET, fallback to JSON if it fails. This step is to make sure that if we still have some old clients lying around, we are still able to understand them.
* JSON -> JSON: final step, goodbye DISET.

The changes from one stage to the next is controlled by environment variables, and it can go to your own pace:
* `DIRAC_USE_JSON_DECODE`: must be the first one. Enables the DISET,JSON decoding
* `DIRAC_USE_JSON_ENCODE`: `DIRAC_USE_JSON_DECODE` must still be enabled ! Sends JSON instead of DISET.

The last stage (JSON only) will be the default of the following release, so before upgrading you will have to go through the previous steps.