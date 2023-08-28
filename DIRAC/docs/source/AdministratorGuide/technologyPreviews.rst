===================
Technology Previews
===================


When new technologies are introduced within DIRAC, there are cases when we allow this technology not to be used.
The reason might be that it is not completely mature yet, or that it can be disturbing. These technologies are toggled by either CS flags or environment variables.
They are meant to stay optional for a couple of releases, and then they become the default.
This page keeps a list of such technologies.

.. _jsonSerialization:

JSON Serialization
==================

We aim at replacing the DEncode serialization over the network with json serialization. In order to be smooth, the transition needs to happen in several steps:

* DISET -> DISET: as it is now. We encode DISET, and decode DISET
* DISET -> DISET, JSON: we send DISET, try to decode with DISET, fallback to JSON if it fails. This step is to make sure all clients will be ready to understand JSON whenever it comes.
* JSON -> DISET, JSON: we send JSON, attempt to decode with DISET, fallback to JSON if it fails. This step is to make sure that if we still have some old clients lying around, we are still able to understand them.
* JSON -> JSON: final step, goodbye DISET.

The changes from one stage to the next is controlled by environment variables, and it can go to your own pace:

* ``DIRAC_USE_JSON_ENCODE``: Sends JSON instead of DISET.

The last stage (JSON only) will be the default of the following release, so before upgrading you will have to go through the previous steps.

HTTPS Services
==============

The aim is to replace the DISET services with HTTPS services. The changes should be almost transparent for users/admins. However, because it is still very much in the state of preview, we do not yet describe how/what to change. If you really want to play around, please check :ref:`httpsTornado`.

OAuth2 authorization
=====================

The main idea is to start using access tokens to communicate with DIRAC systems and third-party services. Tokens can be delivered to DIRAC using Identity Providers such as Indigo IaM (WLCG) and EGI CheckIn. As a result of this integration, a new component type, named :ref:`apis` was developed.
