.. _bashrc_variables:

==================================================
Environment Variables to Configure DIRAC Behaviour
==================================================

There is a small number of environment variables that can be set to control the behaviour of some DIRAC
components. These variables can either be set in the ``bashrc`` file of a **client or server** installation or set manually
when desired.

DIRAC_DEBUG_DENCODE_CALLSTACK
  If set, debug information for the encoding and decoding will be printed out

DIRAC_DEBUG_M2CRYPTO
  If ``true`` or ``yes``, print a lot of SSL debug output

DIRAC_DEBUG_STOMP
  If set, the stomp library will print out debug information

DIRAC_DEPRECATED_FAIL
  If set, the use of functions or objects that are marked ``@deprecated`` will fail. Useful for example in continuous
  integration tests against future versions of DIRAC

DIRAC_FEWER_CFG_LOCKS
  If ``true`` or ``yes`` or ``on`` or ``1`` or ``y`` or ``t``, DIRAC will reduce the number of locks used when accessing the CS for better performance (default, ``no``).

DIRAC_GFAL_GRIDFTP_SESSION_REUSE
  If set to ``true`` or ``yes`` the GRIDFT SESSION RESUSE option will be set to True, should be set on server
  installations. See the information in the :ref:`resourcesStorageElement` page.

DIRAC_USE_JSON_DECODE
  Controls the transition to JSON serialization. See the information in :ref:`jsonSerialization` page (default=Yes since v7r2)

DIRAC_USE_JSON_ENCODE
  Controls the transition to JSON serialization. See the information in :ref:`jsonSerialization` page (default=No)

DIRAC_USE_M2CRYPTO
  If anything else than ``true`` or ``yes`` (default) DIRAC will revert back to using pyGSI instead of m2crypto for handling certificates, proxies, etc.
  Unused since v7r2.

DIRAC_M2CRYPTO_SPLIT_HANDSHAKE
  If ``true`` or ``yes`` the SSL handshake is done in a new thread (default Yes)

DIRAC_M2CRYPTO_SSL_CIPHERS
  If set, overwrites the default SSL ciphers accepted. It should be a column separated list. See :py:mod:`DIRAC.Core.DISET`

DIRAC_M2CRYPTO_SSL_METHODS
  If set, overwrites the default SSL methods accepted. It should be a column separated list. See :py:mod:`DIRAC.Core.DISET`

DIRAC_NO_CFG
  If set to anything, cfg files on the command line must be passed to the command using the --cfg option.

DIRACSYSCONFIG
  If set, its value should be (the full locations on the file system of) one of more DIRAC cfg file(s) (comma separated), whose content will be used for the DIRAC configuration
  (see :ref:`dirac-cs-structure`)

DISABLE_WATCHDOG_CPU_WALLCLOCK_CHECK
  This variable only makes sense for DIRAC pilots. When set, the wallclock check done within the watchdog is disabled.

DIRAC_X509_HOST_CERT
  Defines the location of the host certificate, and takes precedence over CS options. This option is not meant to be used in the ``bashrc`` of DIRAC, but rather by external tools starting DIRAC (read orchestrators).

DIRAC_X509_HOST_KEY
  Defines the location of the host key, and takes precedence over CS options. This option is not meant to be used in the ``bashrc`` of DIRAC, but rather by external tools starting DIRAC (read orchestrators).

X509_VOMSES
  Must be set to point to a folder containing VOMSES information. See :ref:`multi_vo_dirac`

BEARER_TOKEN
  If the environment variable is set, then the value is taken to be the token contents

BEARER_TOKEN_FILE
  If the environment variable is set, then its value is interpreted as a filename. The contents of thespecified file are taken to be the token contents.
