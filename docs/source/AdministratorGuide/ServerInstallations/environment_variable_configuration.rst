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

DIRAC_GFAL_GRIDFTP_ENABLE_IPV6
  If set to ``false`` or ``no``, disable IPv6 for the GRIDFTP plugin (default true).
  See the information in the :ref:`resourcesStorageElement` page.

DIRAC_GFAL_GRIDFTP_SESSION_REUSE
  If set to ``true`` or ``yes`` the GRIDFTP SESSION REUSE option will be set to True, should be set on server
  installations. See the information in the :ref:`resourcesStorageElement` page.

DIRAC_HTTPS_SSL_CIPHERS
  If set, overrides the default SSL ciphers accepted when using HTTPS. It should be a colon separated list.

DIRAC_HTTPS_SSL_METHOD_MAX
  If set, overrides the highest supported TLS version when using HTTPS. It should be a valid value of :py:class:`ssl.TLSVersion`.

DIRAC_HTTPS_SSL_METHOD_MIN
  If set, overrides the lowest supported TLS version when using HTTPS. It should be a valid value of :py:class:`ssl.TLSVersion`.

DIRAC_M2CRYPTO_SPLIT_HANDSHAKE
  If ``true`` or ``yes`` the SSL handshake is done in a new thread (default Yes)

DIRAC_M2CRYPTO_SSL_CIPHERS
  If set, overwrites the default SSL ciphers accepted. It should be a colon separated list. See :py:mod:`DIRAC.Core.DISET`

DIRAC_M2CRYPTO_SSL_METHODS
  If set, overwrites the default SSL methods accepted. It should be a colon separated list. See :py:mod:`DIRAC.Core.DISET`

DIRAC_MYSQL_OPTIMIZER_TRACES_PATH
  If set, it should point to an existing directory, where MySQL Optimizer traces will be stored. See :py:func:`DIRAC.Core.Utilities.MySQL.captureOptimizerTraces`

DIRAC_NO_CFG
  If set to anything, cfg files on the command line must be passed to the command using the --cfg option.

DIRAC_USE_JSON_ENCODE
  Controls the transition to JSON serialization. See the information in :ref:`jsonSerialization` page (default=Yes since 8.1)

DIRAC_ROOT_PATH
  If set, overwrites the value of DIRAC.rootPath.
  Useful for using a non-standard location for `etc/dirac.cfg`, `runit/`, `startup/`, etc.

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
  If the environment variable is set, then the value is taken to be the token contents (https://doi.org/10.5281/zenodo.3937438).

BEARER_TOKEN_FILE
  If the environment variable is set, then its value is interpreted as a filename. The content of the specified file is used as token string (https://doi.org/10.5281/zenodo.3937438).

DIRAC_USE_ACCESS_TOKEN
  If this environment is set to ``true``, then when trying to connect to the server, access tokens will be used (default=false)
