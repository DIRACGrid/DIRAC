==============================
dirac-admin-get-site-protocols
==============================

Check the defined protocols for all SEs of a given site

Usage::

  dirac-admin-get-site-protocols [option|cfgfile]

Options::

  -S  --Site <value>           : Site for which protocols are to be checked (mandatory)

Example::

  $ dirac-admin-get-site-protocols --Site LCG.IN2P3.fr

  Summary of protocols for StorageElements at site LCG.IN2P3.fr

  StorageElement               ProtocolsList

  IN2P3-disk                    file, root, rfio, gsiftp
