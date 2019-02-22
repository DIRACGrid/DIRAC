.. _admin_dirac-admin-get-proxy:

=====================
dirac-admin-get-proxy
=====================

Retrieve a delegated proxy for the given user and group

Usage::

  dirac-admin-get-proxy [option|cfgfile] ... <DN|user> group

Arguments::

  DN:       DN of the user
  user:     DIRAC user name (will fail if there is more than 1 DN registered)
  group:    DIRAC group name

Options::

  -v  --valid <value>          : Valid HH:MM for the proxy. By default is 24 hours
  -l  --limited                : Get a limited proxy
  -u  --out <value>            : File to write as proxy
  -a  --voms                   : Get proxy with VOMS extension mapped to the DIRAC group
  -m  --vomsAttr <value>       : VOMS attribute to require

Example::

  $ dirac-admin-get-proxy vhamar dirac_user
  Proxy downloaded to /afs/in2p3.fr/home/h/hamar/proxy.vhamar.dirac_user
