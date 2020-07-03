.. _dirac-proxy-init:

================
dirac-proxy-init
================

Usage::

  dirac-proxy-init.py (<options>|<cfgFile>)*

Options::

  -v  --valid <value>          : Valid HH:MM for the proxy. By default is 24 hours
  -g  --group <value>          : DIRAC Group to embed in the proxy
  -b  --strength <value>       : Set the proxy strength in bytes
  -l  --limited                : Generate a limited proxy
  -t  --strict                 : Fail on each error. Treat warnings as errors.
  -S  --summary                : Enable summary output when generating proxy
  -C  --Cert <value>           : File to use as user certificate
  -K  --Key <value>            : File to use as user key
  -u  --out <value>            : File to write as proxy
  -x  --nocs                   : Disable CS check
  -p  --pwstdin                : Get passwd from stdin
  -i  --version                : Print version
  -j  --noclockcheck           : Disable checking if time is ok
  -r  --rfc                    : Create an RFC proxy, true by default, deprecated flag
  -L  --legacy                 : Create a legacy non-RFC proxy
  -U  --upload                 : Upload a long lived proxy to the ProxyManager
  -M  --VOMS                   : Add voms extension

Example::

  $ dirac-proxy-init -g dirac_user --rfc
  Enter Certificate password:
  $
