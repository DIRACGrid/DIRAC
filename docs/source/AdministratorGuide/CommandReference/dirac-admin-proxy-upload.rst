========================
dirac-admin-proxy-upload
========================

Usage::

  dirac-admin-proxy-upload.py (<options>|<cfgFile>)*

Options::

  -v  --valid <value>          : Valid HH:MM for the proxy. By default is one month
  -g  --group <value>          : DIRAC Group to embed in the proxy
  -C  --Cert <value>           : File to use as user certificate
  -K  --Key <value>            : File to use as user key
  -P  --Proxy <value>          : File to use as proxy
  -f  --onthefly               : Generate a proxy on the fly
  -p  --pwstdin                : Get passwd from stdin
  -i  --version                : Print version
