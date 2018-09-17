=======================
dirac-proxy-init
=======================

Usage::

  dirac-proxy-init.py (<options>|<cfgFile>)*



Options::

  -v:  --valid=          : Valid HH:MM for the proxy. By default is 24 hours

  -g:  --group=          : DIRAC Group to embed in the proxy

  -b:  --strength=       : Set the proxy strength in bytes

  -l   --limited         : Generate a limited proxy

  -t   --strict          : Fail on each error. Treat warnings as errors.

  -S   --summary         : Enable summary output when generating proxy

  -C:  --Cert=           : File to use as user certificate

  -K:  --Key=            : File to use as user key

  -u:  --out=            : File to write as proxy

  -x   --nocs            : Disable CS check

  -p   --pwstdin         : Get passwd from stdin

  -i   --version         : Print version

  -j   --noclockcheck    : Disable checking if time is ok

  -U   --upload          : Upload a long lived proxy to the ProxyManager

  -P   --uploadPilot     : Upload a long lived pilot proxy to the ProxyManager

  -M   --VOMS            : Add voms extension

  -r   --rfc             : Create and RFC proxy style (https://www.ietf.org/rfc/rfc3820.txt)

Example::

  $ dirac-proxy-init -g dirac_user --rfc
  Enter Certificate password:
  $
