============================
dirac-rss-renew-token
============================

  Extend the duration of given token

Usage::

  dirac-rss-renew-token [option|cfgfile] <resource_name> <token_name> [<hours>]

Arguments::

  resource_name (string): name of the resource, e.g. "lcg.cern.ch"

  token_name (string): name of a token, e.g. "RS_SVC"

  hours (int, optional): number of hours (default: 24)

 

 

Options::

  -e:  --Extension=      :       Number of hours of token renewal (will be 24 if unspecified) 

