===================
dirac-rss-set-token
===================

DIRAC v6r20-pre17

Script that helps setting the token of the elements in RSS.
It can acquire or release the token.

If the releaseToken switch is used, no matter what was the previous token, it will be set to rs_svc (RSS owns it).
If not set, the token will be set to whatever username is defined on the proxy loaded while issuing
this command. In the second case, the token lasts one day.

Usage::

  dirac-rss-token --element=[Site|Resource] --name=[name] --reason=[some reason]

Options::

  --element=               : Element family to be Synchronized ( Site, Resource or Node )
  --name=                  : Name, name of the element where the change applies
  --statusType=            : StatusType, if none applies to all possible statusTypes
  --reason=                : Reason to set the Status
  --days=                  : Number of days the token is acquired
  --releaseToken           : Release the token and let the RSS take control
