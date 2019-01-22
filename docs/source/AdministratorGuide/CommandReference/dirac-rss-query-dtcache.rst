=======================
dirac-rss-query-dtcache
=======================

DIRAC version: v6r20-pre17

Select/Add/Delete a new DownTime entry for a given Site or Service.

Usage::

    dirac-rss-query-dtcache [option] <query>

Queries::

    [select|add|delete]

Verbosity::

    -o LogLevel=LEVEL     NOTICE by default, levels available: INFO, DEBUG, VERBOSE..

Options::

  --downtimeID=            : ID of the downtime
  --element=               : Element (Site, Service) affected by the downtime
  --name=                  : Name of the element
  --startDate=             : Starting date of the downtime
  --endDate=               : Ending date of the downtime
  --severity=              : Severity of the downtime (Warning, Outage)
  --description=           : Description of the downtime
  --link=                  : URL of the downtime announcement
  --ongoing                : To force "select" to return the ongoing downtimes
