===============
dirac-configure
===============

Main script to write dirac.cfg for a new DIRAC installation and initial download of CAs and CRLs

Usage::

  dirac-configure [option|cfgfile] ...



Options::

  -S  --Setup <value>          : Set <setup> as DIRAC setup
  -e  --Extensions <value>     : Set <extensions> as DIRAC extensions
  -C  --ConfigurationServer <value>  : Set <server> as DIRAC configuration server
  -I  --IncludeAllServers      : include all Configuration Servers
  -n  --SiteName <value>       : Set <sitename> as DIRAC Site Name
  -N  --CEName <value>         : Determiner <sitename> from <cename>
  -V  --VO <value>             : Set the VO name
  -W  --gateway <value>        : Configure <gateway> as DIRAC Gateway for the site
  -U  --UseServerCertificate   : Configure to use Server Certificate
  -H  --SkipCAChecks           : Configure to skip check of CAs
  -D  --SkipCADownload         : Configure to skip download of CAs
  -M  --SkipVOMSDownload       : Configure to skip download of VOMS info
  -v  --UseVersionsDir         : Use versions directory
  -A  --Architecture <value>   : Configure /Architecture=<architecture>
  -L  --LocalSE <value>        : Configure LocalSite/LocalSE=<localse>
  -F  --ForceUpdate            : Force Update of cfg file (i.e. dirac.cfg) (otherwise nothing happens if dirac.cfg already exists)
  -O  --output <value>         : output configuration file
