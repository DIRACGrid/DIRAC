======================
dirac-configure
======================

  Main script to write dirac.cfg for a new DIRAC installation and initial download of CAs and CRLs

Usage::

  dirac-configure [option|cfgfile] ...

 

 

Options::

  -S:  --Setup=          : Set <setup> as DIRAC setup 

  -C:  --ConfigurationServer= : Set <server> as DIRAC configuration server 

  -I   --IncludeAllServers : include all Configuration Servers 

  -n:  --SiteName=       : Set <sitename> as DIRAC Site Name 

  -N:  --CEName=         : Determiner <sitename> from <cename> 

  -V:  --VO=             : Set the VO name 

  -W:  --gateway=        : Configure <gateway> as DIRAC Gateway for the site 

  -U   --UseServerCertificate : Configure to use Server Certificate 

  -H   --SkipCAChecks    : Configure to skip check of CAs 

  -D   --SkipCADownload  : Configure to skip download of CAs 

  -v   --UseVersionsDir  : Use versions directory 

  -A:  --Architecture=   : Configure /Architecture=<architecture> 

  -L:  --LocalSE=        : Configure LocalSite/LocalSE=<localse> 

  -F   --ForceUpdate     : Force Update of dirac.cfg (otherwise nothing happens if dirac.cfg already exists) 


