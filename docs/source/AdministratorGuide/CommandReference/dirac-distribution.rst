=========================
dirac-distribution
=========================

  Create tarballs for a given DIRAC release

Usage::

  dirac-distribution [option|cfgfile] ...

 

 

Options::

  -r:  --releases=       : releases to build (mandatory, comma separated) 

  -l:  --project=        : Project to build the release for (DIRAC by default) 

  -D:  --destination     : Destination where to build the tar files 

  -i:  --pythonVersion   : Python version to use (25/26) 

  -P   --ignorePackages  : Do not make tars of python packages 

  -C:  --relcfg=         : Use <file> as the releases.cfg 

  -b   --buildExternals  : Force externals compilation even if already compiled 

  -B   --ignoreExternals : Skip externals compilation 

  -t:  --buildType=      : External type to build (client/server) 

  -x:  --externalsLocation= : Use externals location instead of downloading them 

  -j:  --makeJobs=       : Make jobs (default is 1) 

  -M:  --defaultsURL=    : Where to retrieve the global defaults from 


