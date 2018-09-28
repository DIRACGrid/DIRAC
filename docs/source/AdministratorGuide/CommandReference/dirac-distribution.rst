==================
dirac-distribution
==================

Create tarballs for a given DIRAC release

Usage::

  dirac-distribution [option|cfgfile] ...



Options::

  -r  --releases <value>       : releases to build (mandatory, comma separated)
  -l  --project <value>        : Project to build the release for (DIRAC by default)
  -D  --destination <value>    : Destination where to build the tar files
  -i  --pythonVersion <value>  : Python version to use (27)
  -P  --ignorePackages         : Do not make tars of python packages
  -C  --relcfg <value>         : Use <file> as the releases.cfg
  -b  --buildExternals         : Force externals compilation even if already compiled
  -B  --ignoreExternals        : Skip externals compilation
  -t  --buildType <value>      : External type to build (client/server)
  -x  --externalsLocation <value>  : Use externals location instead of downloading them
  -j  --makeJobs <value>       : Make jobs (default is 1)
  -M  --defaultsURL <value>    : Where to retrieve the global defaults from
  -E  --extjspath <value>      : directory of the extjs library
