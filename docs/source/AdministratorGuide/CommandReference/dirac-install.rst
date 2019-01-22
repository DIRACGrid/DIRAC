=============
dirac-install
=============

2019-01-14 14:02:38 UTC dirac-install [NOTICE]  Processing installation requirements

Usage::

  dirac-install.py <opts> <cfgFile>

Options::

  r:  release=             : Release version to install

  l:  project=             : Project to install

  e:  extensions=          : Extensions to install (comma separated)

  t:  installType=         : Installation type (client/server)

  i:  pythonVersion=       : Python version to compile (27/26)

  p:  platform=            : Platform to install

  P:  installationPath=    : Path where to install (default current working dir)

  b   build                : Force local compilation

  g:  grid=                : lcg tools package version

      no-lcg-bundle        : lcg tools not to be installed

  B   noAutoBuild          : Do not build if not available

  v   useVersionsDir       : Use versions directory

  u:  baseURL=             : Use URL as the source for installation tarballs

  d   debug                : Show debug messages

  V:  installation=        : Installation from which to extract parameter values

  X   externalsOnly        : Only install external binaries

  M:  defaultsURL=         : Where to retrieve the global defaults from

  h   help                 : Show this help

  T:  Timeout=             : Timeout for downloads (default = %s)

      dirac-os-version=    : the version of the DIRAC OS

      dirac-os             : Enable installation of DIRAC OS

      tag=                 : release version to install from git, http or local

  m:  module=              : Module to be installed. for example: -m DIRAC or -m git://github.com/DIRACGrid/DIRAC.git:DIRAC

  s:  source=              : location of the modules to be installed

  x:  external=            : external version

Known options and default values from /defaults section of releases file
 Release =
 Project = DIRAC
 ModulesToInstall = []
 ExternalsType = client
 PythonVersion = 27
 LcgVer =
 UseVersionsDir = False
 BuildExternals = False
 NoAutoBuild = False
 Debug = False
 Timeout = 300

The main DIRAC installer script. It can be used to install the main DIRAC software, its
modules, web, rest etc. and DIRAC extensions.

In order to deploy DIRAC you have to provide: globalDefaultsURL, which is by default:
"http://lhcbproject.web.cern.ch/lhcbproject/dist/DIRAC3/globalDefaults.cfg", but it can be
in the local file system in a separate directory. The content of this file is the following::

  Installations
  {
    DIRAC
    {
       DefaultsLocation = http://lhcbproject.web.cern.ch/lhcbproject/dist/DIRAC3/defaultsDIRAC.cfg
       LocalInstallation
       {
        PythonVersion = 27
       }
       # in case you have a DIRAC extension
       LHCb
      {
      DefaultsLocation = http://lhcbproject.web.cern.ch/lhcbproject/dist/DIRAC3/defaults/lhcb.cfg
      }
    }
  }
  Projects
  {
    DIRAC
    {
      DefaultsLocation = http://lhcbproject.web.cern.ch/lhcbproject/dist/DIRAC3/defaults/dirac.cfg
    }
    # in case you have a DIRAC extension
    LHCb
    {
      DefaultsLocation = http://lhcbproject.web.cern.ch/lhcbproject/dist/DIRAC3/defaults/lhcb.cfg
    }
  }

the DefaultsLocation for example::

  DefaultsLocation = http://lhcbproject.web.cern.ch/lhcbproject/dist/DIRAC3/defaults/dirac.cfg

must contain a minimal configuration. The following options must be in this
file::

  Releases=,UploadCommand=,BaseURL=

In case you want to overwrite the global configuration file, you have to use --defaultsURL

After providing the default configuration files, DIRAC or your extension can be installed from:

1. in a directory you have to be present globalDefaults.cfg, dirac.cfg and all binaries.
   For example::

    zmathe@dzmathe zmathe]$ ls tars/
    dirac.cfg  diracos-0.1.md5  diracos-0.1.tar.gz  DIRAC-v6r20-pre16.md5  DIRAC-v6r20-pre16.tar.gz
    globalDefaults.cfg release-DIRAC-v6r20-pre16.cfg  release-DIRAC-v6r20-pre16.md5
    zmathe@dzmathe zmathe]$

   For example::

    dirac-install -r v6r20-pre16 --dirac-os --dirac-os-version=0.0.1 -u /home/zmathe/tars

   this command will use  /home/zmathe/tars directory for the source code.
   It will install DIRAC v6r20-pre16, DIRAC OS 0.1 version

2. You can use your dedicated web server or the official DIRAC web server

   for example::

    dirac-install -r v6r20-pre16 --dirac-os --dirac-os-version=0.0.1

   It will install DIRAC v6r20-pre16

3. You have possibility to install a not-yet-released DIRAC, module or extension using -m or --tag options.
   The non release version can be specified.

   for example::

    dirac-install -l DIRAC -r v6r20-pre16 -g v14r0 -t client -m DIRAC --tag=integration

   It will install DIRAC v6r20-pre16, where the DIRAC package based on integration, other other packages will be
   the same what is specified in release.cfg file in v6r20-pre16 tarball.

    dirac-install -l DIRAC -r v6r20-pre16 -g v14r0 -t client  -m DIRAC --tag=v6r20-pre22

   It installs a specific tag

   Note: If the source is not provided, DIRAC repository is used, which is defined in the global
   configuration file.

   We can provide the repository url:code repository:::Project:::branch. for example::
   
     dirac-install -l DIRAC -r v6r20-pre16 -g v14r0 -t client \
     -m https://github.com/zmathe/DIRAC.git:::DIRAC:::dev_main_branch, \
     https://github.com/zmathe/WebAppDIRAC.git:::WebAppDIRAC:::extjs6 -e WebAppDIRAC
   
   it will install DIRAC based on dev_main_branch and WebAppDIRAC based on extjs6::
   
     dirac-install -l DIRAC -r v6r20-pre16 -g v14r0 -t client \
     -m WebAppDIRAC --tag=integration -e WebAppDIRAC
   
   it will install DIRAC v6r20-pre16 and WebAppDIRAC integration branch

You can use install.cfg configuration file::

  DIRACOS = http://lhcb-rpm.web.cern.ch/lhcb-rpm/dirac/DIRACOS/
  WebAppDIRAC = https://github.com/zmathe/WebAppDIRAC.git
  DIRAC=https://github.com/DIRACGrid/DIRAC.git
  LocalInstallation
  {
    # Project = LHCbDIRAC
    # The project LHCbDIRAC is not defined in the globalsDefaults.cfg
    Project = LHCb
    Release = v9r2-pre8
    Extensions = LHCb
    ConfigurationServer = dips://lhcb-conf-dirac.cern.ch:9135/Configuration/Server
    Setup = LHCb-Production
    SkipCAChecks = True
    SkipCADownload = True
    WebAppDIRAC=extjs6
    DIRAC=rel-v6r20
  }

  dirac-install -l LHCb -r v9r2-pre8 -t server --dirac-os --dirac-os-version=0.0.6 install.cfg
