.. _dirac_projects:

==============
DIRAC Projects
==============

DIRAC is used by several user communities. Some of them are creating their own modules for DIRAC. 
These modules require a certain version of DIRAC in order to function properly. Virtual organizations 
have to be able to create their own releases of their modules and install them seamlessly.
This is achieved by creating and releasing software projects in the DIRAC framework.

Preparing DIRAC distribution
@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@ 

Releases schema
---------------

DIRAC *modules* are released and distributed in *projects*. Each project has a *releases.cfg* 
configuration file where the releases, modules and dependencies are defined. A single *releases.cfg* 
can take care of one or more modules. *releases.cfg* file follows a simplified schema of DIRAC's cfg 
format. It can have several sections, nested sections and options. Section *Releases* contains the 
releases definition. Each section in the *Releases* section defines a release. The name of the 
section will be the release name. Each release will contain a list of dependencies (if any) 
and a list of modules (if more than one). An example of a *release.cfg* for a single module is 
shown below::
 
   DefaultModules = MyExt
   
   Sources
   {
     MyExt = git://somerepohosting/MyExt.git
   }
   
   Releases
   {
     v1r2p3
     {
       depends = DIRAC:v5r12
     }
   
     v1r2p2
     {
       depends = DIRAC:v5r12p1
     }
   }
   RequiredExternals
   {
     Server = tornado>=4.4.2, apache-libcloud==2.2.1
     Client = apache-libcloud==2.2.1
   }

The *DefaultModules* option (outside any section) defines what modules will be installed by default 
if there's nothing explicitly specified at installation time. Because there is only one module defined 
in *DefaultModules* each release will try to install the *MyExt* module with the same version as the 
release name. Each release can require a certain version of any other project (DIRAC is also an project).

The *RequiredExternals* section contains lists of extra python modules that can be installed with
a *pip* installer for different installation types. Each module in the lists is specified in a format
suitable to pass to the *pip* command.

An example with more than one module follows::

   DefaultModules = MyExt
   RequiredExtraModules = WebApp
   
   Sources
   {
     MyExt = git://somerepohosting/MyExt.git
     MyExtExtra = svn | http://someotherrepohosting/repos/randomname/MyExtExtra/tags
   }
   
   Releases
   {
     v1r2p3
     {
       Modules = MyExt:v1r2p1, MyExtExtra:v1r1p1
       Depends = DIRAC:v5r12p1
     }
   
     v1r2p2
     {
       Modules = MyExt:v1r2p1, MyExtExtra:v1r1
       Depends = DIRAC:v5r12
     }
   }
 
If a project requires a module that is not installed by default from another project to be installed, 
it can be defined in the *RequiredExtraModules* option. For instance, DIRAC project contains *DIRAC* 
and *Web*. But by default DIRAC project only installs DIRAC module. If another project requires the 
DIRAC Web module to be installed it can be defined in this option. That way, when installing this 
other project, Web module will also be installed.

The *Modules* option can define explicitly which modules (and their version) to install. This is useful 
if a given VO is managing more than one module. In that scenario a release can be a combination of modules 
that can evolve independently. By defining releases as groups of modules with their versions the VO can 
ensure that a release is consistent for its modules. DIRAC uses this mechanism to ensure that the DIRAC 
Web will always be installed with a DIRAC version that it works with.

The *Sources* section defines where to extract the source code from for each module. 

The releases are created using the *dirac-distribution* docker image, which can be found in GitHub package registry or in docker hub::

  docker.pkg.github.com/diracgrid/management/dirac-distribution:latest (https://github.com/DIRACGrid/management/packages/79929)
  diracgrid/dirac-distribution (https://hub.docker.com/r/diracgrid/dirac-distribution)

Pull it and run inside the dirac-distribution command::

  docker pull diracgrid/dirac-distribution
  python3 dirac-distribution.py -r v7r0p8

The above works also for DIRAC extensions, in this case just remember to specify the project name, e.g.::

  python3 dirac-distribution.py --release v10r0-pre11 --project LHCb

The *dirac-distribution* image is re-created weekly starting from the
`management repository <https://github.com/DIRACGrid/management>`_

You can also pass the releases.cfg to use via command line using the *-relcfg* switch. 
*dirac-distribution* will generate a set of tarballs, release notes in *html* and md5 files.

In the end of its execution, the *dirac-distribution* will print out a command that can be
used to upload generated release files to a predefined repository, as you read above.

*dirac-distribution* knows how to handle several types of VCS. The ones supported are:

file
 A directory in the filesystem. *dirac-distribution* will assume that the directory especified contains 
 the required module version of the module.
 
svn
 A subversion url that contains a directory with the same name as the version to be tagged. If the module 
 version is v1r0 and the url is http://host/extName, *dirac-distribution* will check out 
 http://host/extName/v1r0 and assume it contains the module contents.
 
hg
 A mercurial repository. *dirac-distribution* will check out the a tag with the same name as the module 
 version and assume it contains the module contents.
 
git
 A git repository. *dirac-distribution* will clone the repository and check out to a tag with the same 
 name as the module version and assume it contains the module contents.
 
Some of the VCS URLs may not explicitly define which VCS has to be used (for instance http://... it can 
be a subversion or mercurial repository). In that case the option value can take the form ``<vcsName> | <vcsURL>``. 
In that case *dirac-distribution* will use that VCS to check out the source code.

When installing, a project name can be given. If it is given *dirac-install* will try to install that project 
instead of the DIRAC project. *dirac-install* will have a mapping to discover where to find the *releases.cfg* 
based on the project name. Any VO can modify *dirac-install* to directly include their repositories inside 
*dirac-install* in their module source code, and use their modified version. DIRAC developers will also maintain 
a project name to *releases.cfg* location mapping in the DIRAC repository. Any VO can also notify the DIRAC 
developers to update the mapping in the DIRAC repository so *dirac-install* will automatically find the 
project's *releases.cfg* without any change to *dirac-install*.

If a project is given, all modules inside that *releases.cfg* have to start with the same name as the project. 
For instance, if *dirac-install* is going to install project LHCb, all modules inside LHCb's *releases.cfg* 
have to start with LHCb. 


How to define how to make a project distribution
------------------------------------------------

*dirac-distribution* needs to know where to find the *releases.cfg* file. *dirac-distribution* will load 
some global configuration from a DIRAC web server. That configuration can instruct *dirac-distribution* 
to load the project defaults file from a URL. Those defaults will define default values for 
*dirac-distribution* and *dirac-install* command line options. An example of a project defaults file would be:::

 #Where to load the release.cfg file from
 Releases = https://github.com/DIRACGrid/DIRAC/raw/integration/releases.cfg
 #Where to download the released tarballs from
 BaseURL = http://diracproject.web.cern.ch/diracproject/tars/
 #How to upload the release tarballs to the BaseURL
 UploadCommand = ( cd %OUTLOCATION% ; tar -cf - %OUTFILENAMES% ) | ssh webuser@webhost 'cd /diracproject/tars &&  tar -xvf - && ls *.tar.gz > tars.list'

Once the tarballs and required files have been generated by *dirac-distribution* (see below), 
if *UploadCommand* is defined the variables will be substituted and the final command printed to 
be executed by the user.

*dirac-install* will download the project files from the *BaseURL* location.

The defaults file is defined per project and can live in any web server.


Installation
@@@@@@@@@@@@

When installing, *dirac-install* requires a release version and optionally a project name. If the project 
name is given *dirac-install* will try to load the project's versioned ``release-<projectName>-<version>.cfg`` 
instead of the DIRAC's one (this file is generated by *dirac-distribution* when generating the release). 
*dirac-install* has several mechanisms on how to find the URL where the released tarballs and releases 
files for each project are. *dirac-install* will try the following steps:

1. Load DIRAC's default global locations. This file contains the default values and paths for each project 
   that DIRAC knows of and it's maintained by DIRAC developers.
2. Load the required project's defaults file. DIRAC's default global locations has defined where this file 
   is for each project. It can be in a URL that is maintained by the project's developers/maintainers.
3. If an option called *BaseURL* is defined in the project's defaults file then use that as the base URL to 
   download the releases and tarballs files for the projects.
4. If it's defined inside *dirac-install*, use it.
5. If not found then the installation is aborted.

The ``release-<projectName>-<version>.cfg`` file will specify which module and version to install. All modules 
that are defined inside a ``release-<projectName>-<version>.cfg`` will be downloaded from the same parent URL. 
For instance, if the ``release-<projectName>-<version>.cfg``  is in ``http://diracgrid.org/releases/releases.cfg`` 
and DIRAC v5r14 has to be installed, *dirac-install* will try to download it from 
``http://diracgrid.org/releases/DIRAC-v5r14.tar.gz``.

If nothing else is defined, *dirac-install* will only install the modules defined in *DefaultModules* option. 
To install other modules that are defined in the ``release-<projectName>-<version>.cfg`` the *-e* flag has to 
be used. 

Once all the modules defined in the ``release-<projectName>-<version>.cfg``  are installed. *dirac-install* 
will try to load the dependencies. The *depends* option defines on which projects the installed project 
depends on. That will trigger loading that ``release-<projectName>-<version>.cfg``  and process it as the 
main one was processed. *dirac-install* will try to resolve recursively all the dependencies either until 
all the required modules are installed or until there's a mismatch in the requirements. If after resolving 
all the ``release-<projectName>-<version>.cfg``  an module is required to be installed with more than one 
version, an error will be raised and the installation stopped.

The set of parameters used to install a project is called an *installation*. *dirac-install* also has support 
for *installations*. Each *installation* is a set of default values for *dirac-install*. If the -V switch 
is used *dirac-install* will try to load the defaults file for that installation and use those defaults for 
the arguments.


Reference of *releases.cfg*  schema
-----------------------------------

::

 #List of modules to be installed by default for the project
 DefaultModules = MyExt
 #Extra modules to be installed
 RequiredExtraModules = WebApp
 
 #Section containing where to find the source code to generate releases
 Sources
 {
   #Source URL for module MyExt
   MyExt = git://somerepohosting/MyExt.git
   MyExtExtra = svn | http://someotherrepohosting/repos/randomname/MyExtExtra/tags
 }
 
 #Section containing the list of releases
 Releases
 {
   #Release v1r2p3
   v1r2p3
   {
     #(Optional) Contains a comma separated list of modules for this release and their version in format
     # *extName(:extVersion)? (, extName(:extVersion)?)** . 
     #If this option is not defined, modules defined in *DefaultExtensions* will be installed 
     # with the same version as the release.
     Modules = MyExt:v1r2p1, MyExtExtra:v1r1p1
     
     #(Optional) Comma separated list of projects on which this project depends in format 
     # *projectName(:projectVersion)? (, projectName(:projectVersion)?)**. 
     #Defining this option triggers installation on the depended project. 
     #This is useful to install the proper version of DIRAC on which a set of modules depend.
     Depends = DIRAC:v5r12p1
   }
 
   v1r2p2
   {
     Modules = MyExt:v1r2p1, MyExtExtra:v1r1
   }
 }
 
Reference of an installation's defaults file
--------------------------------------------

::

 #(Everything in here is optional) Default values for dirac-install
 LocalInstallation
 {
   #Install the requested project instead of this one
   # Useful for setting defaults for VOs by defining them as projects and
   # using this feature to install DIRAC instead of the VO name
   Project = DIRAC
   #Release to install if not defined via command line
   Release = v1r4
   #Modules to install by default
   ModulesToInstall = MyExt
   #Type of externals to install (client, client-full, server)
   ExternalsType = client
   #Version of lcg bundle to install
   LcgVer = v14r2
   #Install following DIRAC's pro/versions schema
   UseVersionDir = False
   #Force building externals
   BuildExternals = False
   #Build externals if the required externals is not available
   BuildIfNotAvailable = False
   #Enable debug logging
   Debug = False
 }
 
 
Reference of global default's file
----------------------------------

Global defaults is the file that *dirac-install* will try to load to discover where the each project's 
``defaults.cfg`` file is. The schema is as follows::

 Projects
 {
    #Project name
    ProjectName
    { 
       #Where to find the defaults
       DefaultsLocation = http://somehost/somepath/defaultsProject.cfg
       #Release file location
       ReleasesLocation = http://endoftheworld/releases.cfg
    }
    Project2Name
    {
       DefaultsLocation = http://someotherhost/someotherpath/chunkybacon.cfg
    }
 }
 Installations
 {
   #Project name or installation name
   InstallationName
   {
     #Location of the defaults for this installation
     DefaultsLocation = http://somehost/somepath/defaultsProject.cfg
     #Default values for dirac-install
     LocalInstallation
     {
       #This section can contain the same as the LocalInstallation section in each project's defaults.cfg
     }
   }
   #And repeat for each installation or project
   OtherInstallation
   {
     ....
   }
   #Alias with another names
   ThisIsAnAlias = InstallationName
 }


All the values in the defined defaults file file take precedence over the global ones. This file is useful 
for DIRAC maintainers to keep track of all the projects installable via native dirac-install.

Common pitfalls
---------------

Installation will find a given *releases.cfg*  by looking up the project name. All modules defined inside 
a *releases.cfg*  have to start with the same name as the project. For instance, if the project is *MyVO*, 
all modules inside have to start with *MyVO*. *MyVOWeb*, *MyVOSomething* and MyVO are all valid module 
names inside a *MyVO* *releases.cfg* 
