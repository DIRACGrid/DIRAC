========================================
dirac-create-distribution-tarball
========================================

  Create tarballs for a given DIRAC release

Usage::

  dirac-create-distribution-tarball <option> ...

  A source, name and version are required to build the tarball

  For instance:

     dirac-create-distribution-tarball -n DIRAC -v v1r0 -z svn -u http://svnweb.cern.ch/guest/dirac/DIRAC/tags/DIRAC/v1r0 

 

Options::

  -v:  --version=        : version to tar 

  -u:  --source=         : VCS path to retrieve sources from 

  -D:  --destination=    : Destination where to build the tar files 

  -n:  --name=           : Tarball name 

  -z:  --vcs=            : VCS to use to retrieve the sources (try to find out if not specified) 

  -b:  --branch=         : VCS branch (if needed) 

  -p:  --path=           : VCS path (if needed) 

  -K:  --releasenotes=   : Path to the release notes 

  -A   --notesoutside    : Leave a copy of the compiled release notes outside the tarball 


