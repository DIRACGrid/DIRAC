.. _admin_dirac-create-distribution-tarball:

=================================
dirac-create-distribution-tarball
=================================

Create tarballs for a given DIRAC release

Usage::

  dirac-create-distribution-tarball <option> ...

  A source, name and version are required to build the tarball

  For instance::

     dirac-create-distribution-tarball -n DIRAC -v v1r0 -z svn -u http://svnweb.cern.ch/guest/dirac/DIRAC/tags/DIRAC/v1r0

Options::

  -v  --version <value>        : version to tar
  -u  --source <value>         : VCS path to retrieve sources from
  -D  --destination <value>    : Destination where to build the tar files
  -n  --name <value>           : Tarball name
  -z  --vcs <value>            : VCS to use to retrieve the sources (try to find out if not specified)
  -b  --branch <value>         : VCS branch (if needed)
  -p  --path <value>           : VCS path (if needed)
  -K  --releasenotes <value>   : Path to the release notes
  -A  --notesoutside           : Leave a copy of the compiled release notes outside the tarball
  -e  --extensionVersion <value>  : if we have an extension, we can provide the base module version (if it is needed): for example: v3r0
  -E  --extensionSource <value>  : if we have an extension we must provide code repository url
  -P  --extjspath <value>      : directory of the extjs library
