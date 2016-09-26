==============================
dirac-compile-externals
==============================

Compile DIRAC externals (does not require DIRAC code)

Usage::

  dirac-compile-externals [options]...

Options::

  -D:  --destination=    : Destination where to build the externals

  -t:  --type=           : Type of compilation (default: client)

  -e:  --externalsPath=  : Path to the externals sources

  -v:  --version=        : Version of the externals to compile (default will be the latest commit)

  -i:  --pythonVersion=  : Python version to compile (default 26)

  -f   --fixLinksOnly    : Only fix absolute soft links

  -j:  --makeJobs=       : Number of make jobs, by default is 1

