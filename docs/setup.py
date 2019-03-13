""" Basic setuptools script for DIRACDocs.
    Does not contain any dependency
"""

import sys
import os
import glob
# Actual setuptools
from setuptools import setup, find_packages

# Find the base dir where the setup.py lies
base_dir = os.path.abspath(os.path.dirname(__file__))

# Take all the packages but the scripts and tests
allPackages = find_packages(where=base_dir, exclude=["*test*", "*scripts*"])
print "CHRIS allPackages %s" % allPackages

# Because we want to have a 'DIRAC' base module and that the setup.py
# is lying inside it, we need to define a mapping
# < module name : directory >
# e.g. DIRAC.DataManagementSystem is base_dir/DataManagementSystem

package_dir = dict(("DIRACDocs.%s" % p, os.path.join(base_dir, p.replace('.', '/'))) for p in allPackages)
print "CHRIS package_dir %s" % package_dir

# We also rename the packages so that they contain DIRAC
allPackages = ['DIRACDocs.%s' % p for p in allPackages]
print "CHRIS allPackages %s" % allPackages

# Artificially create the 'DIRAC' package
# at the root
allPackages.insert(0, 'DIRACDocs')
package_dir['DIRACDocs'] = base_dir

# The scripts to be distributed
scripts = glob.glob('%s/Tools/scripts/*.py' % base_dir)
print "CHRIS scripts %s" % scripts
setup(
    name="DIRACDocs",
    version="6.19.0",
    url="https://github.com/DIRACGRID/DIRAC/docs",
    license="GPLv3",
    package_dir=package_dir,
    packages=allPackages,
    scripts=scripts,
)
