""" Basic setuptools script for DIRAC.
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

# Because we want to have a 'DIRAC' base module and that the setup.py
# is lying inside it, we need to define a mapping
# < module name : directory >
# e.g. DIRAC.DataManagementSystem is base_dir/DataManagementSystem

package_dir = dict(("DIRAC.%s" % p, os.path.join(base_dir, p.replace('.', '/'))) for p in allPackages)

# We also rename the packages so that they contain DIRAC
allPackages = ['DIRAC.%s' % p for p in allPackages]

# Artificially create the 'DIRAC' package
# at the root
allPackages.insert(0, 'DIRAC')
package_dir['DIRAC'] = base_dir

# The scripts to be distributed
scripts = glob.glob('%s/*/scripts/*.py' % base_dir)

setup(
    name="DIRAC",
    version="6.21.10",
    url="https://github.com/DIRACGRID/DIRAC",
    license="GPLv3",
    package_dir=package_dir,
    packages=allPackages,
    scripts=scripts,
)
