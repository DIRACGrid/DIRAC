"""Basic setuptools script for DIRACDocs."""
import os
import glob

# Actual setuptools
from setuptools import setup, find_packages

# Find the base dir where the setup.py lies
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), 'diracdoctools'))

# Take all the packages but the scripts and tests
ALL_PACKAGES = find_packages(where=BASE_DIR, exclude=['*test*'])

PACKAGE_DIR = dict(("%s" % p, os.path.join(BASE_DIR, p.replace('.', '/'))) for p in ALL_PACKAGES)

# We rename the packages so that they contain diracdoctools
ALL_PACKAGES = ['diracdoctools.%s' % p for p in ALL_PACKAGES]
ALL_PACKAGES.insert(0, 'diracdoctools')

PACKAGE_DIR['diracdoctools'] = BASE_DIR
# The scripts to be distributed
SCRIPTS = glob.glob('%s/scripts/*.py' % BASE_DIR)

setup(
    name='diracdoctools',
    version='6.19.2',
    url='https://github.com/DIRACGRID/DIRAC/docs',
    license='GPLv3',
    package_dir=PACKAGE_DIR,
    packages=ALL_PACKAGES,
    scripts=SCRIPTS,
)
