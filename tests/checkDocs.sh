#!/bin/bash

sudo apt-get install graphviz;
cd docs;
# need to have this folder in the PYTHONPATH so we can find diracdoctools
export PYTHONPATH=$PWD:$PYTHONPATH

SPHINXOPTS=-wsphinxWarnings make htmlall || { echo "Failed to build documentation, check for errors above" ; exit 1; }

# check for :param / :return in html, points to faulty syntax, missing empty lines, etc.
grep --color -nH -e :param -e :return -r build/html/CodeDocumentation/ >> sphinxWarnings

# Check that sphinxWarnings is not empty
if [[ -s sphinxWarnings ]]; then
    echo "********************************************************************************"
    echo "Warnings When Creating Doc:"
    echo "********************************************************************************"
    cat sphinxWarnings
    echo "********************************************************************************"
    exit 1
fi
