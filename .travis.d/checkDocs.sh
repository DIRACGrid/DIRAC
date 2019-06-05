sudo apt-get install graphviz;
cd docs;
# need to have this folder in the PYTHONPATH so we can find diracdoctools
export PYTHONPATH=$PWD:$PYTHONPATH

SPHINXOPTS=-wsphinxWarnings make htmlall

# check for :param / :return in html, points to faulty syntax, missing empty lines, etc.
grep --color -nH -e :param -e :return -r build/html/CodeDocumentation/ >> sphinxWarnings

# Check that sphinxWarnings is not empty
if [ -s sphinxWarnings ]; then
    echo "********************************************************************************"
    echo "Warnings When Creating Doc:"
    echo "********************************************************************************"
    cat sphinxWarnings
    echo "********************************************************************************"
    exit 1
fi
