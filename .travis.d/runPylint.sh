#!/bin/bash

if [[ "${CHECK}" == "pylintPY3K"  ]]; then
  sed -i "s/load-plugins=/load-plugins=caniusepython3.pylint_checker/" .pylintrc
  find . -name "*.py" -and -not -name 'pep8_*' -exec pylint -E --rcfile=.travis.d/.pylintrc3k --py3k --msg-template="{path}:{line}: [{msg_id}({symbol}), {obj}] {msg}" --extension-pkg-whitelist=GSI,numpy {} +
else
  find . -name "*.py" -and -not -name 'pep8_*' -exec pylint -E --rcfile=.pylintrc --msg-template="{path}:{line}: [{msg_id}({symbol}), {obj}] {msg}" --extension-pkg-whitelist=GSI,numpy {} +
fi
