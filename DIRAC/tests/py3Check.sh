#!/bin/bash
file="tests/py3CheckDirs.txt"
exitCode=0
while IFS= read -r directory
do
    find ${directory} -name "*.py" -and -not -name 'pep8_*' -exec pylint --rcfile=tests/.pylintrc3k --py3k --msg-template="{path}:{line}: [{msg_id}({symbol}), {obj}] {msg}" --extension-pkg-whitelist=numpy {} +
    exitCode=$(($?+exitCode))
done <"$file"

exit $exitCode
