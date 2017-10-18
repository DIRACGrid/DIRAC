#!/bin/bash

find . -name "*.py" -exec pylint -E --rcfile=.pylintrc --msg-template="{path}:{line}: [{msg_id}({symbol}), {obj}] {msg}" --extension-pkg-whitelist=GSI,numpy {} +
