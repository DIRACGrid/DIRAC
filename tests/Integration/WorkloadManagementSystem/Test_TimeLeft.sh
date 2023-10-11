#!/usr/bin/env bash
#-------------------------------------------------------------------------------

# Tests that require a $DIRACSCRIPTS pointing to DIRAC deployed scripts location,
# and a $DIRAC variable pointing to an installed DIRAC
# It also assumes that pilot.cfg contains all the necessary for running

echo "\n======> Test_TimeLeft <======\n"

if [[ ! -z "$DEBUG" ]]; then
  echo '==> Running in DEBUG mode'
  DEBUG='-ddd'
else
  echo '==> Running in non-DEBUG mode'
fi

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

###############################################################################
# Can't find anywhere a batch plugin

dirac-wms-get-queue-cpu-time --cfg "${SCRIPT_DIR}/pilot.cfg" $DEBUG

if [[ "${?}" -eq 0 ]]; then
  echo -e "\nSuccess\n\n"
else
  echo -e "\nSomething wrong!\n\n" >&2
  exit 1
fi
