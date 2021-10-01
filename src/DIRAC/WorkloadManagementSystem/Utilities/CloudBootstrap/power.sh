#!/bin/sh
#
# Replacement for /etc/acpi/actions/power.sh that creates a shutdown_message
#
# andrew.mcnab@cern.ch - April 2014
#

echo "100 VM received ACPI shutdown signal from hypervisor" > /etc/joboutputs/shutdown_message
/sbin/shutdown -h now
