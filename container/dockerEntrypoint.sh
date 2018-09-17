#!/bin/bash

# Entry point of docker to setup the DIRAC environment before executing the command

source /opt/dirac/bashrc
exec "$@"
