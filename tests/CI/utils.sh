#!/bin/bash

#...............................................................................
#
# parseCommandLine:
#
#   parseCommandLine looks at the environment variables that are prefixed with 
#       DEFAULT_ and sets the unprefixed variable to the DEFAULT_* (default) value
#       if the unprefixed variable does not exists. Otherwise the unprefixed
#       variable is kept.
#   Should the variable $CONFIGFILE be defined, the parsed variables will be
#   exported to a sourceable file defined by $CONFIGFILE
#
#   Example:
#       DEFAULT_MYSQL_VER=5.7
#       MYSQL_VER not set ==> MYSQL_VER=5.7
#       ==== or ====
#       DEFAULT_MYSQL_VER=5.7
#       MYSQL_VER=8.0     ==> MYSQL_VER=8.0
#
#............................................................................... 
function parseCommandLine() {

    local DEFAULT_VARS=( ${!DEFAULT_@} )
    local DEFAULT_VAR
    
    for DEFAULT_VAR in "${DEFAULT_VARS[@]}"; do
        local VAR_NAME=${DEFAULT_VAR#"DEFAULT_"}
	
	if [ -z "${!VAR_NAME}" ]; then
	    local DEFAULT_VAL=${!DEFAULT_VAR}
	    eval $VAR_NAME="${DEFAULT_VAL}"
	    export $VAR_NAME
	    echo "Setting default value ${DEFAULT_VAL} for ${VAR_NAME}"
        else
	    local VAR_VAL="${!VAR_NAME}"
	    echo "Using injected value ${VAR_VAL} for ${VAR_NAME}"
	fi

	if [ ! -z $CONFIGFILE ]; then
	    echo "export ${VAR_NAME}=${!VAR_NAME}" >> $CONFIGFILE
	fi
    done
}


#...............................................................................
#
# getLogs:
#
#   getLogs is an utility function that moves logs from spawned docker containers
#   to the $PWD
#
#............................................................................... 
function getLogs() {
    USER=dirac
    USER_HOME=/home/$USER

    docker cp server:$USER_HOME/log.txt ./log_server_install.txt
    docker cp server:$USER_HOME/testOutputs.txt ./log_server_tests.txt
    docker cp client:$USER_HOME/log.txt ./log_client_install.txt
    docker cp client:$USER_HOME/testOutputs.txt ./log_client_tests.txt
}
