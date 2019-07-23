#!/bin/bash
#...............................................................................
#
# parseArguments:
#
#   parseArguments looks at the environment variables that are prefixed with 
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
function parseArguments() {

    local DEFAULT_VARS=( ${!DEFAULT_@} )
    local DEFAULT_VAR

    echo "=> Parsing configuration"
    for DEFAULT_VAR in "${DEFAULT_VARS[@]}"; do
        local VAR_NAME=${DEFAULT_VAR#"DEFAULT_"}
	local CLIENT_VAR_NAME="CLIENT_"$VAR_NAME
	local SERVER_VAR_NAME="SERVER_"$VAR_NAME

	# Client config
	if [ ! -z "${!CLIENT_VAR_NAME}" ]; then
	    local CLIENT_VAR_VAL=${!CLIENT_VAR_NAME}
	    if [ $CLIENT_VAR_VAL == "unset" ]; then
		echo "[CLIENT] Unsetting default value ${CLIENT_VAR_VAL}"
	    else
		echo "[CLIENT] Overriding default value with ${CLIENT_VAR_VAL} for ${CLIENT_VAR_NAME}"
		writeToConfig $VAR_NAME $CLIENT_VAR_VAL $CLIENTCONFIG
	    fi
	elif [ -z "${!VAR_NAME}" ]; then
	    local DEFAULT_VAL=${!DEFAULT_VAR}
	    if [ $DEFAULT_VAL == "unset" ]; then
		echo "[CLIENT] Variable ${VAR_NAME} is unset, skipping."
		continue
	    else
		echo "[CLIENT] Setting default value ${DEFAULT_VAL} for ${VAR_NAME}"
		writeToConfig $VAR_NAME $DEFAULT_VAL $CLIENTCONFIG
	    fi
        else
	    local VAR_VAL="${!VAR_NAME}"

	    if [ $VAR_VAL == "unset" ]; then
		echo "[CLIENT] Unsetting default value ${VAR_NAME}"
	    else
		echo "[CLIENT] Using injected value ${VAR_VAL} for ${VAR_NAME}"
		writeToConfig $VAR_NAME $VAR_VAL $CLIENTCONFIG
	    fi
	fi

	# Server config
	if [ ! -z "${!SERVER_VAR_NAME}" ]; then
	    local SERVER_VAR_VAL=${!SERVER_VAR_NAME}
	    if [ $SERVER_VAR_VAL == "unset" ]; then
		echo "[SERVER] Unsetting default value ${SERVER_VAR_VAL}"
	    else
		echo "[SERVER] Overriding default value with ${SERVER_VAR_VAL} for ${SERVER_VAR_NAME}"
		writeToConfig $VAR_NAME $SERVER_VAR_VAL $SERVERCONFIG
	    fi
	elif [ -z "${!VAR_NAME}" ]; then
	    local DEFAULT_VAL=${!DEFAULT_VAR}
	    if [ $DEFAULT_VAL == "unset" ]; then
		echo "[SERVER] Variable ${VAR_NAME} is unset, skipping."
		continue
	    else
		echo "[SERVER] Setting default value ${DEFAULT_VAL} for ${VAR_NAME}"
		writeToConfig $VAR_NAME $DEFAULT_VAL $SERVERCONFIG
	    fi
        else
	    local VAR_VAL="${!VAR_NAME}"

	    if [ $VAR_VAL == "unset" ]; then
		echo "[SERVER] Unsetting default value ${VAR_NAME}"
	    else
		echo "[SERVER] Using injected value ${VAR_VAL} for ${VAR_NAME}"
		writeToConfig $VAR_NAME $VAR_VAL $SERVERCONFIG
	    fi
	fi
    done
}

#...............................................................................
#
#   Writes "export $1=$2" to $3 (Config file)
#
#...............................................................................

writeToConfig() {
    local VAR_NAME=$1
    local VAR_VAL=$2
    local CONFIG=$3
    
    if [ $VAR_NAME == "MYSQL_VER" ] || [ $VAR_NAME == "ES_VER" ]; then
	eval $VAR_NAME="${VAR_VAL}"
	export $VAR_NAME
    else
	echo "export ${1}=${2}" >> $CONFIG
    fi
}

#...............................................................................
#
#   Copies local source and test code to docker containers, if they are
#       directories.
#   Requires $CLIENTCONFIG and $SERVERCONFIG to be defined. 
#
#...............................................................................

function copyLocalSource() {
    source $CLIENTCONFIG
    if [ ! -z $TESTREPO ] && [ -d $TESTREPO ]; then
	docker exec client mkdir -p $WORKSPACE/LocalRepo/TestCode
	docker cp $TESTREPO client:$WORKSPACE/LocalRepo/TestCode
	
	sed -i "s@\(export TESTREPO=\).*@\1${WORKSPACE}/LocalRepo/TestCode/$(basename $TESTREPO)@" $CLIENTCONFIG
    fi
    if [ ! -z $ALTERNATIVE_MODULES ] && [ -d $ALTERNATIVE_MODULES ]; then
	docker exec client mkdir -p $WORKSPACE/LocalRepo/ALTERNATIVE_MODULES
	docker cp $ALTERNATIVE_MODULES client:$WORKSPACE/LocalRepo/ALTERNATIVE_MODULES
	
	sed -i "s@\(export ALTERNATIVE_MODULES=\).*@\1${WORKSPACE}/LocalRepo/ALTERNATIVE_MODULES/$(basename $ALTERNATIVE_MODULES)@" $CLIENTCONFIG
    fi

    source $SERVERCONFIG
    if [ ! -z $TESTREPO ] && [ -d $TESTREPO ]; then
	docker exec server mkdir -p $WORKSPACE/LocalRepo/TestCode
	docker cp $TESTREPO server:$WORKSPACE/LocalRepo/TestCode

	sed -i "s@\(export TESTREPO=\).*@\1${WORKSPACE}/LocalRepo/TestCode/$(basename $TESTREPO)@" $SERVERCONFIG
    fi
    if [ ! -z $ALTERNATIVE_MODULES ] && [ -d $ALTERNATIVE_MODULES ]; then
	docker exec server mkdir -p $WORKSPACE/LocalRepo/ALTERNATIVE_MODULES
	docker cp $ALTERNATIVE_MODULES server:$WORKSPACE/LocalRepo/ALTERNATIVE_MODULES
	
	sed -i "s@\(export ALTERNATIVE_MODULES=\).*@\1${WORKSPACE}/LocalRepo/ALTERNATIVE_MODULES/$(basename $ALTERNATIVE_MODULES)@" $SERVERCONFIG
    fi
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
