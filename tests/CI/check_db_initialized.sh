#!/bin/bash
dbMissing=true;
allDBs=(AccountingDB FTS3DB JobDB JobLoggingDB PilotAgentsDB ProductionDB ProxyDB ReqDB ResourceManagementDB ResourceStatusDB SandboxMetadataDB StorageManagementDB TaskQueueDB TransformationDB)
while ${dbMissing};
do
    dbMissing=false;
    allExistingDBs=$(mysql -uDirac -pDirac -h mysql -P 3306 -e "show databases;");
    for db in "${allDBs[@]}";
    do
        if grep -q "${db}" <<< "${allExistingDBs}";
        then
            echo "${db} OK";
        else
            echo "${db} not created";
            dbMissing=true;
        fi;
    done;
    if ${dbMissing};
    then
        sleep 1;
    fi
done
