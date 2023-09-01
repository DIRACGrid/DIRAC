#!/bin/bash
dbMissing=true;
allDBs=(JobDB FileCatalogDB)
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
