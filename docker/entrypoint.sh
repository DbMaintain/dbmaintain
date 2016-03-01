#!/bin/sh

set -e


if [ ! $# -eq 1 ] 
then
   echo "<schema>_dbmaintain.properties, Operation given like this one "
   echo "docker run -rm -v <path_schema_dbmaintain.properties>/dbmaintain.properties:/dbmaintain.properties -v <path>/schema/:<dbmaintain.properties#dbMaintainer.script.locations>  dbmaintain/dbmaintain updateDatabase"
   exit 1
fi

java -DdbMaintainer.sqlPlusScriptRunner.preScriptFilePath=/prescriptsqlpus.sql -DdbMaintainer.sqlPlusScriptRunner.postScriptFilePath=/postscriptsqlpus.sql -Dlog4j.configuration=file:/log4j.properties -cp "/lib/*" org.dbmaintain.launch.commandline.CommandLine $1 -config /dbmaintain.properties
