#!/bin/sh

set -e


if [ ! $# -eq 1 ] 
then
   echo "<schema>_dbmaintain.properties, Operation given like this one "
   echo "using volumes to provide sql and properties:"
   echo "docker run --rm -v <path_schema_dbmaintain.properties>/dbmaintain.properties:/dbmaintain.properties -v <path>/schema/:<dbmaintain.properties#dbMaintainer.script.locations>  dbmaintain/dbmaintain updateDatabase"
   echo "using git to provide sql and properties:"
   echo "docker run --rm -e 'SQL_GIT_URL=--branch 0.0.1 http://myuser:mypassword@git.my-company.de/dbschema.git' -e 'PROPERTIES_GIT_URL=--branch 0.0.1 http://myuser:mypassword@git.my-company.de/dbmaintainproperties.git' -e 'DBMAINTAIN_PROPERTIES_PATH=my_relative_path_in_properties_scm_repo/testschema.properties' dbmaintain/dbmaintain updateDatabase"
   echo "using svn to provide sql and properties:"
   echo "docker run --rm -e 'SQL_SVN_URL=--username myuser --password mysecret http://svn.my-company.de/svn/dbschemas/xyz/tags/0.0.1' -e 'PROPERTIES_SVN_URL=--username myuser --password mysecret http://svn.my-company.de/svn/dbmaintainconfigs/xyz/tags/0.0.1' -e 'DBMAINTAIN_PROPERTIES_PATH=my_relative_path_in_properties_scm_repo/testschema.properties' dbmaintain/dbmaintain updateDatabase"
   echo "override settings from properties file with system properties:"
   echo "docker run --rm -e 'DBMAINTAIN_SYSTEM_PROPERTIES=-DdbMaintainer.script.locations=/sql/dbmaintain/sql -Ddatabase.url=jdbc:oracle:thin:@localhost:1521:XE -Ddatabase.userName=mydbuser -Ddatabase.password=secretpassword' -e 'SQL_GIT_URL=--branch 0.0.1 http://myuser:mypassword@git.my-company.de/dbschema.git' -e 'PROPERTIES_GIT_URL=--branch 0.0.1 http://myuser:mypassword@git.my-company.de/dbmaintainproperties.git' -e 'DBMAINTAIN_PROPERTIES_PATH=my_relative_path_in_properties_scm_repo/testschema.properties' dbmaintain/dbmaintain updateDatabase"
   echo "retry up to 10 times in case of error"
   echo "docker run --rm -e 'MAX_RETRY=10' -e 'SQL_GIT_URL=--branch 0.0.1 http://myuser:mypassword@git.my-company.de/dbschema.git' -e 'PROPERTIES_GIT_URL=--branch 0.0.1 http://myuser:mypassword@git.my-company.de/dbmaintainproperties.git' -e 'DBMAINTAIN_PROPERTIES_PATH=my_relative_path_in_properties_scm_repo/testschema.properties' dbmaintain/dbmaintain updateDatabase"
   echo "do not exit but wait forever if successful"
   echo "docker run --rm -e 'SLEEP_SUCCESS=yes' -e 'MAX_RETRY=10' -e 'SQL_GIT_URL=--branch 0.0.1 http://myuser:mypassword@git.my-company.de/dbschema.git' -e 'PROPERTIES_GIT_URL=--branch 0.0.1 http://myuser:mypassword@git.my-company.de/dbmaintainproperties.git' -e 'DBMAINTAIN_PROPERTIES_PATH=my_relative_path_in_properties_scm_repo/testschema.properties' dbmaintain/dbmaintain updateDatabase"
   exit 1
fi

if [ -n "$SQL_GIT_URL" ]; then
  git clone $SQL_GIT_URL $HOME/sql
elif [ -n "$SQL_SVN_URL" ]; then
  svn export $SQL_SVN_URL $HOME/sql
fi


if [ -n "$PROPERTIES_GIT_URL" ]; then
  git clone $PROPERTIES_GIT_URL $HOME/dbmaintain
elif [ -n "$PROPERTIES_SVN_URL" ]; then
  svn export $PROPERTIES_SVN_URL $HOME/dbmaintain
fi  


if [ -n "$PROPERTIES_GIT_URL" ] || [ -n "$PROPERTIES_SVN_URL" ]; then
  if [ -n "$DBMAINTAIN_PROPERTIES_PATH" ]; then
    cp /dbmaintain/$DBMAINTAIN_PROPERTIES_PATH $HOME/dbmaintain.properties
  else   
    cp /dbmaintain/dbmaintain.properties $HOME/dbmaintain.properties
  fi
fi


if [ -n "$MAX_RETRY" ]; then
   n=0   
   until [ $n -ge $MAX_RETRY ]
   do
      dbmaintainreturn=0
      java -DdbMaintainer.sqlPlusScriptRunner.preScriptFilePath=$HOME/prescriptsqlpus.sql $DBMAINTAIN_SYSTEM_PROPERTIES -DdbMaintainer.sqlPlusScriptRunner.postScriptFilePath=$HOME/postscriptsqlpus.sql -Dlog4j.configuration=file:$HOME/log4j.properties -cp "/lib/*" org.dbmaintain.launch.commandline.CommandLine $1 -config $HOME/dbmaintain.properties && break
      dbmaintainreturn=$?	  
	  n=$((n+1)) 
	  echo "retry no. $n"
      sleep 20
   done  
   if [ $dbmaintainreturn -ne 0 ]; then
     echo "giving up after $n retries - exit $dbmaintainreturn"
	 exit $dbmaintainreturn
   fi
else 
  java -DdbMaintainer.sqlPlusScriptRunner.preScriptFilePath=$HOME/prescriptsqlpus.sql $DBMAINTAIN_SYSTEM_PROPERTIES -DdbMaintainer.sqlPlusScriptRunner.postScriptFilePath=$HOME/postscriptsqlpus.sql -Dlog4j.configuration=file:$HOME/log4j.properties -cp "/lib/*" org.dbmaintain.launch.commandline.CommandLine $1 -config $HOME/dbmaintain.properties
fi

if [ -n "$SLEEP_SUCCESS" ]; then
  echo "dbmaintain will sleep forever"
  sleep infinity
fi
