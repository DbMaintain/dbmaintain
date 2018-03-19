[![Build Status](https://travis-ci.org/DbMaintain/dbmaintain.svg?branch=master)](https://travis-ci.org/DbMaintain/dbmaintain)

DbMaintain
==========

Fork of [DbMaintain](http://www.dbmaintain.org/overview.html) with new Features.

Docker
======

DbMaintain Docker container available at https://hub.docker.com/r/dbmaintain/dbmaintain/.

The container can checkout the DbMaintain configuration files (properties file and sql scripts) from scm system or they can be given to the container by mounting volumes.

#### Container's environment variables:
##### SQL_GIT_URL
If a git url is given to the container via this variable, the container will clone the git repo to /sql
##### SQL_SVN_URL
A subversion URL given to the container results in a svn export to /sql
##### PROPERTIES_GIT_URL
The git repo will be cloned to /dbmaintain
##### PROPERTIES_SVN_URL
svn export to /dbmaintain
##### DBMAINTAIN_PROPERTIES_PATH
If one of the PROPERTIES URLs is given and this variable is not set the container expects the properties file at /dbmaintain/dbmaintain.properties.
Configure different relative paths with this variable, e.g. DBMAINTAIN_PROPERTIES_PATH=myconfigs/myschema/myschema.properties will use the file /dbmaintain/myconfigs/myschema/myschema.properties
##### MAX_RETRY
The container will invoke dbmaintain up to MAX_RETRY times if an error occured, waiting 20 seconds between the invocations. Useful e.g. if a database container is starting up and we want to retry until the database is ready.
##### SLEEP_SUCCESS
The container will sleep forever if dbmaintain was successful and this variable is set to an arbitrary value. This is useful e.g. when the container is bundled with a database container in a kubernetes pod.
##### DBMAINTAIN_SYSTEM_PROPERTIES
Allows to set dbmaintain properties via system properties. Via this variable you can override properties from the properties file, e.g. -Ddatabase.userName=myuser -Ddatabase.password=secret -DdbMaintainer.script.locations=/sql/myschema/

#### Examples
using volumes to provide sql and properties
```
docker run --rm -v <path_schema_dbmaintain.properties>/dbmaintain.properties:/dbmaintain.properties -v <path>/schema/:<dbmaintain.properties#dbMaintainer.script.locations>  dbmaintain/dbmaintain updateDatabase
```
using git to provide sql and properties
```
docker run --rm -e 'SQL_GIT_URL=--branch 0.0.1 http://myuser:mypassword@git.my-company.de/dbschema.git' -e 'PROPERTIES_GIT_URL=--branch 0.0.1 http://myuser:mypassword@git.my-company.de/dbmaintainproperties.git' -e 'DBMAINTAIN_PROPERTIES_PATH=my_relative_path_in_properties_scm_repo/testschema.properties' dbmaintain/dbmaintain updateDatabase
```
using svn to provide sql and properties
```
docker run --rm -e 'SQL_SVN_URL=--username myuser --password mysecret http://svn.my-company.de/svn/dbschemas/xyz/tags/0.0.1' -e 'PROPERTIES_SVN_URL=--username myuser --password mysecret http://svn.my-company.de/svn/dbmaintainconfigs/xyz/tags/0.0.1' -e 'DBMAINTAIN_PROPERTIES_PATH=my_relative_path_in_properties_scm_repo/testschema.properties' dbmaintain/dbmaintain updateDatabase
```
override settings from properties file with system properties:
```
docker run --rm -e 'DBMAINTAIN_SYSTEM_PROPERTIES=-DdbMaintainer.script.locations=/sql/dbmaintain/sql -Ddatabase.url=jdbc:oracle:thin:@localhost:1521:XE -Ddatabase.userName=mydbuser -Ddatabase.password=secretpassword' -e 'SQL_GIT_URL=--branch 0.0.1 http://myuser:mypassword@git.my-company.de/dbschema.git' -e 'PROPERTIES_GIT_URL=--branch 0.0.1 http://myuser:mypassword@git.my-company.de/dbmaintainproperties.git' -e 'DBMAINTAIN_PROPERTIES_PATH=my_relative_path_in_properties_scm_repo/testschema.properties' dbmaintain/dbmaintain updateDatabase
```
retry up to 10 times in case of error
```
docker run --rm -e 'MAX_RETRY=10' -e 'SQL_GIT_URL=--branch 0.0.1 http://myuser:mypassword@git.my-company.de/dbschema.git' -e 'PROPERTIES_GIT_URL=--branch 0.0.1 http://myuser:mypassword@git.my-company.de/dbmaintainproperties.git' -e 'DBMAINTAIN_PROPERTIES_PATH=my_relative_path_in_properties_scm_repo/testschema.properties' dbmaintain/dbmaintain updateDatabase
```
do not exit but wait forever if successful
```
docker run --rm -e 'SLEEP_SUCCESS=yes' -e 'MAX_RETRY=10' -e 'SQL_GIT_URL=--branch 0.0.1 http://myuser:mypassword@git.my-company.de/dbschema.git' -e 'PROPERTIES_GIT_URL=--branch 0.0.1 http://myuser:mypassword@git.my-company.de/dbmaintainproperties.git' -e 'DBMAINTAIN_PROPERTIES_PATH=my_relative_path_in_properties_scm_repo/testschema.properties' dbmaintain/dbmaintain updateDatabase
``` 
