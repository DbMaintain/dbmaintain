#!/bin/sh
# ----------------------------------------------------------------------------
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
# ----------------------------------------------------------------------------

# ----------------------------------------------------------------------------
# DbMaintain Start Up Shell script
#
# Required ENV vars:
# ------------------
#   JAVA_HOME - location of a JDK home dir
#
# Optional ENV vars
# -----------------
#   DBMAINTAIN_JDBC_DRIVER - JDBC driver library to be used by DbMaintain. May optionally be multiple jars separated by semicolons.
#        Preferably, this variable is set in the script setJdbcDriver.sh.
#   DBMAINTAIN_HOME - location of dbmaintain's installed home dir
#   DBMAINTAIN_OPTS - parameters passed to the Java VM when running DbMaintain
#     e.g. to debug DbMaintain itself, use
#       set DBMAINTAIN_OPTS=-Xdebug -Xnoagent -Djava.compiler=NONE -Xrunjdwp:transport=dt_socket,server=y,suspend=y,address=8000
# ----------------------------------------------------------------------------

# This shell script is based on the shell script for starting maven2 (mvn)

QUOTED_ARGS=""
while [ "$1" != "" ] ; do

  QUOTED_ARGS="$QUOTED_ARGS $1"
  shift

done

if [ -f /etc/dbmaintainrc ] ; then
  . /etc/dbmaintainrc
fi

if [ -f "$HOME/.dbmaintainrc" ] ; then
  . "$HOME/.dbmaintainrc"
fi

# OS specific support.  $var _must_ be set to either true or false.
cygwin=false;
darwin=false;
mingw=false
case "`uname`" in
  CYGWIN*) cygwin=true ;;
  MINGW*) mingw=true;;
  Darwin*) darwin=true 
           if [ -z "$JAVA_VERSION" ] ; then
             JAVA_VERSION="CurrentJDK"
           else
             echo "Using Java version: $JAVA_VERSION"
           fi
           if [ -z "$JAVA_HOME" ] ; then
             JAVA_HOME=/System/Library/Frameworks/JavaVM.framework/Versions/${JAVA_VERSION}/Home
           fi
           ;;
esac

if [ -z "$JAVA_HOME" ] ; then
  if [ -r /etc/gentoo-release ] ; then
    JAVA_HOME=`java-config --jre-home`
  fi
fi

if [ -z "$DBMAINTAIN_HOME" ] ; then
  ## resolve links - $0 may be a link to dbmaintain's home
  PRG="$0"

  # need this for relative symlinks
  while [ -h "$PRG" ] ; do
    ls=`ls -ld "$PRG"`
    link=`expr "$ls" : '.*-> \(.*\)$'`
    if expr "$link" : '/.*' > /dev/null; then
      PRG="$link"
    else
      PRG="`dirname "$PRG"`/$link"
    fi
  done

  saveddir=`pwd`

  DBMAINTAIN_HOME=`dirname "$PRG"`/..

  # make it fully qualified
  DBMAINTAIN_HOME=`cd "$DBMAINTAIN_HOME" && pwd`

  cd "$saveddir"
  # echo Using dbmaintain at $DBMAINTAIN_HOME
fi

# For Cygwin, ensure paths are in UNIX format before anything is touched
if $cygwin ; then
  [ -n "$DBMAINTAIN_HOME" ] &&
    DBMAINTAIN_HOME=`cygpath --unix "$DBMAINTAIN_HOME"`
  [ -n "$JAVA_HOME" ] &&
    JAVA_HOME=`cygpath --unix "$JAVA_HOME"`
fi

# For Migwn, ensure paths are in UNIX format before anything is touched
if $mingw ; then
  [ -n "$DBMAINTAIN_HOME" ] &&
    DBMAINTAIN_HOME="`(cd "$DBMAINTAIN_HOME"; pwd)`"
  [ -n "$JAVA_HOME" ] &&
    JAVA_HOME="`(cd "$JAVA_HOME"; pwd)`"
  # TODO classpath?
fi

if [ -z "$JAVACMD" ] ; then
  if [ -n "$JAVA_HOME"  ] ; then
    if [ -x "$JAVA_HOME/jre/sh/java" ] ; then
      # IBM's JDK on AIX uses strange locations for the executables
      JAVACMD="$JAVA_HOME/jre/sh/java"
    else
      JAVACMD="$JAVA_HOME/bin/java"
    fi
  else
    JAVACMD="`which java`"
  fi
fi

if [ ! -x "$JAVACMD" ] ; then
  echo "Error: JAVA_HOME is not defined correctly."
  echo "  We cannot execute $JAVACMD"
  exit 1
fi

if [ -z "$JAVA_HOME" ] ; then
  echo "Warning: JAVA_HOME environment variable is not set."
fi

DBMAINTAIN_LAUNCHER="org.dbmaintain.launch.commandline.CommandLine"
DBMAINTAIN_JAR="${DBMAINTAIN_HOME}/lib/dbmaintain-1.1.jar"
COMMONS_LOGGING_JAR="${DBMAINTAIN_HOME}/lib/commons-logging-1.1.1.jar"

# Check if $DBMAINTAIN_JDBC_DRIVER is set. If not, call setJdbcDriver.sh.
if [ -z "$DBMAINTAIN_JDBC_DRIVER" ] ; then
  if [ -f "$DBMAINTAIN_HOME/bin/setJdbcDriver.sh" ] ; then
    . "$DBMAINTAIN_HOME/bin/setJdbcDriver.sh"
  else
    . setJdbcDriver.sh
  fi
else
  JDBC_DRIVER="$DBMAINTAIN_JDBC_DRIVER"
fi

# For Cygwin, switch paths to Windows format before running java
if $cygwin; then
  [ -n "$DBMAINTAIN_HOME" ] &&
    DBMAINTAIN_HOME=`cygpath --path --windows "$DBMAINTAIN_HOME"`
  [ -n "$JAVA_HOME" ] &&
    JAVA_HOME=`cygpath --path --windows "$JAVA_HOME"`
  [ -n "$HOME" ] &&
    HOME=`cygpath --path --windows "$HOME"`
  [ -n "$DBMAINTAIN_JAR" ] &&
    DBMAINTAIN_JAR=`cygpath --path --windows "$DBMAINTAIN_JAR"`
  [ -n "$COMMONS_LOGGING_JAR" ] &&
    COMMONS_LOGGING_JAR=`cygpath --path --windows "$COMMONS_LOGGING_JAR"`
  [ -n "$JDBC_DRIVER" ] &&
    JDBC_DRIVER=`cygpath --path --windows "$JDBC_DRIVER"`
fi

if $cygwin; then
  CLASSPATH_SEPARATOR=";"
else
  CLASSPATH_SEPARATOR=":"
fi

DBMAINTAIN_CLASSPATH="${DBMAINTAIN_JAR}${CLASSPATH_SEPARATOR}${COMMONS_LOGGING_JAR}${CLASSPATH_SEPARATOR}${JDBC_DRIVER}"

exec "$JAVACMD" \
  $DBMAINTAIN_OPTS \
  -classpath "${DBMAINTAIN_CLASSPATH}" ${DBMAINTAIN_LAUNCHER} $QUOTED_ARGS
