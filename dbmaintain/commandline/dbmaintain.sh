# Validate JAVA_HOME
if [ "x$JAVA_HOME" != "x" ]; then
  JAVACMD="$JAVA_HOME/bin/java"
else
  echo "Please set JAVA_HOME to match the location of your Java installation."
  exit 1
fi

# Set DBMAINTAIN_HOME
if [ "x$DBMAINTAIN_HOME" = "x" ]; then
	DBMAINTAIN_HOME=`pwd`
fi

# Set DBMAINTAIN_LIB
DBMAINTAIN_LIB="$DBMAINTAIN_HOME/lib"

# Set classpath
DBMAINTAIN_LAUNCH_CLASSPATH="$DBMAINTAIN_LIB/dbmaintain-1.0.jar;$DBMAINTAIN_LIB/commons-collections-3.2.jar;$DBMAINTAIN_LIB/commons-dbcp-1.2.2.jar;$DBMAINTAIN_LIB/commons-lang-2.3.jar;$DBMAINTAIN_LIB/commons-logging-1.1.1.jar;$DBMAINTAIN_LIB/commons-pool-1.2.jar;$DBMAINTAIN_CLASSPATH"

echo "Using classpath $DBMAINTAIN_LAUNCH_CLASSPATH"
"$JAVACMD" -cp %DBMAINTAIN_LAUNCH_CLASSPATH% org.dbmaintain.launch.commandline.CommandLine "$@"
