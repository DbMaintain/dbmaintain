@echo off

@REM Validate JAVA_HOME
if "%JAVA_HOME%"=="" (
    echo Please set JAVA_HOME to match the location of your Java installation.
	exit /B 1
)
set JAVACMD = %JAVA_HOME%\bin\java

@REM Set DBMAINTAIN_LIB
if "%DBMAINTAIN_HOME%"=="" set DBMAINTAIN_LIB=lib
if not "%DBMAINTAIN_HOME%"=="" set DBMAINTAIN_LIB=%DBMAINTAIN_HOME%\lib

@REM Set classpath
set DBMAINTAIN_LAUNCH_CLASSPATH=%DBMAINTAIN_LIB%\dbmaintain-1.0.jar;%DBMAINTAIN_LIB%\commons-collections-3.2.jar;%DBMAINTAIN_LIB%\commons-dbcp-1.2.2.jar;%DBMAINTAIN_LIB%\commons-lang-2.3.jar;%DBMAINTAIN_LIB%\commons-logging-1.1.1.jar;%DBMAINTAIN_LIB%\commons-pool-1.2.jar;%DBMAINTAIN_CLASSPATH%

echo Using classpath %DBMAINTAIN_LAUNCH_CLASSPATH%
%JAVACMD% -cp "%DBMAINTAIN_LAUNCH_CLASSPATH%" org.dbmaintain.launch.commandline.CommandLine %*
