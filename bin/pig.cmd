@echo off
:: Licensed to the Apache Software Foundation (ASF) under one or more
:: contributor license agreements.  See the NOTICE file distributed with
:: this work for additional information regarding copyright ownership.
:: The ASF licenses this file to You under the Apache License, Version 2.0
:: (the "License"); you may not use this file except in compliance with
:: the License.  You may obtain a copy of the License at
::
::     http://www.apache.org/licenses/LICENSE-2.0
::
:: Unless required by applicable law or agreed to in writing, software
:: distributed under the License is distributed on an "AS IS" BASIS,
:: WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
:: See the License for the specific language governing permissions and
:: limitations under the License.

:: The Pig command script
::
:: Environment Variables
::
::     JAVA_HOME           The java implementation to use.    Overrides JAVA_HOME.
::
::     PIG_CLASSPATH       Extra Java CLASSPATH entries.
::
::     HADOOP_HOME         Environment HADOOP_HOME
::
::     HADOOP_CONF_DIR     Hadoop conf dir
::
::     PIG_HEAPSIZE        The maximum amount of heap to use, in MB. 
::                                        Default is 1000.
::
::     PIG_OPTS            Extra Java runtime options.
::
::     PIG_CONF_DIR    Alternate conf dir. Default is ${PIG_HOME}/conf.
::
::     HBASE_CONF_DIR - Optionally, the HBase configuration to run against
::                      when using HBaseStorage
::

setlocal enabledelayedexpansion

set HADOOP_BIN_PATH=%HADOOP_HOME%\bin

set hadoop-config-script=%HADOOP_BIN_PATH%\hadoop-config.cmd
call %hadoop-config-script%

:main
set PIGARGS=
:ProcessCmdLine 
	if [%1]==[] goto :FinishArgs  

	if %1==--config (
    set HADOOP_CONF_DIR=%2
    shift 
		shift
    if exist %HADOOP_CONF_DIR%\hadoop-env.cmd (
      call %HADOOP_CONF_DIR%\hadoop-env.cmd
    )
		goto :ProcessCmdLine 
  )
	if %1==-useHCatalog (
        shift
        set HCAT_FLAG="true"
        goto :ProcessCmdLine 
	)
	set PIGARGS=%PIGARGS% %1
    shift
    goto :ProcessCmdLine
:FinishArgs

  if not defined PIG_HOME (
    if exist %HADOOP_HOME%\pig (
      set PIG_HOME=%HADOOP_HOME%\pig
    )
  )

  if not defined PIG_HOME (
    set PIG_HOME=%~dp0\..\
  )

  if not defined PIG_HEAPSIZE (
    set PIG_HEAPSIZE=1000
  )

  if defined PIG_HOME (
    for %%i in (%PIG_HOME%\*.jar) do (
      set CLASSPATH=!CLASSPATH!;%%i
    )
    if not defined PIG_CONF_DIR (
      set PIG_CONF_DIR=%PIG_HOME%\conf
    )
  )

  set HCAT_DEPENDCIES=
  if not defined HCAT_FLAG (
    goto HCAT_END
  )
  
  if defined HCAT_HOME (
      call :AddJar %HCAT_HOME%\share\hcatalog *hcatalog-*.jar
  ) else (
      echo "HCAT_HOME should be defined"
      exit /b 1
  )
  if defined HIVE_HOME (
      call :AddJar %HIVE_HOME%\lib hive-metastore-*.jar
      call :AddJar %HIVE_HOME%\lib libthrift-*.jar
      call :AddJar %HIVE_HOME%\lib hive-exec-*.jar
      call :AddJar %HIVE_HOME%\lib libfb303-*.jar
      call :AddJar %HIVE_HOME%\lib jdo*-api-*.jar
      call :AddJar %HIVE_HOME%\lib slf4j-api-*.jar
      call :AddJar %HIVE_HOME%\lib hive-hbase-handler-*.jar
      call :AddJar %HIVE_HOME%\lib httpclient-*.jar
  ) else (
      echo "HIVE_HOME should be defined"
      exit /b 1
  )
  set PIG_CLASSPATH=%PIG_CLASSPATH%;%HCAT_DEPENDCIES%;%HIVE_HOME%\conf
  set PIG_OPTS=%PIG_OPTS% -Dpig.additional.jars=%HCAT_DEPENDCIES%;%PIG_ADDITIONAL_JARS%
:HCAT_END

  if defined PIG_CLASSPATH (
    set CLASSPATH=!CLASSPATH!;%PIG_CLASSPATH%
  )

  if defined PIG_CONF_DIR (
    set CLASSPATH=!CLASSPATH!;%PIG_CONF_DIR%
  )

  if defined HADOOP_CONF_DIR (
    set CLASSPATH=!CLASSPATH!;%HADOOP_CONF_DIR%
  )

  if not defined PIG_LOGDIR (
    set PIG_LOGDIR=%HADOOP_LOG_DIR%
  )
  
  if not defined PIG_LOGFILE (
    set PIG_LOGFILE=%PIG_LOGDIR%
  )
  
  if defined PIG_HEAPSIZE (
    set JAVA_HEAP_MAX= -Xmx%PIG_HEAPSIZE%M
  )

  set CLASSPATH=!CLASSPATH!;%JAVA_HOME%\lib\tools.jar

  if defined HBASE_CONF_DIR (
    set CLASSPATH=!CLASSPATH!;%HBASE_CONF_DIR%
  )

  set PIG_OPTS=%PIG_OPTS% -Dpig.log.dir=%PIG_LOGDIR%
  set PIG_OPTS=%PIG_OPTS% -Dpig.logfile=%PIG_LOGFILE%
  set PIG_OPTS=%PIG_OPTS% -Dpig.home.dir=%PIG_HOME%
  set PIG_OPTS=%PIG_OPTS% -Dpig.root.logger=%HADOOP_ROOT_LOGGER%
  set PIG_OPTS=%PIG_OPTS% -Dfile.encoding=UTF-8 
  set PIG_OPTS=%PIG_OPTS% %HADOOP_OPTS%

  call %JAVA% %JAVA_HEAP_MAX% %PIG_OPTS% -classpath %CLASSPATH% org.apache.pig.Main %PIGARGS%
  exit /b %ERRORLEVEL%
  goto endlocal

  :AddJar
    pushd %1
    for /f %%a IN ('dir /b %2') do (
   	  set HCAT_DEPENDCIES=!HCAT_DEPENDCIES!;%1\%%a
    )
    popd
:endlocal
