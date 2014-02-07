# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# 'License'); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an 'AS IS' BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# 
# The Pig command script
#
# Environment Variables
#
#     JAVA_HOME                The java implementation to use.    Overrides JAVA_HOME.
#
#     PIG_CLASSPATH Extra Java CLASSPATH entries.
#
#     HADOOP_HOME/HADOOP_PREFIX     Environment HADOOP_HOME/HADOOP_PREFIX(0.20.205)
#
#     HADOOP_CONF_DIR     Hadoop conf dir
#
#     PIG_HEAPSIZE    The maximum amount of heap to use, in MB. 
#                                        Default is 1000.
#
#     PIG_OPTS            Extra Java runtime options.
#
#     PIG_CONF_DIR    Alternate conf dir. Default is ${PIG_HOME}/conf.
#
#     HBASE_CONF_DIR - Optionally, the HBase configuration to run against
#                      when using HBaseStorage


import sys
import os
import glob
import subprocess

debug = False
restArgs = []
includeHCatalog = False
additionalJars = ""

for arg in sys.argv:
  if arg == __file__:
    continue
  if arg == "-secretDebugCmd":
    debug = True
  elif arg == "-useHCatalog":
    includeHCatalog = True
  elif arg.split("=")[0] == "-D.pig.additional.jars":
    if includeHCatalog == True:
      additionalJars = arg.split("=")[1]
    else:
      restArgs.append(arg)
  else:
    restArgs.append(arg)

# Determine our absolute path, resolving any symbolic links
this = os.path.realpath(sys.argv[0])
bindir = os.path.dirname(this) + os.path.sep

# the root of the pig installation
os.environ['PIG_HOME'] = os.path.join(bindir, os.path.pardir)

if 'PIG_CONF_DIR' not in os.environ:
  pigPropertiesPath = os.path.join(os.environ['PIG_HOME'], 'conf', 'pig.properties')
  if os.path.exists(pigPropertiesPath):
    try:
      fhdl = open(pigPropertiesPath, 'r')
      fhdl.close()
      os.environ['PIG_CONF_DIR'] = os.path.join(os.environ['PIG_HOME'], 'conf')
    except:
      # in the small window after checking for file, if file is deleted, 
      # we should fail if we hit an exception
      sys.exit('Failed to access file %s' % pigPropertiesPath)

  elif os.path.exists(os.path.join(os.path.sep, 'etc', 'pig')):
    os.environ['PIG_CONF_DIR'] = os.path.join(os.path.sep, 'etc', 'pig')

  else:
    sys.exit('Cannot determine PIG_CONF_DIR. Please set it to the directory containing pig.properties')

# Hack to get to read a shell script and the changes to the environment it makes
# This is potentially bad because we could execute arbitrary code
try:
  importScript = os.path.join(os.environ['PIG_CONF_DIR'], 'runPigEnv.sh')
  fd = open(importScript, 'w')
  fd.write(". " + os.path.join(os.environ['PIG_CONF_DIR'], 'pig-env.sh'))
  fd.write("\n")
  fd.write("set")
  fd.close()
  outFd = open(os.path.join(os.environ['PIG_CONF_DIR'], 'pigStartPython.out'), 'w')
  output = subprocess.Popen(importScript, shell=True, stdout=outFd)
  output.wait()
  outFd.close()
  outFd = open(os.path.join(os.environ['PIG_CONF_DIR'], 'pigStartPython.out'), 'r')
  for line in outFd:
    if line.split(' ') > 1:
      continue

    envSplit = line.split('=')
    if len(envSplit) == 2:
      os.environ[envSplit[0]] = os.environ[1]
  outFd.close()
except:
  pass

# functionality similar to the shell script. This executes a pig script instead
try:
  if os.path.exists(os.environ['PIG_CONF_DIR'], 'pig.conf'):
    pigConf = os.path.join(os.environ['PIG_CONF_DIR'], 'pig.conf')
    __import__(pigConf)
except:
  pass

if 'JAVA_HOME' not in os.environ:
  sys.exit('Error: JAVA_HOME is not set')

if 'HADOOP_HOME' not in os.environ:
  os.environ['HADOOP_HOME'] = os.path.sep + 'usr'

java = os.path.join(os.environ['JAVA_HOME'], 'bin', 'java')
javaHeapMax = "-Xmx1000m"

if 'PIG_HEAPSIZE' in os.environ:
  javaHeapMax = '-Xmx' + os.environ['PIG_HEAPSIZE'] + 'm'

classpath = os.environ['PIG_CONF_DIR']
classpath += os.pathsep + os.path.join(os.environ['JAVA_HOME'], 'lib', 'tools.jar')

if 'PIG_CLASSPATH' in os.environ:
  classpath += os.pathsep + os.environ['PIG_CLASSPATH']

if 'HADOOP_CONF_DIR' in os.environ:
  classpath += os.pathsep + os.environ['HADOOP_CONF_DIR']

pigLibJars = glob.glob(os.path.join(os.environ['PIG_HOME'], "lib", "*.jar"))
for jar in pigLibJars:
  classpath += os.pathsep + jar


######### if hcatalog is to be included, add hcatalog and its required jars

if includeHCatalog == True:
  # adding the hive jars required by hcatalog
  hiveJarLoc = ""
  if 'HIVE_HOME' in os.environ:
    hiveJarLoc = os.path.join(os.environ['HIVE_HOME'], "lib")
  else:
    if os.path.exists(os.path.join('usr', 'lib', 'hive')):
      hiveJarLoc = os.path.join('usr', 'lib', 'hive', 'lib')
    else:
      sys.exit("Please initialize HIVE_HOME to the hive install directory")

  allHiveJars = ["hive-metastore-*.jar", "libthrift-*.jar", "hive-exec-*.jar", "libfb303-*.jar", "jdo*-api-*.jar", "slf4j-api-*.jar", "hive-hbase-handler-*.jar"]
  for jarName in allHiveJars:
    jar = glob.glob(os.path.join(hiveJarLoc, jarName))
    if (len(jar) != 0) and (os.path.exists(jar)):
      classpath += os.pathsep + jar[0]
    else:
      sys.exit("Failed to find the jar %s" % os.path.join(hiveJarLoc, jarName))

  # done with adding the hive jars required by hcatalog

  # adding the hcat jars
  hcatHome = ""
  if 'HCAT_HOME' in os.environ:
    hcatHome = os.environ['HCAT_HOME']
  else:
    if os.path.exists(os.path.join(os.path.sep + "usr", "lib", "hcatalog")):
      hcatHome = os.path.join(os.path.sep + "usr", "lib", "hcatalog")
    else:
      sys.exit("Please initialize HCAT_HOME to the hcatalog install directory")

  hcatJars = glob.glob(os.path.join(hcatHome, "share", "hcatalog", "*hcatalog-*.jar"))
  found = False
  for hcatJar in hcatJars:
    if hcatJar.find("server") != -1:
      found = True
      classpath += os.pathsep + hcatJar
      break

  if found == False:
    sys.exit("Failed to find the hcatalog server jar in %s" % (os.path.join(hcatHome, "share", "hcatalog")))

  hcatHBaseJar = glob.glob(os.path.join(hcatHome, "lib", "hbase-storage-handler-*.jar"))
  try:
    classpath += os.pathsep + hcatHBaseJar[0]
  except:
    pass
  # done with adding the hcat jars

  # now also add the additional jars passed through the command line
  classpath += os.pathsep + additionalJars
  # done adding the additional jars from the command line

######### done with adding hcatalog and related jars


######### Add the jython jars to classpath

jythonJars = glob.glob(os.path.join(os.environ['PIG_HOME'], "lib", "jython*.jar"))
if len(jythonJars) == 1:
  classpath += os.pathsep + jythonJars[0]
else:
  jythonJars = glob.glob(os.path.join(os.environ['PIG_HOME'], "build", "ivy", "lib", "Pig", "jython*.jar"))
  if len(jythonJars) == 1:
    classpath += os.pathsep + jythonJars[0]

######### Done adding the jython jars to classpath


######### Add the jruby jars to classpath

jrubyJars = glob.glob(os.path.join(os.environ['PIG_HOME'], "lib", "jruby-complete-*.jar"))
if len(jrubyJars) == 1:
  classpath += os.pathsep + jrubyJars[0]
else:
  jrubyJars = glob.glob(os.path.join(os.environ['PIG_HOME'], "build", "ivy", "lib", "Pig", "jruby-complete-*.jar"))
  if len(jrubyJars) == 1:
    classpath += os.pathsep + jrubyJars[0]

pigJars = glob.glob(os.path.join(os.environ['PIG_HOME'], "share", "pig", "lib", "*.jar"))
for jar in pigJars:
  classpath += os.pathsep + jar

######### Done adding jruby jars to classpath


######### Add hadoop and hbase conf directories

hadoopConfDir = os.path.join(os.environ['PIG_HOME'], "etc", "hadoop")
if os.path.exists(hadoopConfDir):
  classpath += os.pathsep + hadoopConfDir

if 'HBASE_CONF_DIR' in os.environ:
  classpath += os.pathsep + os.environ['HBASE_CONF_DIR']
else:
  hbaseConfDir = os.path.join(os.path.sep + "etc", "hbase")
  if os.path.exists(hbaseConfDir):
    classpath += os.pathsep + hbaseConfDir

######### Done adding hadoop and hbase conf directories


######### Locate and add Zookeeper jars if they exist

zkDir = ""
if 'ZOOKEEPER_HOME' in os.environ:
  zkDir = os.environ['ZOOKEEPER_HOME']
else:
  zkDir = os.path.join(os.environ['PIG_HOME'], "share", "zookeeper")

if os.path.exists(zkDir):
  zkJars = glob.glob(os.path.join(zkdir, "zookeeper-*.jar"))
  for jar in zkJars:
    classpath += os.pathsep + jar

######### Done adding zookeeper jars


######### Locate and add hbase jars if they exist

hbaseDir = ""
if 'HBASE_HOME' in os.environ:
  hbaseDir = os.environ['HBASE_HOME']
else:
  hbaseDir = os.path.join(os.environ['PIG_HOME'], "share", "hbase")

if os.path.exists(hbaseDir):
  hbaseJars = glob.glob(os.path.join(hbaseDir, "hbase-*.jar"))
  for jar in hbaseJars:
    classpath += os.pathsep + jar

######### Done adding hbase jars


######### set the log directory and logfile if they don't exist

if 'PIG_LOG_DIR' not in os.environ:
  pigLogDir = os.path.join(os.environ['PIG_HOME'], "logs")

if 'PIG_LOGFILE' not in os.environ:
  pigLogFile = 'pid.log'

######### Done setting the logging directory and logfile

pigOpts = ""
try:
  pigOpts = os.environ['PIG_OPTS']
except:
  pass

pigOpts += " -Dpig.log.dir=" + pigLogDir
pigOpts += " -Dpig.log.file=" + pigLogFile
pigOpts += " -Dpig.home.dir=" + os.environ['PIG_HOME']

pigJar = ""
hadoopBin = ""

print "HADOOP_HOME: %s" % os.path.expandvars(os.environ['HADOOP_HOME'])
print "HADOOP_PREFIX: %s" % os.path.expandvars(os.environ['HADOOP_PREFIX'])

if (os.environ.get('HADOOP_PREFIX') is not None):
  print "Found a hadoop prefix"
  hadoopPrefixPath = os.path.expandvars(os.environ['HADOOP_PREFIX'])
  if os.path.exists(os.path.join(hadoopPrefixPath, "bin", "hadoop")):
    hadoopBin = os.path.join(hadoopPrefixPath, "bin", "hadoop")

if (os.environ.get('HADOOP_HOME') is not None):
  print "Found a hadoop home"
  hadoopHomePath = os.path.expandvars(os.environ['HADOOP_HOME'])
  print "Hadoop home path: %s" % hadoopHomePath
  if os.path.exists(os.path.join(hadoopHomePath, "bin", "hadoop")):
    hadoopBin = os.path.join(hadoopHomePath, "bin", "hadoop")

if hadoopBin == "":
  if os.path.exists(os.path.join(os.path.sep + "usr", "bin", "hadoop")):
    hadoopBin = os.path.join(os.path.sep + "usr", "bin", "hadoop")

if hadoopBin != "":
  if debug == True:
    print "Find hadoop at %s" % hadoopBin

  if os.path.exists(os.path.join(os.environ['PIG_HOME'], "pig-withouthadoop.jar")):
    pigJar = os.path.join(os.environ['PIG_HOME'], "pig-withouthadoop.jar")

  else:
    pigJars = glob.glob(os.path.join(os.environ['PIG_HOME'], "pig-?.*withouthadoop.jar"))
    if len(pigJars) == 1:
      pigJar = pigJars[0]

    elif len(pigJars) > 1:
      print "Ambiguity with pig jars found the following jars"
      print pigJars
      sys.exit("Please remove irrelavant jars fromt %s" % os.path.join(os.environ['PIG_HOME'], "pig-?.*withouthadoop.jar"))
    else:
      pigJars = glob.glob(os.path.join(os.environ['PIG_HOME'], "share", "pig", "pig-*withouthadoop.jar"))
      if len(pigJars) == 1:
        pigJar = pigJars[0]
      else:
        sys.exit("Cannot locate pig-withouthadoop.jar do 'ant jar-withouthadoop', and try again")

  if 'HADOOP_CLASSPATH' in os.environ:
    os.environ['HADOOP_CLASSPATH'] += os.pathsep + classpath
  else:
    os.environ['HADOOP_CLASSPATH'] = classpath

  if debug == True:
    print "dry run:"
    print "HADOOP_CLASSPATH: %s" % os.environ['HADOOP_CLASSPATH']
    try:
      print "HADOOP_OPTS: %s" % os.environ['HADOOP_OPTS']
    except:
      pass
    print "%s jar %s %s" % (hadoopBin, pigJar, ' '.join(restArgs))

  else:
    cmdLine = hadoopBin + ' jar ' + pigJar + ' ' + ' '.join(restArgs)
    subprocess.call(cmdLine, shell=True)

else:
  # fall back to use fat pig.jar
  if debug == True:
    print "Cannot find local hadoop installation, using bundled hadoop 20.2"
    
  if os.path.exists(os.path.join(os.environ['PIG_HOME'], "pig.jar")):
    pigJar = os.path.join(os.environ['PIG_HOME'], "pig.jar")

  else:
    pigJars = glob.glob(os.path.join(os.environ['PIG_HOME'], "pig-?.!(*withouthadoop).jar"))
    if len(pigJars) == 1:
      pigJar = pigJars[0]

    elif len(pigJars) > 1:
      print "Ambiguity with pig jars found the following jars"
      print pigJars
      sys.exit("Please remove irrelavant jars fromt %s" % os.path.join(os.environ['PIG_HOME'], "pig-?.*withouthadoop.jar"))
    else:
      sys.exit("Cannot locate pig.jar. do 'ant jar' and try again")

  classpath += os.pathsep + pigJar
  pigClass = "org.apache.pig.Main"
  if debug == True:
    print "dry runXXX:"
    print "%s %s %s -classpath %s %s %s" % (java, javaHeapMax, pigOpts, classpath, pigClass, ' '.join(restArgs)) 
  else:
    cmdLine = java + ' ' + javaHeapMax + ' ' + pigOpts
    cmdLine += ' ' + '-classpath ' + classpath + ' ' + pigClass +  ' ' + ' '.join(restArgs)
    subprocess.call(cmdLine, shell=True)
  
