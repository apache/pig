# Building Apache Pig

## Requirements:

* Unix System
* JDK 1.7+
* Ant 1.8.1+
* Findbugs 3.x+
* Forrest 0.9 (for building the documentation)
* Internet connection for first build (to fetch all dependencies)

**Note**: Further down this document you can read about the _ready to run build environment_.

## Building Pig

To compile with Hadoop 2.x 

    ant clean jar piggybank

Building and running the tests needed before submitting a patch.
For more details https://cwiki.apache.org/confluence/display/PIG/HowToContribute
    
    ANT_OPTS='-Djavac.args="-Xlint -Xmaxwarns 1000"'
    ant ${ANT_OPTS} clean piggybank jar compile-test test-commit
    cd contrib/piggybank/java && ant ${ANT_OPTS} test

Generate documentation

    ant docs

# Ready to run build environment
The easiest way to get an environment with all the appropriate tools is by means
of the provided Docker config.
This requires a recent version of docker ( 1.4.1 and higher are known to work ).

## How it works
By using the mounted volumes feature of Docker this image will wrap itself around the directory from which it is started.
So the files within the docker environment are actually the same as outsite.

A very valid way of working is by having your favourite IDE that has the project 
open and a commandline into the docker that has the exact right tools to do the full build.

## Using it on Linux:
Install Docker and run this command:

    $ ./start-build-env.sh

## Using it on Mac:
First make sure Homebrew has been installed ( http://brew.sh/ )
    
    $ brew install docker boot2docker
    $ boot2docker init -m 4096
    $ boot2docker start
    $ $(boot2docker shellinit)
    $ ./start-build-env.sh

The prompt which is then presented is located at a mounted version of the source tree
and all required tools for testing and building have been installed and configured.

Note that from within this docker environment you ONLY have access to the source
tree from where you started. 

## Known issues:
On Mac with Boot2Docker the performance on the mounted directory is currently extremely slow.
This is a known problem related to boot2docker on the Mac.
https://github.com/boot2docker/boot2docker/issues/593
    This issue has been resolved as a duplicate, and they point to a new feature for utilizing NFS mounts as the proposed solution:

https://github.com/boot2docker/boot2docker/issues/64
    An alternative solution to this problem is when you install Linux native inside a virtual machine and run your IDE and Docker etc in side that VM.

