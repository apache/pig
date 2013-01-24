#!/bin/sh -e
ant -Dforrest.home=/opt/apache-forrest-0.9 -Dant.home=/opt/verticloud/ant-1.8.4/ -Dversion=${VERSION} -Dhadoopversion=23 clean jar jar-withouthadoop
pushd contrib/zebra
ant -Dhadoopversion=23
popd
pushd contrib/piggybank/java
ant -Dhadoopversion=23
popd
ant -Dforrest.home=/opt/apache-forrest-0.9 -Dant.home=/opt/verticloud/ant-1.8.4/ -Dversion=${VERSION} -Dhadoopversion=23 tar
