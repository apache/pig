#!/bin/sh -e
export RPM_VERSION=${VERSION}.${BUILD_NUMBER}
export RPM_NAME=`echo pig_${VERSION} | sed s/[.]/_/g`
echo "Building Pig Version ${VERSION} to RPM version ${RPM_VERSION} with RPM name ${RPM_NAME}"

rm -rf ${WORKSPACE}/install-*
ant -Dforrest.home=/opt/apache-forrest-0.9 -Dant.home=/opt/verticloud/ant-1.8.4/ -Dversion=${VERSION} -Dhadoopversion=23 clean jar jar-withouthadoop
pushd contrib/zebra
ant -Dhadoopversion=23
popd
pushd contrib/piggybank/java
ant -Dhadoopversion=23
popd
ant -Dforrest.home=/opt/apache-forrest-0.9 -Dant.home=/opt/verticloud/ant-1.8.4/ -Dversion=${VERSION} -Dhadoopversion=23 tar
export RPM_BUILD_DIR=${WORKSPACE}/install-${BUILD_NUMBER}/opt
mkdir --mode=0755 -p ${RPM_BUILD_DIR}
cd ${RPM_BUILD_DIR}
tar -xvzpf ${WORKSPACE}/pig-${VERSION}.tar.gz

cd ${WORKSPACE}
fpm --verbose \
--maintainer ops@verticloud.com \
--vendor VertiCloud \
--provides ${RPM_NAME} \
-s dir \
-t rpm \
-n ${RPM_NAME} \
-v ${VERSION} \
--iteration ${BUILD_NUMBER} \
--rpm-user root \
--rpm-group root \
-C ${WORKSPACE}/install-${BUILD_NUMBER} \
opt
