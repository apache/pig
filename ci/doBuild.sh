set -e

echo Building build image
cd ci
./ciBuild.sh
cd ..

USE_UID=$UID

if [ ${USE_UID} -eq 500345588 ]; then
    USE_UID=1000
fi

echo Using UID ${USE_UID}

echo Building pig
docker run -u ${USE_UID} -v $(pwd):/app --entrypoint /bin/bash  local/pigbuilder /app/ci/build-and-test.sh

