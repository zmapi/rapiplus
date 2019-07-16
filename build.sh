# /usr/bin/env bash

set -e

if [[ -z ${COMPILE_JOBS} ]]; then
	COMPILE_JOBS=$(grep ^cpu\\scores /proc/cpuinfo | uniq |  awk '{print $4}')
fi
echo "compile jobs: ${COMPILE_JOBS}"

docker build . \
	--build-arg COMPILE_JOBS=${COMPILE_JOBS} \
	-t rapiplus

if [[ -z ${CACHE_FN} ]]; then
	CACHE_FN="$PWD/cache/general.cache"
	mkdir -p $(dirname "${CACHE_FN}")
fi

if [[ -z ${RUN_DIR} ]]; then
	RUN_DIR="$PWD/run"
	mkdir -p "${RUN_DIR}"
fi

if [[ -z ${LOG_LEVEL} ]]; then
	export LOG_LEVEL="DEBUG"
fi

if [[ -z ${RAPI_ENV} ]]; then
	export RAPI_ENV="example_env"
fi

docker run --rm -it \
	-h rapiplus \
	-v "${RUN_DIR}":/run/rapiplus \
	-v "$(dirname ${CACHE_FN})":/rapiplus/bin/cache \
	-e LOG_LEVEL \
	-e RAPI_ENV \
	rapiplus
