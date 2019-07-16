#/bin/bash

set -e

cd /rapiplus/bin
./run.sh \
	${RAPI_ENV} \
	ipc:///run/rapiplus/ctl \
	ipc:///run/rapiplus/pub \
	--cache-fn /rapiplus/bin/cache/general.cache \
	--log-level ${LOG_LEVEL}
