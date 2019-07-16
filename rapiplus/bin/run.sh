#!/bin/bash

set -e

if [[ $# -le 1 ]]; then
	echo "USAGE: run.sh [env_name] [connector_args...]" >&2
	exit 1
fi

script_dir=$(dirname $0)
rapi_env=$1

cd $script_dir/..

[[ -f .env.sh ]] && source .env.sh

found=false
for fn in $(ls env); do
	if [[ $fn == $rapi_env ]]; then
		source env/$fn
		echo "environment: $fn"
		found=true
		break
	fi
done

if [[ $found == false ]]; then
	echo "environment not found: $fn" 1>&2
	exit 1
fi

export USER=$RAPI_USERNAME

cd bin

shift

echo ""
echo ""
echo ""

export LD_LIBRARY_PATH="../build:../lib:$LD_LIBRARY_PATH"
export PYTHONPATH="../build:../lib:../pylib:$PYTHONPATH"
# export PYTHONPATH="$script_dir/build:$script_dir/lib:$PYTHONPATH"

if [[ -v DEBUG ]]; then
	# exec lldb -o run -- python connector.py $@
	exec lldb -- python connector.py $@
else
	exec python connector.py $@
fi

# if [[ -v DEBUG ]]; then
# 	exec lldb -s lldbinit -- ./connector $@
# else
# 	exec ./connector $@
# fi
