#!/bin/bash

set -e
shopt -s globstar

[[ -f .env.sh ]] && source .env.sh


args_fwd=""
build_pch=false
for opt in "$@"; do
	case $opt in
		--pch)
			build_pch=true
			;;
		*)
			args_fwd+=" $opt"
	esac
done

if [[ ! -z "$args_fwd" ]]; then
	echo "USAGE: build.sh [--pch]" >&2
	exit 1
fi

if [[ -f .srchash ]]; then
	new_hash="$(cat include/**/* src/**/* | md5sum)"
	old_hash="$(cat .srchash)"
	[[ $new_hash == $old_hash ]] && exit 0
fi

defines="-DLINUX -D_REENTRANT"

warn_opts="-Wall -Wno-sign-compare -Wno-write-strings -Wpointer-arith -Winline -Wno-deprecated"

opts=""

# find information about the compiler
		
if [[ -z ${CXX} ]]; then
	compiler="gcc"
	compile_cmd="g++"
elif [[ $(${CXX} --version | head -1) == clang* ]]; then
	compiler="clang"
	compile_cmd="${CXX}"
else
	echo "compiler not recognized, check CXX environment variable" >&2
	exit 1
fi

includes="-Iinclude"
[[ ${compiler} == "clang" ]] && includes="$includes -Igch"

stdver="c++17"

# why -fno-strict-aliasing is recommended (needed?) by RAPI?
opts="$opts -fno-strict-aliasing"

# https://en.wikipedia.org/wiki/Buffer_overflow_protection#GNU_Compiler_Collection_(GCC)
opts="$opts -fstack-protector-all"

opts="$opts -shared"

opts="$opts -fPIC"

opts="$opts -fno-inline"

libs="-Llib"

libs="$libs -lRApiPlus-optimize -lOmneStreamEngine-optimize -lOmneChannel-optimize -lOmneEngine-optimize -l_api-optimize -l_apipoll-stubs-optimize -l_kit-optimize -lssl -lcrypto -lz -lpthread -lrt -ldl"

libs="$libs -lpython3.7m -lboost_python37"

libs="$libs -lzmq"

################################ FOR DEBUGGING ################################

opts="$opts -O0 -g"

# for nicer stack traces
opts="$opts -fomit-frame-pointer"
opts="$opts -fno-optimize-sibling-calls"

######################## TRACEBACKS WITH BACKWARDS-CPP ########################

# https://github.com/bombela/backward-cpp

defines="$defines -DBACKWARD_HAS_BFD=1 -DBACKWARD_HAS_DW=1 -DBACKWARD_ENABLED"
libs="$libs -lbfd -ldl -ldw"

########################### DEBUG OPTIONS FOR CLANG ###########################

if [[ -v DEBUG ]]; then
	# https://clang.llvm.org/docs/AddressSanitizer.html
	# Causes problems with compiled rapi libraries ...
	# opts="$opts -fsanitize=address"

	# https://clang.llvm.org/docs/MemorySanitizer.html
	# opts="$opts -fsanitize=memory"

	# https://clang.llvm.org/docs/LeakSanitizer.html
	# opts="$opts -fsanitize=leaks"

	# Otherwise will get problems when trying to debug with third party libraries
	# without debug symbols ...
	opts="$opts -fstandalone-debug"
fi

###############################################################################

[[ -d build ]] || mkdir build

time $compile_cmd $defines $warn_opts $opts src/*.cpp $includes $libs -std=$stdver -o build/rapiplus.so

# Building minimalistic native library with boost.python takes about 3.5sec ...

cat include/**/* src/**/* | md5sum > .srchash
