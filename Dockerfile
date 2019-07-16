FROM ubuntu:18.04

COPY apt/sources.list /etc/apt

RUN set -ex \
	&& apt-get update && apt-get -y install \
		sudo \
		wget \
		build-essential \
		libssl-dev \
		zlib1g-dev \
		libncurses5-dev \
		libncursesw5-dev \
		libreadline-dev \
		libsqlite3-dev \
		libgdbm-dev \
		libdb5.3-dev \
		libbz2-dev \
		libexpat1-dev \
		liblzma-dev \
		libffi-dev \
		uuid-dev \
		git \
		libzmq5 \
		libzmq3-dev \
		binutils-dev \
		libdw1 \
		libdw-dev

RUN set -ex \
	&& cd /tmp \
	&& wget https://www.python.org/ftp/python/3.7.3/Python-3.7.3.tgz

ARG COMPILE_JOBS=1

RUN set -ex \
	&& cd /tmp \
	&& tar -xvf Python-3.7.3.tgz \
	&& cd Python-3.7.3 \
	&& ./configure --enable-shared \
	&& make -j ${COMPILE_JOBS} \
	&& make install \
	&& ln -s $(which pip3) /usr/local/bin/pip \
	&& ln -s $(which python3) /usr/local/bin/python

ENV LD_LIBRARY_PATH /usr/local/lib
ENV CPATH /usr/local/include/python3.7m

RUN set -ex \
	&& cd /tmp \
	&& wget https://sourceforge.net/projects/boost/files/boost/1.69.0/boost_1_69_0.tar.gz/download -O boost.tar.gz \
	&& tar xvf boost.tar.gz \
	&& cd boost_* \
	&& ./bootstrap.sh --with-python=/usr/local/bin/python3 2>&1 | tee /share \
	&& ./b2 -j ${COMPILE_JOBS} \
	&& ./b2 install

RUN set -ex \
	&& pip3 install --upgrade pip \
	&& pip3 install ipdb \
	&& pip3 install pyzmq==17.1.3 \
	&& pip3 install numpy==1.16.2 \
	&& pip3 install pandas==0.24.2 \
	&& pip3 install bidict==0.17.5 \
	&& pip3 install sortedcontainers==2.1.0 \
	&& pip3 install aiohttp

COPY rapiplus /rapiplus

RUN set -ex \
	&& cd /rapiplus \
	&& ./gen_py_constants.py \
	&& ./build.sh

COPY entrypoint.sh /usr/local/bin
CMD ["/bin/bash", "/usr/local/bin/entrypoint.sh"]
