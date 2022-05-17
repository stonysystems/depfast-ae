FROM ubuntu:20.04

ENV TZ=Europe/Kiev
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

RUN apt-get update

RUN echo 'root:root' |chpasswd

RUN apt-get clean && \
    rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

# install basic libraries
RUN apt-get update
RUN apt update
RUN apt-get install -y \
    git \
    wget \
    python2 \
    pkg-config \
    build-essential \
    clang \
    cgroup-tools \
    libapr1-dev libaprutil1-dev \
    libboost-all-dev \
    libyaml-cpp-dev \
    libjemalloc-dev \
    python3-dev \
    python3-pip \
    python3-wheel \
    python3-setuptools \
    libgoogle-perftools-dev
RUN wget https://github.com/mikefarah/yq/releases/download/v4.24.2/yq_linux_amd64 -O /usr/bin/yq && chmod +x /usr/bin/yq

# copy .gitconfig (For the permission)
# COPY gitconfig /root/.gitconfig

WORKDIR "/root"
RUN git clone --recursive https://github.com/stonysystems/depfast-ae.git depfast

WORKDIR "/root/depfast"
# install dependencies
RUN git checkout atc_ae
RUN pip3 install -r requirements.txt
RUN python3 waf configure build
