# Dependably Fast 
[![Build Status](https://travis-ci.org/NYU-NEWS/janus.svg?branch=master)](https://travis-ci.org/NYU-NEWS/janus)

A programming framework and library for building fail-slow tolerant distributed systems

## Quick start (with Ubuntu 16.04 or newer)

Get source code:

```
git clone --recursive https://github.com/WolfDOS/depfast.git
```

Install dependencies:

```
sudo apt-get update
sudo apt-get install -y \
    git \
    pkg-config \
    build-essential \
    clang \
    libapr1-dev libaprutil1-dev \
    libboost-all-dev \
    libyaml-cpp-dev \
    libjemalloc-dev \
    python3-dev \
    python3-pip \
    python3-wheel \
    python3-setuptools \
    libgoogle-perftools-dev
cd depfast
sudo pip3 install -r requirements.txt
```

Build:

```
python3 waf configure build 

```

Test run:
```
python3 test_run.py -m janus
```

## More
Check out the doc directory to find more about how to build the system on older or newer distros, how to run the system in a distributed setup, and how to generate figures in the paper, etc.

