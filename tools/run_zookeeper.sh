#!/bin/bash

set -e

PKGS=""
PKG_CMD=""
if [ -f "/etc/redhat-release" ]; then
    PKGS="$PKGS java-1.7.0-openjdk-devel"
    PKG_CMD="yum install -y"
elif [ -f "/etc/debian_version" ]; then
    PKGS="$PKGS openjdk-7-jdk"
    PKG_CMD="apt-get install -y"
else
    echo "Unknown operating system!"
    exit 1
fi

JAVA=`which java`
if [ -z "$JAVA" ]; then
    sudo $PKG_CMD $PKGS
fi

function on_exit() {
    local ec="$?"
    if [ -n "$ZOO_DIR" ]; then
        if [ -f "$ZOO_DIR/bin/zkServer.sh" ]; then
            $ZOO_DIR/bin/zkServer.sh stop
        fi
    fi
    return $ec
}

trap "on_exit" EXIT

VERSION="3.4.5"
if [ ! -d "$PWD/.zookeeper" ]; then
    mkdir -p "$PWD/.zookeeper"
fi

ZOO_BASE_DIR="$PWD/.zookeeper/"
if [ ! -f "$PWD/.zookeeper/zookeeper-$VERSION.tar.gz" ]; then
    echo "Downloading: zookeeper-$VERSION -> $PWD/.zookeeper/zookeeper-$VERSION.tar.gz"
    curl -o "$PWD/.zookeeper/zookeeper-$VERSION.tar.gz" \
            "http://www.us.apache.org/dist/zookeeper/zookeeper-$VERSION/zookeeper-$VERSION.tar.gz"
fi

cd "$ZOO_BASE_DIR"
if [ ! -d "zookeeper-$VERSION" ]; then
    echo "Extracting zookeeper-$VERSION.tar.gz -> $ZOO_BASE_DIR/zookeeper-$VERSION"
    mkdir -p $ZOO_BASE_DIR/zookeeper-$VERSION
    tar xzf ./zookeeper-$VERSION.tar.gz -C $ZOO_BASE_DIR/zookeeper-$VERSION --strip-components 1
fi

echo "Adjusting configuration..."
ZOO_DIR="$ZOO_BASE_DIR/zookeeper-$VERSION"
cp "$ZOO_DIR/conf/zoo_sample.cfg" "$ZOO_DIR/conf/zoo.cfg"
ZOO_DATADIR="$ZOO_BASE_DIR/.data/"
mkdir -p "$ZOO_DATADIR"
sed -i -r "s@(dataDir *= *).*@\1$ZOO_DATADIR@" "$ZOO_DIR/conf/zoo.cfg"

export ZOO_DATADIR="$ZOO_DATADIR"
echo "Starting..."
$ZOO_DIR/bin/zkServer.sh start-foreground
