#!/bin/sh
set -e

[ -z $1 ] && exit 1
find dist -type f -name $1 -exec upx -q {} \;
