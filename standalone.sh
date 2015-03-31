#!/bin/bash

set -e

# Use this script to generate a deployment directory in
# PrefetchFS-YYYYMMDD/ that contains a built PrefetchFS binary with
# all the necessary clibs included in standalone/lib.  You can then
# move the standalone subdirectory to any system to use PrefetchFS,
# without having to worry about libraries.

# This script is designed to be started from shells which have Ceh
# (http://github.com/errge/ceh) enabled, because this script uses the
# patchelf binary provided by Ceh, and this script depends on
# /nix/store included into the library pathnames in the PrefetchFS
# executable, and most non-Ceh GHCs don't add such pathnames to the
# executables they create.

DATE=PrefetchFS-$(date +%Y%m%d)-$(ghc --print-target-platform)

cabal configure --user
cabal build
rm -rf $DATE
mkdir $DATE
cp dist/build/PrefetchFS/PrefetchFS $DATE/PrefetchFS.bin
LDLINUX=$(ldd dist/build/PrefetchFS/PrefetchFS | grep 'ld-linux.*\.so\.2' | awk '{print $1}')
LDLINUX_NAME=$(basename $LDLINUX)
cd $DATE

patchelf --set-interpreter /shimmed/do/not/use/directly PrefetchFS.bin
mkdir lib
cp -aLv $(ldd ./PrefetchFS.bin | grep -o '/nix/store/[^ ]*') lib/
cp -aLv $LDLINUX lib/
mkdir lib/gconv
cp -aLv $(dirname $LDLINUX)/gconv/gconv-modules lib/gconv
cp -aLv $(dirname $LDLINUX)/gconv/UTF-* lib/gconv
cp -aLv $(dirname $LDLINUX)/gconv/ISO8859-* lib/gconv
chmod u+w lib/*
chmod u+w lib/gconv/*

patchelf --set-rpath '$ORIGIN/lib' PrefetchFS.bin

cat <<'EOF' | sed -e "s/LDLINUX_NAME/$LDLINUX_NAME/" >PrefetchFS
#!/bin/sh

STANDALONE_DIR="$(dirname $(readlink -f "$0"))"
export STANDALONE_PREFETCHFS_EXECUTABLE="$0"
export GCONV_PATH=$PWD/lib/gconv
exec "${STANDALONE_DIR}/lib/LDLINUX_NAME" "${STANDALONE_DIR}/PrefetchFS.bin" "$@"
EOF
chmod a+x PrefetchFS
chmod a-x PrefetchFS.bin

cd ..

tar cvfz $DATE.tgz $DATE
