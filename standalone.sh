#!/bin/bash -e

# Use this script to generate a deployment directory in standalone/
# that contains a built PrefetchFS binary with all the necessary clibs
# included in standalone/lib.  You can then move the standalone
# subdirectory to any system to use PrefetchFS, without having to
# worry about libraries.

# For this to work, you have to use the GHC provided by
# Ceh (http://github.com/errge/ceh).

cabal configure --user
cabal build
rm -rf standalone
mkdir standalone
cp dist/build/PrefetchFS/PrefetchFS standalone
cd standalone

patchelf --set-interpreter /lib/ld-linux.so.2 PrefetchFS
mkdir lib
cp -aLv $(ldd ./PrefetchFS | grep -o '/nix/store/[^ ]*') lib/
patchelf --set-rpath '$ORIGIN/lib' PrefetchFS
