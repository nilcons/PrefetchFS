#!/opt/ceh/lib/perl

use strict;
use warnings;
use lib "/opt/ceh/lib";
use CehInstall;

if ($ENV{CEH_GHC64}) {
    ceh_nixpkgs_install_for_ghc64('fuse', nixpkgs_version => '1f2ecd08cc28d0d199d7f0304da9d8bbc2ff6239', derivation => 'wzz6nqa4sbq3awqgq33gcq2a27mmzsnz-fuse-2.9.2.drv', out => 'n7fv3gkpd5r7gbh6q4qapkz0k1vjc1lv-fuse-2.9.2');
} else {
    ceh_nixpkgs_install_for_ghc('fuse', nixpkgs_version => '1f2ecd08cc28d0d199d7f0304da9d8bbc2ff6239', derivation => 'chzsflhylzgr6hj9h9sa2sn75kmaazqq-fuse-2.9.2.drv', out => 'r3qbai1far76d7mqa2ajz490mk37bfdq-fuse-2.9.2');
}
