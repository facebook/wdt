#!/bin/sh
find . \( -name "*~" -o -name "*.orig" -o -name "*.rej" -o -name "#*#" \
    -o -name ".#*" \) -print0 | xargs -0 rm -v
