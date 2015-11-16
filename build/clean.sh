#!/bin/sh
find . \( -name "*~" -o -name "*.orig" -o -name "*.rej" \) -print0 | \
    xargs -0 rm -v
