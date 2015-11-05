#!/bin/sh
find . \( -name "*~" -o -name "*.orig" \) -print0 | xargs -0 rm -v
