#!/bin/bash
find . -type f -a \( -name "*.h" -o -name "*.cpp" \) | xargs clang-format -i
