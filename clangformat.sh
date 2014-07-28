#!/ bin / bash
find . -type f | grep "\.cpp$\|\.h$" | clang-format -i
