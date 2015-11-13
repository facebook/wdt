#! /bin/sh
# Quick hack to figure out where is the space coming from

FILE=$1
if [ -z "$FILE" -o "$FILE" = "-h" ] ; then
  echo "Usage: $0 binary"
  exit 1
fi

TMPRESULT=`mktemp /tmp/nm_out.XXX`

RESULT=$TMPRESULT

echo "Working on $FILE, intermediate nm result in $RESULT"

nm -C --defined-only --print-size -l $FILE > $RESULT

echo "nm done... analyzing..."
#RESULT=/dev/shm/nm.out

gawk '
{sizeHex="0x" $2; size=strtonum(sizeHex); sum+=size}
/:[0-9]+$/ {
  if (size) {
    ssum += size;
    # not all lines of nm output do find a file with line number this finds
    # lines like    "xxxx/blah.cc:123" and returns "xxxx/blah"
    # for the xxx part it tries to keep it shorter by not allowing .
    # (nm sometimes outputs  path/./tmp/morepath)
    match($0, "([^ \t.]+)(\\.(h|c|cc|tcc|cpp|hpp|S|rl|ipp|y))?:[0-9]+$", arr);
    src=arr[1]
    # unknown extensions...
    if (length(src) < 5) print src, "  XXXX ", $0;
    # get rid of irrelevant stuff
    sub("/tmp/", "/", src)
    # somehow lots of ....fbcode...////////morestuf
    sub(".*///", "", src)
    # x/ or y/ ...
    sub("^./", "", src)
    # 20/....
    sub("^[0-9/]+", "", src)
    sizes[src] += size;
    match(src, "(.*)/[^/]*$", pkg);
    sizes[pkg[1] "/*"]+= size;
  }
}
END {
  print "Extracted file attribution for", ssum/1024/1024., "Mb out of",
     sum/1024/1024., "Mb :", int((1000*ssum/sum+.5))/10, "%"
  n = asorti(sizes, siter)
  for (s in sizes) {
    print s, sizes[s]
  }
}' $RESULT | sort -k 2 -n

rm $TMPRESULT
