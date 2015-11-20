#! /bin/sh
# Very inneficient way to colorize log input
awk '/^I/ {system("tput setaf 2"); print $0} /^W/ {system("tput setaf 3"); print $0} /^E/ {system("tput setaf 1"); print $0} /^C/ {system("tput setaf 5"); print $0} /^[^IWEC]/ {system("tput sgr0"); print $0}'
