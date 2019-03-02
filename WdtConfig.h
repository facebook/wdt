// fbcode version (with settings for the platform) of WdtConfig.h.in
// WARNING:
// If you change anything in this file you MUST change WdtConfig.h.in and
// CMakeLists.txt so it stays in sync and generates the correct values
// on (opensource) Linux, MacOS,...
// Never edit this file manually for protocol version or version, edit
// CMakeLists.txt and run build/version_update.tcl instead
#pragma once

#include <fcntl.h>

#define WDT_VERSION_MAJOR 1
#define WDT_VERSION_MINOR 31
#define WDT_VERSION_BUILD 1903011
// Add -fbcode to version str
#define WDT_VERSION_STR "1.31.1903011-fbcode"
// Tie minor and proto version
#define WDT_PROTOCOL_VERSION WDT_VERSION_MINOR

#define HAS_POSIX_FALLOCATE 1
#define HAS_SYNC_FILE_RANGE 1
#define HAS_POSIX_MEMALIGN 1
#define HAS_POSIX_FADVISE 1

#define WDT_SUPPORTS_ODIRECT 1
#define WDT_HAS_SOCKIOS_H 1
// Again do not add new defines here without editing WdtConfig.h.in ...
