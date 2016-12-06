#! /bin/sh

cat <<EOF
[{
    "alias": "wdt-opensource-linux",
    "command": "SandcastleUniversalCommand",
    "vcsType": "fbcode-fbsource",
    "user": "ldemailly",
    "capabilities": {
        "vcs": "fbcode-fbsource",
        "type": "lego-linux",
        "os": "ubuntu_16.04"
    },
    "args": {
        "name": "wdt-opensource-linux",
        "oncall": "wdt",
        "timeout": 10800,
        "steps": [
            {
                "user": "facebook",
                "name": "Build Wdt OpenSource Linux",
                "shell": "mkdir wdt_build; cd wdt_build; pwd; cmake ../wdt -DFOLLY_SOURCE_DIR=$BOX_DIR -DBUILD_TESTING=on && make -j",
                "required": true
            },
            {
                "user": "facebook",
                "name": "Run Wdt OpenSource Linux Tests",
                "shell": "cd wdt_build; CTEST_OUTPUT_ON_FAILURE=1 make test",
                "required": true
            }
        ]
    }
},
{
    "alias": "wdt-opensource-mac",
    "command": "SandcastleUniversalCommand",
    "vcsType": "fbcode-fbsource",
    "user": "ldemailly",
    "capabilities": {
        "vcs": "fbcode-fbsource",
        "type": "lego-mac",
    },
    "args": {
        "name": "wdt-opensource-mac",
        "oncall": "wdt",
        "timeout": 10800,
        "steps": [
        "steps": [
            {
                "user": "facebook",
                "name": "Build Wdt OpenSource Mac",
                "shell": "wdt/build/lego-mac-build.sh",
                "required": true
            },
            {
                "user": "facebook",
                "name": "Run Wdt OpenSource Mac Tests",
                "shell": "cd wdt_build; CTEST_OUTPUT_ON_FAILURE=1 make test",
                "required": true
            }
        ]
    }
}]
EOF
