#! /bin/sh
cat <<EOF
[{
    "alias": "wdt-opensource",
    "command": "SandcastleUniversalCommand",
    "vcsType": "fbcode-fbsource",
    "user": "ldemailly",
    "capabilities": {
        "vcs": "fbcode-fbsource",
        "type": "lego-linux",
        "os": "ubuntu_16.04"
    },
    "args": {
        "name": "wdt-opensource",
        "oncall": "wdt",
        "timeout": 10800,
        "steps": [
            {
                "user": "facebook",
                "name": "Build Wdt",
                "shell": "mkdir wdt_build; cd wdt_build; pwd; cmake ../wdt -DFOLLY_SOURCE_DIR=$BOX_DIR -DBUILD_TESTING=on && make -j",
                "required": true
            },
            {
                "user": "facebook",
                "name": "Run Wdt Tests",
                "shell": "cd wdt_build; CTEST_OUTPUT_ON_FAILURE=1 make test",
                "required": true
            }
        ]
    }
}]
EOF
