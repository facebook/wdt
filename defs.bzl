load("@fbcode_macros//build_defs:custom_unittest.bzl", "custom_unittest")

WDT_ENV = {
    "WDT_BINARY": "$(location :wdt)",
    "WDT_GEN_BIGRAMS": "$(location //wdt/bench:book1.bigrams)",
    "WDT_GEN_FILES": "$(location //wdt/bench:wdt_gen_files)",
}

def wdt_test(test_name, ext, tags):
    custom_unittest(
        name = test_name,
        command = [
            "wdt/test/" + test_name + ext,
        ],
        env = WDT_ENV,
        tags = tags,
        type = "simple",
        deps = [
            ":wdt",
            "//wdt/bench:wdt_gen_files",
        ],
    )

def wdt_sh_test(test_name, tags = []):
    wdt_test(test_name, ".sh", tags)

def wdt_py_test(test_name, tags = []):
    wdt_test(test_name, ".py", tags)
