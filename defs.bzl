WDT_ENV = {
    "WDT_BINARY": "$(location :wdt)",
    "WDT_GEN_FILES": "$(location //wdt/bench:wdt_gen_files)",
    "WDT_GEN_BIGRAMS": "$(location //wdt/bench:book1.bigrams)",
}

def wdt_test(test_name, ext, tags):
    custom_unittest(
        name = test_name,
        command = [
            "wdt/test/" + test_name + ext
        ],
        type = 'simple',
        env = WDT_ENV,
        deps = [
            ':wdt',
            '//wdt/bench:wdt_gen_files'
        ],
        tags = tags
    )

def wdt_sh_test(test_name, tags = []):
    wdt_test(test_name, ".sh", tags)

def wdt_py_test(test_name, tags = []):
    wdt_test(test_name, ".py", tags)
