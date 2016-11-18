/// override-include-guard

// If you make any changes to this - use g++ -E to check the generated code

/// Which options object to use (we can reuse those macros for fbonly options)
#ifndef OPTIONS
#define OPTIONS wdt::WdtOptions
#endif

// Short symbol A is a field inside the Options struct
// The flag name is either the short one DEFINE_type(A,...) or
// prefixed by wdt_ so we play nice with others when making a
// library (long flag)
#define WDT_READ_OPT(A) facebook::OPTIONS::get().A
#define WDT_OPT_VARIABLE(A) options.A

// Generic macros to concat and stringify:
// Turns wdt_ and foo into wdt_foo
#define WDT_CONCAT1(a, b) a##b
#define WDT_CONCAT(a, b) WDT_CONCAT1(a, b)
// Turns a symbol into a string literal ie foo to "foo"
// Needs two steps so WDT_TOSTR(WDT_CONCAT(wdt_,foo)) gives "wdt_foo"
// and not "WDT_CONCAT(wdt_,foo)"
#define WDT_TOSTR1(x) #x
#define WDT_TOSTR(x) WDT_TOSTR1(x)

#ifndef WDT_LONG_PREFIX
#define WDT_LONG_PREFIX wdt_
#endif

#ifndef STANDALONE_APP
#define WDT_PREFIX(argument) WDT_CONCAT(WDT_LONG_PREFIX, argument)
#else
#define WDT_PREFIX(argument) argument
#endif

// Symbol. eg wdt_foo
#define WDT_FLAG_SYM(A) WDT_PREFIX(A)
// String version eg "wdt_foo"
#define WDT_FLAG_STR(A) WDT_TOSTR(WDT_FLAG_SYM(A))
// Flag variable eg  FLAGS_wdt_foo
#define WDT_FLAG_VAR(A) WDT_CONCAT(FLAGS_, WDT_FLAG_SYM(A))

#ifdef WDT_OPT
#undef WDT_OPT
#endif

/// Setup variants to replace WDT_OPT by the right code depending
/// on the mode/context. Trailing semi colon is expected to be in the .inc
#ifdef ASSIGN_OPT
// Assign option from flags
#define WDT_OPT(A, type, description) WDT_OPT_VARIABLE(A) = WDT_FLAG_VAR(A)
#else
#ifdef PRINT_OPT
// print options
#define WDT_OPT(A, type, description)                                 \
  out << WDT_LOG_PREFIX << WDT_TOSTR(A) << " " << WDT_OPT_VARIABLE(A) \
      << std::endl
#else
// google flag define or declare:
#define WDT_FLAG_DECLARATION(type, argument) DECLARE_##type(argument);
#define WDT_FLAG_DEFINITION(type, argument, value, description) \
  DEFINE_##type(argument, value, description);

#ifdef DECLARE_ONLY
// declare flags
#define WDT_OPT(A, type, description) \
  WDT_FLAG_DECLARATION(type, WDT_FLAG_SYM(A))
#else
// define flags
#define WDT_OPT(A, type, description) \
  WDT_FLAG_DEFINITION(type, WDT_FLAG_SYM(A), WDT_READ_OPT(A), description)
#endif
#endif
#endif
