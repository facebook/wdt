/// override-include-guard
#ifdef GENMACRO_X
#undef GENMACRO_X
#endif
#ifdef GENMACRO
#undef GENMACRO
#endif

#ifndef OPTIONS
#define OPTIONS WdtOptions
#endif

#ifndef STANDALONE_APP
#define PREFIX(argument) wdt_##argument
#else
#define PREFIX(argument) argument
#endif
#define VALUE(A) facebook::wdt::OPTIONS::get().A

#ifdef DECLARE_ONLY
#define GENMACRO_X(type, argument, value, description) DECLARE_##type(argument);
#else
#define GENMACRO_X(type, argument, value, description) \
  DEFINE_##type(argument, VALUE(value), description);
#endif

#ifdef STANDALONE_APP
#define GENMACRO(type, argument, description) \
  GENMACRO_X(type, argument, argument, description)
#else
#define GENMACRO(type, argument, description) \
  GENMACRO_X(type, wdt_##argument, argument, description)
#endif

#ifndef WDT_OPT
#define WDT_OPT(argument, type, description) \
  GENMACRO(type, argument, description)
#endif

#define VALUE_X(argument) FLAGS_##argument
#define CAT(argument) VALUE_X(argument)
#define FLAG_VALUE(argument) CAT(PREFIX(argument))

#ifdef ASSIGN_OPT
#ifdef WDT_OPT
#undef WDT_OPT
#endif
#define WDT_OPT(argument, type, description) \
  facebook::wdt::OPTIONS::getMutable().argument = FLAG_VALUE(argument);
#endif

#ifdef PRINT_OPT
#ifdef WDT_OPT
#undef WDT_OPT
#endif
#define WDT_OPT(argument, type, description) \
  LOG(INFO) << #argument << " " << facebook::wdt::OPTIONS::get().argument;
#endif
