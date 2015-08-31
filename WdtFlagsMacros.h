/// override-include-guard
#ifndef OPTIONS
#define OPTIONS WdtOptions
#endif

#ifndef STANDALONE_APP
#define PREFIX(argument) wdt_##argument
#else
#define PREFIX(argument) argument
#endif
#define VALUE(A) facebook::wdt::OPTIONS::get().A

#define VALUE_X(argument) FLAGS_##argument
#define CAT(argument) VALUE_X(argument)
#define FLAG_VALUE(argument) CAT(PREFIX(argument))

#ifdef WDT_OPT
#undef WDT_OPT
#endif

#ifdef ASSIGN_OPT
// Assign option from flags
#define WDT_OPT(argument, type, description) \
  facebook::wdt::OPTIONS::getMutable().argument = FLAG_VALUE(argument);
#else
#ifdef PRINT_OPT
// print options
#define WDT_OPT(argument, type, description)                        \
  out << #argument << " " << facebook::wdt::OPTIONS::get().argument \
      << std::endl;
#else

#define FLAG_DECLARATION(type, argument) DECLARE_##type(argument);
#define FLAG_DEFINITION(type, argument, value, description) \
  DEFINE_##type(argument, value, description);

#ifdef DECLARE_ONLY
// declare flags
#define WDT_OPT(argument, type, description) \
  FLAG_DECLARATION(type, PREFIX(argument))
#else
// define flags
#define WDT_OPT(argument, type, description) \
  FLAG_DEFINITION(type, PREFIX(argument), VALUE(argument), description)
#endif
#endif
#endif
