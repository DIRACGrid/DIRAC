================================
dirac-admin-check-config-options
================================

Compare the ConfigTemplate with current configuration.

Usage::

  dirac-admin-check-config-options [option|cfgfile] -[MAUO] [-S <system]

Options::

  -S  --system <value>         : Systems to check, by default all of them are checked
  -M  --modified               : Show entries which differ from the default
  -A  --added                  : Show entries which do not exist in ConfigTemplate
  -U  --missingSection         : Show sections which do not exist in the current configuration
  -O  --missingOption          : Show options which do not exist in the current configuration
