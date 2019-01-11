========================
dirac-rss-policy-manager
========================

DIRAC version: v6r20-pre17

Script to manage the Policy section within a given CS setup of a given dirac cfg file.
It allows you to

- view the policy current section (no option needed)
- test all the policies that apply for a given 'element', 'elementType' or element 'name'
  (one of the aforementioned options is needed)

- update/add a policy to a given dirac cfg file (no option needed)
- remove a policy from a given dirac cfg file ('policy' option needed)
- restore the last backup of the diarc config file, to undo last changes (no option needed)

Usage::

    dirac-rss-policy-manager [option] <command>

Commands::

    [test|view|update|remove]

Verbosity::

    -o LogLevel=LEVEL     NOTICE by default, levels available: INFO, DEBUG, VERBOSE..

Options::

  --elementType=           : ElementType narrows the search; None if default
  --element=               : Element family ( Site, Resource )
  --name=                  : ElementName; None if default
  --setup=                 : Setup where the policy section should be retrieved from; 'Defaults' by default
  --file=                  : Fullpath config file location other then the default one (but for testing use only the original)
  --policy=                : Policy name to be removed
