.. _rss-configuration:

=================
RSS Configuration
=================

The basic configuration for the RSS is minimal, and must be placed under the Operations section,
preferably on Defaults subsection. ::

  /Operations/Defaults/ResourceStatus
                          /Config
                              State       = Active
                              Cache       = 720
                              FromAddress = email@address
                              /StatusTypes
                                  default = all
                                  StorageElement = ReadAccess,WriteAccess,CheckAccess,RemoveAccess   

.. _config section :

--------------
Config section
--------------

This section is all you need to get the RSS working. The parameters are the following:
             
:State: < Active || InActive ( default if not specified ) > is the flag used on the ResourceStatus helper to switch between CS and RSS. If Active, RSS is used.
:Cache: < <int> || 300 ( default if not specified ) > [ seconds ] sets the lifetime for the cached information on RSSCache.
:FromAddress: < <string> || ( default dirac mail address ) > email used t osend the emails from ( sometimes a valid email address is needed ).
:StatusTypes: if a ElementType has more than one StatusType ( aka StorageElement ), we have to specify them here, Otherwise, "all" is taken as StatusType.




