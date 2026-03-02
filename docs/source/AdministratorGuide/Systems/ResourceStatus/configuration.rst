.. _rss-configuration:

=================
RSS Configuration
=================

The basic configuration for the RSS is minimal, and must be placed under the Operations section,
preferably on Defaults subsection. ::

  /Operations/Defaults/ResourceStatus
                          /Config
                              Cache       = 720
                              FromAddress = email@address

.. _config section :

--------------
Config section
--------------

This section is all you need to get the RSS working. The parameters are the following:

:Cache: < <int> || 300 ( default if not specified ) > [ seconds ] sets the lifetime for the cached information on RSSCache.
:FromAddress: < <string> || ( default dirac mail address ) > email used t osend the emails from ( sometimes a valid email address is needed ).
