.. _shorthand_commands_tutorial:

==================
Shorthand Commands
==================

These commands are aimed at frequent users of a DIRAC UI, especially if you experience latency issues, as this setup offers a caching mechanism.
They do require some initial setup.


#. Before using any DIRAC UI commands you need to set up your DIRAC UI environment and get a proxy::

        $ source diracos/diracosrc
        $ dirac-proxy-init -g [your DIRAC group]

#. Setting up your defaults.

   You only need to do this once::

        $ dconfig --minimal

   This will create a skeleton config file in ``~/.dirac/dcommands.conf``

   Now edit the configuration file using your details. If you are a member of more than one VO, you might want to set up different profiles for each. Your current group will be automatically picked up from your proxy.

   .. code-block:: cfg

    [global]
    default_profile = myvo_user

    [myvo_user]
    group_name = myvo_user
    home_dir = /myvo/user/m/mydir
    default_se = MYHOME-SE-disk

    [my2vo_prod]
    group_name = my2vo_prod
    home_dir = /my2vo/prod/mc
    default_se = ANOTHER-SE-disk

   Now you are set up to use the DIRAC short commands. For a full list of what is available, please see the :ref:`shorthand_cmd` section.
