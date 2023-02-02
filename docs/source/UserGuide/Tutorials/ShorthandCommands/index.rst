==================
Shorthand Commands
==================

These commands are aimed at frequest users of a DIRAC UI, especially if you experience latency issues, as this setup offers a caching mechanism.
They do require some initial setup.

#. Setting up your default

You only need to do this once.

    $ source diracos/diracosrc
    $ dirac-proxy-init -g [your DIRAC group]
    $ dconfig --minimal

This will create a skeleton config file in ``~/.dirac/dcommands.conf``
Now edit the configuration file using your details. If you are a member of more than one VO, you might want to setup different profiles for each.

    [global]
    default_profile = my_dirac_group

    [my_dirac_group]
    group_name = my_dirac_group
    home_dir = /my_dirac_group/user/m/mydir
    default_se = MYHOME-SE-disk


#. Session Initialization

You need to do this for every session.

    $ source diracos/diracosrc
    $ dinit

Now you are set up to use the DIRAC short commands. For a full list of what is available,
please see the :ref:`commands_reference` section.
