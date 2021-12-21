==================================
Helper for accessing /Operations
==================================

*/Operations* section is *VO* and *setup* aware. That means that configuration for different *VO/setup* will have a different CS path:

 * For multi-VO installations */Operations/<vo>/<setup>* should be used.
 * For single-VO installations */Operations/<setup>* should be used.

In any case, there is the possibility to define a default configuration, that is valid for all the *setups*. The *Defaults* keyword can be used instead of the setup. For instance */Operations/myvo/Defaults*.

Parameters defined for a specific setup take precedence over parameters defined for the *Defaults* setup. Take a look at :ref:`dirac-operations-cs` for further info.

To ease accessing the */Operations* section a helper has been created. This helper receives the *VO* and the *Setup* at instantiation and
will calculate the *Operations* path automatically. Once instanced it's used as the *gConfig* object. An example would be:

.. code-block:: python

   from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations

   ops = Operations(vo = "dirac", setup="Production")
   #This would check the following paths and return the first one that is defined
   # 1.- /Operations/dirac/Production/JobScheduling/CheckJobLimits
   # 2.- /Operations/dirac/Defaults/JobScheduling/CheckJobLimits
   # 3.- Return True

   print(ops.getValue("JobScheduling/CheckJobLimits", True))


It's not necessary to define the *VO* if a group is known. The helper can extract the *VO* from the group.  It's also possible to skip the setup parameter and let it discover itself.  For instance:

.. code-block:: python

   from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations

   ops = Operations(group="dirac_user")
