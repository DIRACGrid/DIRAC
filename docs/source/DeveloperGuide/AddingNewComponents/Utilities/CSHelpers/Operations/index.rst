==================================
Helper for accessing /Operations
==================================

*/Operations* section is *VO* aware. That means that configuration for different *VO* will have a different CS path:

 * For multi-VO installations */Operations/<vo>* should be used.
 * For single-VO installations */Operations* should be used.

In any case, there is the possibility to define a default configuration, for instance */Operations/myvo/Defaults*.
Take a look at :ref:`dirac-operations-cs` for further info.

To ease accessing the */Operations* section a helper has been created. This helper receives the *VO* and will calculate the *Operations* path automatically. Once instanced it's used as the *gConfig* object. An example would be:

.. code-block:: python

   from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations

   ops = Operations(vo="myVO")
   #This would check the following paths and return the first one that is defined
   # 1.- /Operations/myVO/JobScheduling/CheckJobLimits
   # 2.- /Operations/Defaults/JobScheduling/CheckJobLimits
   # 3.- Return True

   print(ops.getValue("JobScheduling/CheckJobLimits", True))


It's not necessary to define the *VO* if a group is known. The helper can extract the *VO* from the group.  For instance:

.. code-block:: python

   from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations

   ops = Operations(group="dirac_user")
