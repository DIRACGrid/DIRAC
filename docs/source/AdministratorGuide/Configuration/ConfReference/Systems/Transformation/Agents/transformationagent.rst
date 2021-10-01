Systems / Transformation / <INSTANCE> / Agents / TransformationAgent - Sub-subsection
=====================================================================================

The TransformationAgent processes transformations found in the transformation database.

Specific options defined in this sub-sections are:
* TransformationTypes : list of transformation types handled by this specific agent
* transformationStatus : list of statues considered by the agent
* MaxFilesToProcess : maximum number of files passed to the plugin. This can be overwritten for individual plugins (see below)
* ReplicaCacheValidity : validity of hte replica cache (in days)
* maxThreadsInPool : maximum number of threads to be used
* NoUnusedDelay : number of hours until the plugin is called again in case there is no new Unused files since last time

+------------------------------+------------------------------------------------------------+
| **Name**                     | **Example**                                                |
+------------------------------+------------------------------------------------------------+
| PluginLocation               | DIRAC.TransformationSystem.Agent.TransformationPlugin      |
+------------------------------+------------------------------------------------------------+
| transformationStatus         | Active, Completing, Flush                                  |
+------------------------------+------------------------------------------------------------+
| MaxFilesToProcess            | 5000                                                       |
+------------------------------+------------------------------------------------------------+
| TransformationTypes          | Replication                                                |
+------------------------------+------------------------------------------------------------+
| ReplicaCacheValidity         | 2                                                          |
+------------------------------+------------------------------------------------------------+
| maxThreadsInPool             | 1                                                          |
+------------------------------+------------------------------------------------------------+
| NoUnusedDelay                | 6                                                          |
+------------------------------+------------------------------------------------------------+
| Transformation               | All                                                        |
+------------------------------+------------------------------------------------------------+

This Agent also reads some options from :ref:`operations_transformations`:

* DataProcessing
* DataManipulation

And from :ref:`operations_transformationplugins` , depending on the plugin used
for the Transformation.

* SortedBy
* MaxFilesToProcess: supersede the agent's setting
* NoUnusedDelay: supersede the agent's setting
