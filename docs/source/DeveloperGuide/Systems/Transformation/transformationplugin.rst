.. _dev-ts-transformationagent-plugins:

Create a TransformationAgent Plugin
===================================

This page briefly explains the steps necessary to add a new TransformationPlugin to DIRAC, or an extension.
It assumes the reader has some form of a development and testing setup available to them.

The file :mod:`DIRAC/TransformationSystem/Agent/TransformationPlugin.py
<DIRAC.TransformationSystem.Agent.TransformationPlugin>` contains the ``TransformationPlugins`` for DIRAC.
Plugins need to be registered in the configuration system, and the plugin function has to follow the expected return
value structure explained in :mod:`~DIRAC.TransformationSystem.Agent.TransformationPlugin`.

.. note ::

  If you create sufficiently generic plugins, they will be welcome in the vanilla DIRAC code base. If the plugin you
  create is too much focused on your own work you will need to create an extension of DIRAC and create an inherited
  TransformationPlugin class and tell the :mod:`DIRAC.TransformationSystem.Agent.TransformationAgent` where to find the
  plugin module of the extension.


Add New Plugin to the List of Allowed Plugins
---------------------------------------------

The plugins that can be used inside the Transformation System, need to be added in the list of allowed plugins in the
``Operations/Transformations/AllowedPlugins`` option in the Configuration System.

If the option ``AllowedPlugins`` is already defined, simply add the new plugin::

  Transformations
  {
    AllowedPlugins = ...
    AllowedPlugins += ...
    AllowedPlugins += MyNewPlugin
  }
  
Or, if it is not defined, you need to add the ``AllowedPlugins`` option including the list of default plugins, otherwise the other plugins would stop working::

  Transformations
  {
    AllowedPlugins = Broadcast
    AllowedPlugins += Standard
    AllowedPlugins += BySize
    AllowedPlugins += ByShare
    AllowedPlugins += MyNewPlugin
  }

After adding to the ``AllowedPlugins`` option and the code for the plugin, the ``TransformationAgent`` should be
restarted to reloads the configuration and source code.

The EvenOdd Plugin Example
--------------------------

This function shows an example plugin that groups files into even or odd numbers. See the comments to explain how to
obtain LFNs and transformation parameters.

.. code-block:: python
   :caption: Should be part of DIRAC/TransformationSystem/Agent/TransformationPlugin.py
   :linenos:

    def _EvenOdd(self):
      """Group files by Even (0,2,4,6,8) or Odd (1,3,5,7,9) numbering.

      Use Transformation Parameter 'EvenOdd' to chose 'Even' or 'Odd', or 'Both'

      :returns: S_OK with list of tuples. Each Tuple is a pair of SE and list of
                LFNs to treat in given task
      """
      # log parameters
      self.util.logInfo('Running EvenOdd with parameters')
      for param, value in self.params.iteritems():
        self.util.logInfo('%s=%r' % (param, value))

      # the transformation system tells us about the unused LFNs
      lfns = self.data
      self.util.logInfo('Treating the following LFNS:')
      for lfn in lfns:
        self.util.logInfo(lfn)

      # Assuming files end with _[0-9]+
      odd, even = [], []
      for lfn in lfns:
        self.util.logInfo(' odd or even?: %r' % lfn)
        number = int(lfn.rsplit('_', 1)[1])
        if number % 2 != 0:
          self.util.logInfo(' odd')
          odd.append(lfn)
        else:
          self.util.logInfo('even')
          even.append(lfn)

      # Treat only, even or odd numbers, or both
      evenOrOdd = self.params.get('EvenOdd', 'Both')
      if evenOrOdd == 'Both':
        selection = [even, odd]
      elif evenOrOdd == 'Even':
        selection = [even]
      elif evenOrOdd == 'Odd':
        selection = [odd]
      else:
        return S_ERROR("Bad Parameter Value")

      tasks = []
      groupSize = self.params['GroupSize']
      for chunks in selection:
        for chunk in breakListIntoChunks(chunks, groupSize):
          tasks.append(('', chunk))

      self.util.logInfo('Tasks: %r' % tasks)
      return S_OK(tasks)


Using the EvenOdd Plugin
------------------------

When a transformation is created, set the `EvenOdd` plugin with `setPlugin` and set the 'EvenOdd' parameter to 'Odd'
with `setEvenOdd`, and then execute this function to test it.

.. code-block:: python
   :caption: createEvenOdd.py
   :linenos:

    from DIRAC import gLogger, S_OK, S_ERROR
    from DIRAC.Core.Utilities.DIRACScript import DIRACScript as Script
    Script.parseCommandLine()

    from DIRAC.TransformationSystem.Client.Transformation import Transformation

    myTrans = Transformation()
    uniqueIdentifier = "OddOnly"
    myTrans.setTransformationName("ReplicateAndRegister_%s" % uniqueIdentifier)
    myTrans.setDescription("Replicate only Odd files from StorageElementOne")
    myTrans.setLongDescription("Replicate only Odd files from StorageElementOne")
    myTrans.setType('Replication')
    myTrans.setTransformationGroup('MyGroup')
    myTrans.setGroupSize(2)

    # Set the 'EvenOdd' plugin
    myTrans.setPlugin('EvenOdd')
    # set the 'EvenOdd' parameter to 'Odd', we can use python to
    # automagically turn a myTrans.set<PARAMETER> function into a
    # transformation parameter
    myTrans.setEvenOdd('Odd')
    myTrans.setSomeOtherParameter('Value')

    targetSE = 'StorageElementOne'
    myTrans.setBody([("ReplicateAndRegister", {"TargetSE": targetSE, "SourceSE": ''})])
    myTrans.setTargetSE(targetSE)
    res = myTrans.addTransformation()
    if not res['OK']:
      gLogger.error("Failed to add the transformation: %s" % res['Message'])
      exit(1)

    # now activate the transformation
    myTrans.setStatus('Active')
    myTrans.setAgentType('Automatic')
    transID = myTrans.getTransformationID()['Value']
    from DIRAC.TransformationSystem.Client.TransformationClient import TransformationClient
    metadata = {'TransformationID': 2}
    res = TransformationClient().createTransformationInputDataQuery(transID, metadata)
    gLogger.notice('Added input data query', res)
    gLogger.notice('Created EvenOdd transformation: %r' % transID)
    exit(0)
