======================================
Developing Commands
======================================

.. warning::
    This instructions here demonstrate how to support both the legacy (Python 2) and future (Python3) installations of DIRAC.
    If only having Python 3 support is acceptable, the requirement for scripts to be in the the *scripts* directory of their parent system will be removed and the only requirement will be for the function to be decorated with the ``@Script()`` decorator.

Commands are one of the main interface tools for the users. Commands are also called *scripts* in DIRAC lingo. 

Where to place scripts
------------------------

All scripts should live in the *scripts* directory of their parent system. For instance, the command:: 

  dirac-wms-job-submit

will live in::

  DIRAC/WorkloadManagementSystem/scripts/dirac-wms-job-submit.py

The command script name is the same as the command name itself with the *.py* suffix appended. When DIRAC client software is installed, 
all scripts will be placed in the installation scripts directory and stripped of the *.py* extension. This is done by the dirac-deploy-scripts command that you should have already done when you installed.
This way users can see all the scripts in a single place and it makes easy to include all the scripts in the system PATH variable.

Coding commands
------------------

All the commands should be coded following a common recipe and having several mandatory parts. 
The instructions below must be applied as close as possible although some variation are allowed according to developer's habits. 

**1.** All scripts must start with a Shebang line like the following::

    #!/usr/bin/env python

which will set the interpreter directive to the python on the environment.

**2.** The next is the documentation line which is describing the command. This same documentation line will be used also the command help information available with the *-h* command switch.

**3.** The majority of the code should be contained with a function, often called ``main`` though this is not required. This function should be wrapped with the ``@Script()`` decorator to allow the DIRAC plugin mechanism to override the script with the function from the highest priority extension.

.. code-block:: python

   #Import the required DIRAC modules
   from DIRAC.Core.Utilities.DIRACScript import DIRACScript as Script
   from DIRAC.Interfaces.API.DIRAC import DIRAC
   from DIRAC import gLogger

   @Script()
   def main():
     # Do stuff

   if __name__ == "__main__":
     main()

**4.** Next the function must be registered as a ``console_scripts`` ``entrypoint`` in the ``setuptools`` metadata (`more details <https://setuptools.readthedocs.io/en/latest/userguide/entry_point.html>`_). This is done by adding a line to the ``console_scripts`` list in ``setup.cfg`` like below, where the first string is the name for the script you want to create, the left hand side of ``:`` is the module that contains your function and the right hand side is the object you want to invoke (e.g. a function).

.. code-block:: cfg

    console_scripts =
        dirac-info = DIRAC.Core.scripts.dirac_info:main
        dirac-proxy-info = DIRAC.FrameworkSystem.scripts.dirac_proxy_info:main

**5.** Users need to specify parameters to scripts to define what they want to do. To do so, they pass arguments when calling the script. The first thing any script has to do is define what options and arguments the script accepts. Once the valid arguments are defined, the script can parse the command line. An example follows which is a typical command description part

.. literalinclude:: dirac_ping_info.py

Let's follow the example step by step. First, we import the required modules from DIRAC. *S_OK* and *S_ERROR* are the default way DIRAC modules return values or errors. The *Script* module is the initialization and command line parser that scripts use to initialize themselves. **No other DIRAC module should be imported here**.

Once the required modules are imported, a *Params* class is defined. This class holds the values for all the command switches together with all their default values. When the class is instantiated, the parameters get the default values in the constructor function. It also has a set of functions that will be called for each switch that is specified in the command line. We'll come back to that later.

Then the list of valid switches and what to do in case they are called is defined using *registerSwtch()* method of the Scripts module. Each switch definition has 4 parameters:

#. Short switch form. It has to be one letter. Optionally it can have ':' after the letter. If the switch has ':' it requires one parameter with the switch. A valid combination for the previous example would be '-r -p 2'. That means show raw results and make 2 pings.
#. Long switch form. '=' is the equivalent of ':' for the short form. The same combination of command switches in a long form will look like '--showRaw --numPings 2'.
#. Definition of the switch. This text will appear in the script help.
#. Function to call if the user uses the switch in order to process the switch value

There are several reserved switches that DIRAC uses by default and cannot be overwritten by the script. Those are:

* *-h* and *--help* show the script help
* *-d* and *--debug* enables debug level for the script. Note that the forms *-dd* and *-ddd* are accepted
  resulting in increasingly higher verbosity level
* *-s* and *--section* changes the default section in the configuration for the script
* *-o* and *--option* set the value of an option in the configuration
* *-c* and *--cert* use certificates to connect to services

All the command line arguments that are not corresponding to the explicitly defined switches are returned by the *getPositionalArguments()* function.

After defining the switches, the *parseCommandLine()* function has to be called. This method not only parses the command line options but also initializes DIRAC collecting all the configuration data. **It is absolutely important to call this function before importing any other DIRAC module**. The callbacks defined for the switches will be called when parsing the command line if necessary. *Even if the switch is not supposed to receive a parameter, the callback has to receive a value*. Switches without callbacks defined can be obtained with *getUnprocessedSwitches()* function.

**5.** Once the command line has been parsed and DIRAC is properly initialized, the rest of the required DIRAC modules can be imported and the script logic can take place.

Having understood the logic of the script, there are few good practices that must be followed:

* Use *DIRAC.exit( exitCode )* instead of *sys.exit( exitCode )*
* Encapsulate the command code into functions / classes so that it can be easily tested
* Usage of *gLogger* instead of *print* is mandatory. The information in the normal command execution 
  must be printed out in the NOTICE logging level.

Example command
-----------------

Applying all the above recommendations, the command implementation can look like this yet another example:

.. literalinclude:: dirac_my_great_script.py
