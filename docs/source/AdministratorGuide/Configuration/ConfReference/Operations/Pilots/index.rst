=========================================
Pilot version
=========================================

The */Operations/<vo>/<setup>/Pilot* section define What version of DIRAC will be used to submit pilot jobs to the resources.

=============  ========================================================  ===============================================================================================
Parameter      Description                                               Default value
=============  ========================================================  ===============================================================================================
Version        What project version will be used                         Version with which the component that submits pilot jobs is installed
-------------  --------------------------------------------------------  -----------------------------------------------------------------------------------------------
Project        What installation project will be used when submitting    DIRAC
               pilot jobs to the resources
-------------  --------------------------------------------------------  -----------------------------------------------------------------------------------------------
CheckVersion   Check if the version used by pilot jobs                   True
               is the one that they were submitted with
=============  ========================================================  ===============================================================================================

