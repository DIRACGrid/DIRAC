.. _InputDataResolution:

==================================================
InputDataResolution: giving job access to the data
==================================================

When a job needs access to data, there are two ways data can be accessed:

* either by downloading the file on the local worker node
* or by reading the data remotely, aka ``streaming``.

The resolution is done in the ``JobWrapper`` (see :ref:`jobs`). By default, the resolution logic is implemented in :py:mod:`~DIRAC.WorkloadManagementSystem.Client.InputDataResolution`. It can be overwritten by the Job JDL (see ``InputDataModule`` in :ref:`jdlDescription`), or by the ``/Operations/<>/InputDataPolicy/InputDataModule`` parameter.


You can look into this class for more details, but to summarize:

* it will look into the ``job`` JDL if it can find ``InputDataPolicy`` option. If so, it will use that as the module.
* If not, it will check whether a policy is defined for the site we are running on (in ``/Operations/InputDataPolicy/<site>``).
* If not, it will run the default policy specified in ``/Operations/InputDataPolicy/Default``

The ``InputDataPolicy`` parameter can either be set directly in the JDL, in which case it should be a full module, or it can be set using the ``Job`` class (see :py:meth:`~DIRAC.Interfaces.API.Job.Job.setInputDataPolicy`)

DownloadInputData
=================

This module will download the files locally on the worker node for processing.

See :py:mod:`~DIRAC.WorkloadManagementSystem.Client.DownloadInputData` for details.

InputDataByProtocol
===================

This module will generate the URLs necessary to access the files remotely.

See :py:mod:`~DIRAC.WorkloadManagementSystem.Client.InputDataByProtocol` for details.
