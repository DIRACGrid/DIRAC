.. image:: _static/DIRAC-logo.png
  :width: 300pt
  :target: http://diracgrid.org

.. The following raw setting for the Guide enlarged title 

.. raw:: html
    
   <style>  p.first { font-size:150%; }
   </style>
   
===================
DIRAC Documentation
===================

The DIRAC (Distributed Infrastructure with Remote Agent Control) project is a complete Grid solution for a community of users such as the LHCb Collaboration. DIRAC forms a layer between a particular community and various compute resources to allow optimized, transparent and reliable usage.

`A high level overview <https://lhcbweb.pic.es/DIRAC/info/general/diracOverview>`_ gives a general idea
of the DIRAC functionality. A more detailed description of the DIRAC system can be found at this 
`location <https://twiki.cern.ch/twiki/pub/LHCb/DiracProjectPage/DIRAC_CHEP07_mod5.pdf>`_.

The DIRAC Workload Management system realizes the task scheduling paradigm with Generic Pilot Jobs ( or Agents ). 
This task scheduling method solves many problems of using unstable distributed computing resources which are 
available in computing grids. In particular, it helps the management of the user activities in large Virtual 
Organizations such as LHC experiments. In more details the DIRAC WMS with Pilot Jobs is described 
`here <https://twiki.cern.ch/twiki/pub/LHCb/DiracProjectPage/DIRAC_Pilots_Note.pdf>`_. 

.. toctree::
   :hidden:
   
   UserGuide/index 
   AdministratorGuide/index
   DeveloperGuide/index
   CodeDocumentation/index 

Documentation sources
=====================

+-----------------------------------------+-----------------------------------------------------------+
|                                         |                                                           |
| :doc:`UserGuide/index`                  | :doc:`DeveloperGuide/index`                               |
|                                         |                                                           |
| Everything users need to know           | Adding new functionality to DIRAC                         |
|                                         |                                                           |
+-----------------------------------------+-----------------------------------------------------------+
|                                         |                                                           |
| :doc:`AdministratorGuide/index`         | :doc:`CodeDocumentation/index`                            |
|                                         |                                                           |
| Administration of the DIRAC service     | Code reference                                            |
|                                         |                                                           |
+-----------------------------------------+-----------------------------------------------------------+

This documentation in also available in `PDF <../latex/DiracDocs.pdf>`_ version


Indices and tables
==================

* :ref:`genindex`
* :ref:`search`

