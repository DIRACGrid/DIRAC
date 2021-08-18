.. image:: _static/DIRAC-logo.png
  :width: 300pt
  :target: http://diracgrid.org

.. The following raw setting for the Guide enlarged title

The DIRAC project is a complete Grid solution for one,
or more than one community of users that need to exploit distributed, heterogeneous resources.

DIRAC forms a layer between a community and various compute resources to allow optimized, transparent and reliable usage.
The types of resources that DIRAC can handle include:

  - *Computing* Resources, including Grids, Clouds, and Batch systems
  - *Storage* Resources
  - *Catalog* Resources

Many communities use DIRAC, the oldest and most experienced being the `LHCb <http://lhcb-public.web.cern.ch/lhcb-public/>`_ collaboration.
Other communities include, but are not limited to, `Belle2 <https://www.belle2.org/>`_, `ILC <http://www.linearcollider.org/ILC>`_,
and `CTA <https://www.cta-observatory.org/>`_

DIRAC source code is open source (GPLv3), written largely in `python 2.7 <https://docs.python.org/2/>`_,
and hosted on `github <https://github.com/DIRACGrid>`_.

A more detailed description of the DIRAC system can be found at this
`location <https://twiki.cern.ch/twiki/pub/LHCb/DiracProjectPage/DIRAC_CHEP07_mod5.pdf>`_
or in this `presentation <https://indico.cern.ch/event/505613/contributions/2227928/>`_


.. toctree::
   :hidden:

   UserGuide/index
   AdministratorGuide/index
   DeveloperGuide/index
   CodeDocumentation/index

=============
Documentation
=============

.. panels::
  :card: shadow + text-center
  :img-top-cls: p-5

  :img-top: _static/dirac_user.png
  .. link-button:: UserGuide/index
      :type: ref
      :text: User Guide
      :classes: btn-link stretched-link font-weight-bold

  including client installation

  ---
  :img-top: _static/dirac_dev.png
  .. link-button:: DeveloperGuide/index
      :type: ref
      :text: Developer Guide
      :classes: btn-link stretched-link font-weight-bold
  
  adding new functionality to DIRAC

  ---
  :img-top: _static/dirac_admin.png
  .. link-button:: AdministratorGuide/index
      :type: ref
      :text: Administrator Guide
      :classes: btn-link stretched-link font-weight-bold
  
  services administration, server installation

  ---
  :img-top: _static/dirac_code.png
  .. link-button:: CodeDocumentation/index
      :type: ref
      :text: Code Documentation
      :classes: btn-link stretched-link font-weight-bold

  code reference

:ref:`genindex`
