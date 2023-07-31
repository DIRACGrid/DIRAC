.. image:: _static/DIRAC-logo.png
  :width: 300pt
  :target: http://diracgrid.org

The DIRAC interware is a complete Grid solution for one,
or more than one community of users that need to exploit distributed heterogeneous resources.

DIRAC forms a layer between a community and various compute resources to allow optimized, transparent and reliable usage.
The types of resources that DIRAC can handle include:

* *Computing* Resources, including Grids, Clouds, HPCs and Batch systems
* *Storage* Resources
* *Catalog* Resources

Many communities use DIRAC, the oldest and most experienced being the `LHCb <http://lhcb-public.web.cern.ch/lhcb-public/>`_ collaboration.
Other communities include, but are not limited to, `Belle2 <https://www.belle2.org/>`_, `ILC <http://www.linearcollider.org/ILC>`_,
and `CTA <https://www.cta-observatory.org/>`_

DIRAC source code is open source (GPLv3), written in `python <https://docs.python.org/>`_,
and hosted on `github <https://github.com/DIRACGrid>`_.

An alternative description of the DIRAC system can be found in this `presentation <https://indico.cern.ch/event/505613/contributions/2227928/>`_


.. toctree::
   :hidden:

   UserGuide/index
   AdministratorGuide/index
   DeveloperGuide/index
   CodeDocumentation/index

=============
Documentation
=============

.. grid:: 2
   :padding: 3
   :gutter: 3

   .. grid-item-card::
      :shadow: lg
      :text-align: center
      :link-type: ref
      :class-body: btn-link stretched-link font-weight-bold
      :class-img-top: p-5
      :img-top: _static/dirac_user.png
      :link: user-guide
      :link-alt: User Guide

      User Guide

      including client installation

   .. grid-item-card::
      :shadow: lg
      :text-align: center
      :link-type: ref
      :class-body: btn-link stretched-link font-weight-bold
      :class-img-top: p-5
      :img-top: _static/dirac_dev.png
      :link: developer_guide
      :link-alt: Developer Guide

      Developer Guide

      adding new functionality to DIRAC


   .. grid-item-card::
      :shadow: lg
      :text-align: center
      :link-type: ref
      :class-body: btn-link stretched-link font-weight-bold
      :class-img-top: p-5
      :img-top: _static/dirac_admin.png
      :link: administrator_guide
      :link-alt: Administrator Guide

      Administrator Guide

      services administration, server installation


   .. grid-item-card::
      :shadow: lg
      :text-align: center
      :link-type: ref
      :class-body: btn-link stretched-link font-weight-bold
      :class-img-top: p-5
      :img-top: _static/dirac_code.png
      :link: code_documentation
      :link-alt: Code Reference

      code reference

:ref:`genindex`
