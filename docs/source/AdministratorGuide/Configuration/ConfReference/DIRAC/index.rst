DIRAC Section
=============

In this section global attributes are configured.


+---------------------+--------------------------------+----------------+---------------------------------+
| Name                | Description                    | Posible values | Example                         |
+---------------------+--------------------------------+----------------+---------------------------------+
| Extensions          | Define which extensions are    | lhcb, eela     | Extensions = lhcb               |
|                     | going to be used in the server |                |                                 |
+---------------------+--------------------------------+----------------+---------------------------------+
| VirtualOrganization | This option define the default | String         | VirtualOrganization = defaultVO |
|                     | virtual organization           |                |                                 |
+---------------------+--------------------------------+----------------+---------------------------------+



Two subsections are part of DIRAC section:

- Configuration: In this subsection, access to Configuration servers is kept.

- Setups: Define the instance to be used for each the systems of each Setup.

.. toctree::
   :maxdepth: 2
   
   Configuration/index
   Setups/index
   Security/index