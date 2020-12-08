.. _admin_usermanagement:


User management Guides
======================

This section provides information you need for the user management. 

What are the components involved in that.
-----------------------------------------

  * Configuration system

   * Registry
   * VOMS2CSAgent


What is the user in DIRAC context?
----------------------------------

DIRAC users are entities that can be authenticated. Basically, it's a record in the **/Registry** section of the :ref:`Configuration system <configurationSystem>`.
Each user has a username as a unique identifier. Username can be created manually by administrator or automaticaly by scripts.
User attributes are described in the **/Registry/Users** section. To provide authentication a user must have the `DN` attribute, read more about :ref:`authentication <manageAuthNAndAuthZ>`.
User has permissions. To describe permissions we use groups.

What is the DIRAC groups?
-------------------------

DIRAC groups collects DIRAC users and are described in the **/Registry/Groups** section, each subsection of the group is a description of the users permissions and rights for various Grid operations and
is associated with DIRAC Virtual Organization. The name of the DIRAC group usually consists of the DIRAC VO name and a word that describes the permissions for group users,
for example "dteam_user", "dteam_pilot", "dteam_admin".

What is the DIRAC Virtual Organization(VO)?
-------------------------------------------

The VO is a term coming from grid infrastructures where VO parameters are handled by the VOMS services. In DIRAC VO is not necessarily corresponding to some VOMS described VO.
However, the DIRAC VO describtion can include specific VOMS information. It is not manadatory for the DIRAC VO to have the same name as the corresponding VOMS VO.
However, having these names the same can avoid confusions at the expense of having names longer than necessary. DIRAC VO must be described in the **/Registry/VO** section.

Consider the registration process
---------------------------------

User management has been provided by the Registry section of the Configuration System. To manage it you can use:
  * :ref:`dirac commands <registry_cmd>` to managing Registry
  * configuration manager application in the Web portal (need to :ref:`install WebAppDIRAC extension <installwebappdirac>`)
  * modify local cfg file manually (by default it located in /opt/dirac/etc/dirac.cfg)
  * use the :mod:`~DIRAC.ConfigurationSystem.Agent.VOMS2CSAgent` to fetch VOMS VO users

In a nutshell, how to edit the configuration from the portal. First, it should be noted that to be able to do this,
you must be an already registered user in a group that has the appropriate permission to edit the configuration("CSAdministrator").
You need to log in under this user/group and use the Configuration Manager application, then enable the "Manage" mode, this will allow you to make changes.

First of all, define, if necessary, some attributes at the root of the Registry section, that will be applicable for all the configuration are defined.

.. literalinclude:: ../../../../dirac.cfg
  :start-after: ## Registry options:
  :end-before: ##
  :dedent: 2
  :caption: Registry options

To begin, consider how to add new VO to the **Registry/VO** section. Having all the necessary attributes VO you need to add it to the configuration.
One of the mandatory attributes of the VO is the **VOAdmin** (administrator of the VO), it must be registered as a DIRAC user(see the example below).
If the described VO is a VOMS VO, then it is important to note that the VO administrator should have an administrative role in the VOMS VO,
it is necessary to obtain relevant information from the VOMS servers regarding VO users.
In this case, the addition of VOMS VO is considered, respectively, there are attributes specific to VOMS.

.. literalinclude:: ../../../../dirac.cfg
  :start-after: ## VOs
  :end-before: ##
  :dedent: 2
  :caption: Registry section

The next step is to create groups in the  **Registry/Groups** section with the appropriate permissions.
It is usually necessary to have at least three groups:

  * for regular users running tasks(e.g.: "dteam_user")
  * to run pilot tasks(e.g.: "dteam_pilot")
  * administrative group(e.g.: "dteam_admin")

Permissions of the group users are determined by "Properties" option. Full description of all supported :mod:`~DIRAC.Core.Security.Properties`.

.. literalinclude:: ../../../../dirac.cfg
  :start-after: ## Groups:
  :end-before: ##
  :dedent: 2
  :caption: Registry section

To add a new group you can use special DIRAC command that simplifies this action:
:ref:`dirac-admin-add-group <admin_dirac-admin-add-group>`.

Finally, create a user in the **Registry/Users** section:

.. literalinclude:: ../../../../dirac.cfg
  :start-after: ## Users:
  :end-before: ##
  :dedent: 2
  :caption: Registry section

To add a new user you can use special DIRAC command that simplifies this action:
:ref:`dirac-admin-add-user <admin_dirac-admin-add-user>`.
