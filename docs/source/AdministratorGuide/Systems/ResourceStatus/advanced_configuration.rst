.. _rss_advanced_configuration:

======================
Advanced Configuration
======================

The full RSS configuration comprises 4 main sections

* `Config`_
* `Policies`_
* `PolicyActions`_
* `Notification`_

------
Config
------

Already described in :ref:`config section <config section>`.

--------
Policies
--------

This section describes the policies and the conditions to match elements.

::

  /Operations/Defaults/ResourceStatus
                          /Policies
                              /PolicyName
                                  policyType = policyType
                                  doNotCombineResult = something
                                  /matchParams
                                      element = element
                                      elementType = elementType
                                      name = name
                                      statusType = statusType
                                      status = status
                                      reason = reason
                                      tokenOwner = tokenOwner
                                      active = Active

This is the complete definition of a policy. Let's go one by one.

* PolicyName         : this must be a human readable name explaining what the policy is doing ( mandatory ).
* policyType         : is the name of the policy we want to run as defined in DIRAC.ResourceStatusSystem.Policy.Configurations ( mandatory ).
* doNotCombineResult : if this option is present, the status will not be merged with the rest of statuses ( but actions on this policy will apply ).
* matchParams        : is the dictionary containing the policy metadata used by :ref:`Info Getter <info getter>` to match policies. Any of them can be a CSV.

.. note :: Remember, declare ONLY the parameters in match params that want to be taken into account.

There is one caveat. If we want to match the following SEs: CERN-USER for ReadAccess and PIC-USER for WriteAccess,
we cannot define the following matchParams:

::

 .../matchParams
        element = Resource
        elementType = StorageElement
        name = CERN-USER, PIC-USER
        statusType = ReadAccess, WriteAccess

.. warning :: This setting will match the cartesian product of name x statusType. We will match CERN-USER for WriteAccess and PIC-USER for ReadAccess as well. We will need two separate policies.

.. seealso::

   Code templates and examples for creating custom policies: :doc:`../../../DeveloperGuide/Systems/ResourceStatus/index`

Built-in Downtime Policy
~~~~~~~~~~~~~~~~~~~~~~~~

The ``Downtime`` policy type evaluates GOCDB downtime data for a Site or Resource.
Severity is mapped to RSS status as follows:

* **OUTAGE** → **Banned**
* **WARNING** → **Degraded**
* No downtime → **Active**

The look-ahead window is configurable from the Operations CS:

::

  /Operations/Defaults/ResourceStatus
                          /Policies
                              /Downtime
                                  hours = 0   # hours to look ahead (0 = ongoing only, default)

.. note::

   Setting ``hours = 0`` (the default) means only downtimes that are currently ongoing
   are considered. Setting a positive value (e.g. ``12``) also catches downtimes scheduled
   to start within that window, which is useful for proactive status changes.

Example: flag elements with downtimes starting within the next 24 hours::

  /Operations/Defaults/ResourceStatus/Policies/Downtime
  {
    hours = 24
  }

Built-in FreeDiskSpace Policy
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The ``FreeDiskSpace`` policy type monitors Storage Element occupancy.
It compares the free space reported by the SE against two configurable thresholds:

* If free space is below ``Banned_threshold``, the SE is set to **Banned**.
* If free space is below ``Degraded_threshold`` (but above ``Banned_threshold``), the SE is set to **Degraded**.
* Otherwise the SE is set to **Active**.

All three parameters — unit, banned threshold, and degraded threshold — are fully configurable
from the Operations CS and fall back to safe defaults:

::

  /Operations/Defaults/ResourceStatus
                          /Policies
                              /FreeDiskSpace
                                  Unit               = TB     # unit for the SE occupancy query (TB, GB or MB)
                                  Banned_threshold   = 0.1    # in the chosen unit (default)
                                  Degraded_threshold = 5      # in the chosen unit (default)

.. note::

   These keys live under ``/Operations/Defaults/ResourceStatus/Policies/FreeDiskSpace``,
   not under the ``/matchParams`` sub-section.  They tune the **command arguments**, not the
   element-matching logic.

   The default values of ``0.1`` and ``5`` are always used as fallback regardless of unit.
   Make sure to set meaningful threshold values explicitly in the CS when changing the unit.

Example: use GB with tighter thresholds::

  /Operations/Defaults/ResourceStatus/Policies/FreeDiskSpace
  {
    Unit               = GB
    Banned_threshold   = 100
    Degraded_threshold = 5000
  }

-------------
PolicyActions
-------------

It applies the same idea as in `Policies`_, but the number of options is larger.

::

  /Operations/Defaults/ResourceStatus
                          /PolicyActions
                              /PolicyActionName
                                  actionType = actionType
                                  notificationGroups = notificationGroups
                                  /matchParams
                                      element = element
                                      elementType = elementType
                                      name = name
                                      statusType = statusType
                                      status = status
                                      reason = reason
                                      tokenOwner = tokenOwner
                                      active = Active
                                  /combinedResult
                                      Status = Status
                                      Reason = Reason
                                  /policyResults
                                      policyName = policyStatus

.. note :: Mind te upper / lower case ( to be fixed )

* PolicyActionName : must be a human readable name explaining what the action will do ( mandatory ).
* actionType : is one of the following :ref:`actions <actions>` ( mandatory ).
* notificationGroups : if required by the actionType, one of `Notification`_.
* matchParams : as explained in `Policies`_.
* combinedResult : this is the computed final result after merging the single policy results.
* policyResults : allows to trigger an action based on a single policy result, where policyName follows `Policies`_.

Now that you have configured the policies, restart the ElementInspectorAgent and the SiteInspectorAgent,
and see if the run the policies defined.

------------
Notification
------------

This section defines the notification groups ( right now, only for EmailAction ).

::

  /Operations/Defaults/ResourceStatus
                          /Notification
                              /NotificationGroupName
                                  users = email@address, email@address

* NotificationGroupName : human readable of what the group represents
* users : CSV with email addresses

The EmailAgent will take care of sending the appropriate Emails of notification.
