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

  /Operations/[Defaults|SetupName]/ResourceStatus
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

-------------
PolicyActions
-------------

It applies the same idea as in `Policies`_, but the number of options is larger.

::

  /Operations/[Defaults|SetupName]/ResourceStatus
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

  /Operations/[Defaults|SetupName]/ResourceStatus
                          /Notification
                              /NotificationGroupName
                                  users = email@address, email@address

* NotificationGroupName : human readable of what the group represents
* users : CSV with email addresses

The EmailAgent will take care of sending the appropriate Emails of notification.
