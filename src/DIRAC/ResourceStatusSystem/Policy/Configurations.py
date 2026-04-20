"""Configurations module

Configuration to use policies.

Follows the schema::

  <PolicyNameInCS> : {
             'description' : <some human readable description>,
             'module'      : <policy module name>,
             'command'     : ( <command module name >, < command class name > ),
             'args'        : { arguments for the command } or None
                     }

The values in ``args`` are code-level defaults. They can be overridden per-policy
via the CS entry (e.g. ``Unit = GB`` directly under the policy name in
``/Operations/Defaults/ResourceStatus/Policies/<PolicyName>``).
Deployment-wide defaults can also be set in a command-args section named after
the policy type (e.g. ``/Operations/Defaults/ResourceStatus/Policies/FreeDiskSpace``);
these are picked up by InfoGetter before code-level defaults are applied.

"""

POLICIESMETA = {  # DownTime POLICIES
    "Downtime": {
        "description": "Ongoing or scheduled down-times within <hours> from now (0 = ongoing only)",
        "module": "DowntimePolicy",
        "command": ("DowntimeCommand", "DowntimeCommand"),
        "args": {"hours": 0, "onlyCache": True},
    },
    # Free Disk Space
    "FreeDiskSpace": {
        "description": "Free disk space",
        "module": "FreeDiskSpacePolicy",
        "command": ("FreeDiskSpaceCommand", "FreeDiskSpaceCommand"),
        "args": {
            "unit": "TB",
            "Banned_threshold": 0.1,
            "Degraded_threshold": 5,
            "onlyCache": True,
        },
    },
    # GGUS tickets open
    "GGUSTickets": {
        "description": "Open GGUS tickets",
        "module": "GGUSTicketsPolicy",
        "command": ("GGUSTicketsCommand", "GGUSTicketsCommand"),
        "args": {"onlyCache": False},
    },
    # Job POLICIES
    "JobDoneRatio": {
        "description": "done / ( completed + done ) jobs ( 30 min )",
        "module": "JobDoneRatioPolicy",
        "command": ("JobCommand", "JobCommand"),
        "args": {"onlyCache": True, "timespan": 1800},
    },
    "JobEfficiency": {
        "description": "( completed + done ) / ( completed + done + failed ) jobs ( 30 min )",
        "module": "JobEfficiencyPolicy",
        "command": ("JobCommand", "JobCommand"),
        "args": {"onlyCache": True, "timespan": 1800},
    },
    "JobRunningMatchedRatio": {
        "description": "running / ( running + matched + received + checking ) jobs ( 30 min )",
        "module": "JobRunningMatchedRatioPolicy",
        "command": ("JobCommand", "JobCommand"),
        "args": {"onlyCache": True, "timespan": 1800},
    },
    "JobRunningWaitingRatio": {
        "description": "running / ( running + waiting + staging ) jobs ( 30 min )",
        "module": "JobRunningWaitingRatioPolicy",
        "command": ("JobCommand", "JobCommand"),
        "args": {"onlyCache": True, "timespan": 1800},
    },
    # Pilot POLICIES..............................................................
    "PilotInstantEfficiency": {
        "description": "Pilots Instant Efficiency ( 30 min )",
        "module": "PilotEfficiencyPolicy",
        "command": ("PilotCommand", "PilotCommand"),
        "args": {"onlyCache": True, "timespan": 1800},
    },
    # Site status propagation POLICIES..............................................................
    "PropagationPolicy": {
        "description": "Site status propagation",
        "module": "PropagationPolicy",
        "command": ("PropagationCommand", "PropagationCommand"),
        "args": {"onlyCache": True, "timespan": 1800},
    },
    # ALWAYS SOMETHING POLICIES...................................................
    "AlwaysActive": {
        "description": "A Policy that always returns Active",
        "module": "AlwaysActivePolicy",
        "command": None,
        "args": None,
    },
    "AlwaysDegraded": {
        "description": "A Policy that always returns Degraded",
        "module": "AlwaysDegradedPolicy",
        "command": None,
        "args": None,
    },
    "AlwaysProbing": {
        "description": "A Policy that always returns Probing",
        "module": "AlwaysProbingPolicy",
        "command": None,
        "args": None,
    },
    "AlwaysBanned": {
        "description": "A Policy that always returns Banned",
        "module": "AlwaysBannedPolicy",
        "command": None,
        "args": None,
    },
}
