
""" Configurations module

  Configuration to use policies.

  Follows the schema::

    <PolicyNameInCS> : {
               'description' : <some human readable description>,
               'module'      : <policy module name>,
               'command'     : ( <command module name >, < command class name > ),
               'args'        : { arguments for the command } or None
                       }

"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = '$Id: $'


POLICIESMETA = {  # DownTime POLICIES
    'DTOngoing': {'description': "Ongoing and scheduled down-times",
                  'module': 'DowntimePolicy',
                  'command': ('DowntimeCommand', 'DowntimeCommand'),
                  'args': {'hours': 0, 'onlyCache': True}, },

    'DTScheduled1': {'description': "Ongoing and scheduled down-times",
                     'module': 'DowntimePolicy',
                     'command': ('DowntimeCommand', 'DowntimeCommand'),
                     'args': {'hours': 1, 'onlyCache': True}, },

    'DTScheduled3': {'description': "Ongoing and scheduled down-times",
                     'module': 'DowntimePolicy',
                     'command': ('DowntimeCommand', 'DowntimeCommand'),
                     'args': {'hours': 3, 'onlyCache': True}, },

    'DTScheduled': {'description': "Scheduled down-times, starting in <hours>",
                    'module': 'DowntimePolicy',
                    'command': ('DowntimeCommand', 'DowntimeCommand'),
                    'args': {'hours': 12, 'onlyCache': True}, },


    # Free Disk Space in Terabytes
    'FreeDiskSpaceTB': {
        'description': "Free disk space, in TB",
        'module': 'FreeDiskSpacePolicy',
        'command': ('FreeDiskSpaceCommand', 'FreeDiskSpaceCommand'),
        'args': {'unit': 'TB', 'onlyCache': True},
    },

    # Free Disk Space in Gigabytes
    'FreeDiskSpaceGB': {
        'description': "Free disk space, in GB",
        'module': 'FreeDiskSpacePolicy',
        'command': ('FreeDiskSpaceCommand', 'FreeDiskSpaceCommand'),
        'args': {'unit': 'GB', 'onlyCache': True},
    },

    # Free Disk Space in Megabytes
    'FreeDiskSpaceMB': {
        'description': "Free disk space, in MB",
        'module': 'FreeDiskSpacePolicy',
        'command': ('FreeDiskSpaceCommand', 'FreeDiskSpaceCommand'),
        'args': {'unit': 'MB', 'onlyCache': True},
    },

    # GGUS tickets open
    'GGUSTickets': {
        'description': "Open GGUS tickets",
        'module': 'GGUSTicketsPolicy',
        'command': ('GGUSTicketsCommand', 'GGUSTicketsCommand'),
        'args': {'onlyCache': False}
    },

    # Job POLICIES
    'JobDoneRatio': {
        'description': "done / ( completed + done ) jobs ( 30 min )",
        'module': 'JobDoneRatioPolicy',
        'command': ('JobCommand', 'JobCommand'),
        'args': {'onlyCache': True, 'timespan': 1800},
    },

    'JobEfficiency': {
        'description': "( completed + done ) / ( completed + done + failed ) jobs ( 30 min )",
        'module': 'JobEfficiencyPolicy',
        'command': ('JobCommand', 'JobCommand'),
        'args': {'onlyCache': True, 'timespan': 1800},
    },

    'JobRunningMatchedRatio': {
        'description': "running / ( running + matched + received + checking ) jobs ( 30 min )",
        'module': 'JobRunningMatchedRatioPolicy',
        'command': ('JobCommand', 'JobCommand'),
        'args': {'onlyCache': True, 'timespan': 1800},
    },

    'JobRunningWaitingRatio': {
        'description': "running / ( running + waiting + staging ) jobs ( 30 min )",
        'module': 'JobRunningWaitingRatioPolicy',
        'command': ('JobCommand', 'JobCommand'),
        'args': {'onlyCache': True, 'timespan': 1800},
    },


    # Pilot POLICIES..............................................................
    'PilotInstantEfficiency': {
        'description': "Pilots Instant Efficiency ( 30 min )",
        'module': 'PilotEfficiencyPolicy',
        'command': ('PilotCommand', 'PilotCommand'),
        'args': {'onlyCache': True, 'timespan': 1800}
    },

    # Site status propagation POLICIES..............................................................
    'PropagationPolicy': {
        'description': "Site status propagation",
        'module': 'PropagationPolicy',
        'command': ('PropagationCommand', 'PropagationCommand'),
        'args': {'onlyCache': True, 'timespan': 1800}
    },

    # ALWAYS SOMETHING POLICIES...................................................
    'AlwaysActive': {
        'description': "A Policy that always returns Active",
        'module': 'AlwaysActivePolicy',
        'command': None,
        'args': None
    },

    'AlwaysDegraded': {
        'description': "A Policy that always returns Degraded",
        'module': 'AlwaysDegradedPolicy',
        'command': None,
        'args': None
    },

    'AlwaysProbing': {
        'description': "A Policy that always returns Probing",
        'module': 'AlwaysProbingPolicy',
        'command': None,
        'args': None
    },

    'AlwaysBanned': {
        'description': "A Policy that always returns Banned",
        'module': 'AlwaysBannedPolicy',
        'command': None,
        'args': None
    }
}
