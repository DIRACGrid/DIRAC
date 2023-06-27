"""
This module contains constants and lists for the possible scopes to interact with pilots on CEs.
"""

# Based on: https://github.com/WLCG-AuthZ-WG/common-jwt-profile/blob/master/profile.md#capability-based-authorization-scope

#: To submit pilots:
CREATE = "compute.create"
#: To cancel pilots:
CANCEL = "compute.cancel"
#: To modify attributes of submitted pilots:
MODIFY = "compute.modify"
#: To read information about submitted pilots:
READ = "compute.read"

#: Possible pilot scopes:
PILOT_SCOPES = [CANCEL, CREATE, MODIFY, READ]
