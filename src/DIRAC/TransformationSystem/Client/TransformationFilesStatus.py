"""
This module contains constants and lists for the possible Transformation files states.
"""
#:
UNUSED = "Unused"
#:
ASSIGNED = "Assigned"
#:
PROCESSED = "Processed"
#:
PROBLEMATIC = "Problematic"
#:
MAX_RESET = "MaxReset"
#:
MISSING_IN_FC = "MissingInFC"
#:
PROB_IN_FC = "ProbInFC"
#:
REMOVED = "Removed"

# below ones are for inherited transformations
#:
UNUSED_INHERITED = "Unused-inherited"
#:
ASSIGNED_INHERITED = "Assigned-inherited"
#:
MAXRESET_INHERITED = "MaxReset-inherited"
#:
PROCESSED_INHERITED = "Processed-inherited"
#:
MOVED = "Moved"


#: Possible states
TRANSFORMATION_FILES_STATES = [
    UNUSED,
    ASSIGNED,
    PROCESSED,
    PROBLEMATIC,
    MAX_RESET,
    MISSING_IN_FC,
    PROB_IN_FC,
    UNUSED_INHERITED,
    ASSIGNED_INHERITED,
    MAXRESET_INHERITED,
    PROCESSED_INHERITED,
    MOVED,
]
