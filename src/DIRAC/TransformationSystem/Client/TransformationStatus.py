"""
This module contains constants and lists for the possible transformation states.
"""

#:
NEW = "New"
#:
IDLE = "Idle"
#:
ACTIVE = "Active"
#:
FLUSH = "Flush"
#: Status for transformations that are being "derived" from
COMPLETING = "Completing"
#:
COMPLETED = "Completed"
#:
ARCHIVED = "Archived"
#:
CLEANING = "Cleaning"
#:
CLEANED = "Cleaned"
#:
STOPPED = "Stopped"
#:
DELETED = "Deleted"
#:
TRANSFORMATIONCLEANED = "TransformationCleaned"
#:

# States list that don't seem to be set anywhere in vanilla DIRAC:
# ValidatingInput, RemovingFiles, RemovedFiles, ValidatingOutput
# ValidateOutputDataAgent can set ValidatedOutput and WaitingIntegrity states, but on non reach-able conditions (?)


#: Possible Transformation states
TRANSFORMATION_STATES = [
    NEW,
    IDLE,
    ACTIVE,
    FLUSH,
    COMPLETING,
    COMPLETED,
    ARCHIVED,
    CLEANING,
    CLEANED,
    STOPPED,
    DELETED,
    TRANSFORMATIONCLEANED,
]

#: Transformation States when new TransformationTasks might still be created
TRANSFORMATION_ACTIVE_STATES = [NEW, IDLE, ACTIVE, FLUSH, COMPLETING, STOPPED]

#: Transformation States when new TransformationTasks won't be created
TRANSFORMATION_FINAL_STATES = [COMPLETED, ARCHIVED, CLEANING, CLEANED, TRANSFORMATIONCLEANED, DELETED]

#: TS internal Transformation States indicating the Transformation won't be updated
TRANSFORMATION_REALLY_FINAL_STATES = [ARCHIVED, CLEANED, DELETED]
