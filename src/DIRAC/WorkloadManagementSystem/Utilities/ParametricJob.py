""" Utilities to process parametric job definitions and generate
    bunches of parametric jobs. It exposes the following functions:

    getParameterVectorLength() - to get the total size of the bunch of parametric jobs
    generateParametricJobs() - to get a list of expanded descriptions of all the jobs
"""

# Import from DIRACCommon for backward compatibility
from DIRACCommon.WorkloadManagementSystem.Utilities.ParametricJob import (
    getParameterVectorLength,
    generateParametricJobs,
)

# Re-export for backward compatibility
__all__ = ["getParameterVectorLength", "generateParametricJobs"]
