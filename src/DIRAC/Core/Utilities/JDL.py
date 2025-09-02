"""Transformation classes around the JDL format."""

from DIRACCommon.Core.Utilities.JDL import *  # noqa: F403,F401

from DIRAC.WorkloadManagementSystem.Utilities.JobModel import BaseJobDescriptionModel


def jdlToBaseJobDescriptionModel(classAd: ClassAd):
    """
    Converts a JDL string into a JSON string for data validation from the BaseJob model
    This method allows compatibility with older Client versions that used the _toJDL method
    """
    try:
        jobDescription = BaseJobDescriptionModel(
            executable=classAd.getAttributeString(EXECUTABLE),
        )
        if classAd.lookupAttribute(ARGUMENTS):
            jobDescription.arguments = classAd.getAttributeString(ARGUMENTS)
            classAd.deleteAttribute(ARGUMENTS)

        if classAd.lookupAttribute(BANNED_SITES):
            jobDescription.bannedSites = classAd.getListFromExpression(BANNED_SITES)
            classAd.deleteAttribute(BANNED_SITES)

        if classAd.lookupAttribute(CPU_TIME):
            jobDescription.cpuTime = classAd.getAttributeInt(CPU_TIME)
            classAd.deleteAttribute(CPU_TIME)

        if classAd.lookupAttribute(EXECUTABLE):
            jobDescription.executable = classAd.getAttributeString(EXECUTABLE)
            classAd.deleteAttribute(EXECUTABLE)

        if classAd.lookupAttribute(EXECUTION_ENVIRONMENT):
            executionEnvironment = classAd.getListFromExpression(EXECUTION_ENVIRONMENT)
            if executionEnvironment:
                jobDescription.executionEnvironment = {}
                for element in executionEnvironment:
                    key, value = element.split("=")
                    if value.isdigit():
                        value = int(value)
                    else:
                        try:
                            value = float(value)
                        except ValueError:
                            pass
                    jobDescription.executionEnvironment[key] = value
            classAd.deleteAttribute(EXECUTION_ENVIRONMENT)

        if classAd.lookupAttribute(GRID_CE):
            jobDescription.gridCE = classAd.getAttributeString(GRID_CE)
            classAd.deleteAttribute(GRID_CE)

        if classAd.lookupAttribute(INPUT_DATA):
            jobDescription.inputData = classAd.getListFromExpression(INPUT_DATA)
            classAd.deleteAttribute(INPUT_DATA)

        if classAd.lookupAttribute(INPUT_DATA_POLICY):
            jobDescription.inputDataPolicy = classAd.getAttributeString(INPUT_DATA_POLICY)
            classAd.deleteAttribute(INPUT_DATA_POLICY)

        if classAd.lookupAttribute(INPUT_SANDBOX):
            jobDescription.inputSandbox = classAd.getListFromExpression(INPUT_SANDBOX)
            classAd.deleteAttribute(INPUT_SANDBOX)

        if classAd.lookupAttribute(JOB_CONFIG_ARGS):
            jobDescription.jobConfigArgs = classAd.getAttributeString(JOB_CONFIG_ARGS)
            classAd.deleteAttribute(JOB_CONFIG_ARGS)

        if classAd.lookupAttribute(JOB_GROUP):
            jobDescription.jobGroup = classAd.getAttributeString(JOB_GROUP)
            classAd.deleteAttribute(JOB_GROUP)

        if classAd.lookupAttribute(JOB_NAME):
            jobDescription.jobName = classAd.getAttributeString(JOB_NAME)
            classAd.deleteAttribute(JOB_NAME)

        if classAd.lookupAttribute(JOB_TYPE):
            jobDescription.jobType = classAd.getAttributeString(JOB_TYPE)
            classAd.deleteAttribute(JOB_TYPE)

        if classAd.lookupAttribute(LOG_LEVEL):
            jobDescription.logLevel = classAd.getAttributeString(LOG_LEVEL)
            classAd.deleteAttribute(LOG_LEVEL)

        if classAd.lookupAttribute(NUMBER_OF_PROCESSORS):
            jobDescription.maxNumberOfProcessors = classAd.getAttributeInt(NUMBER_OF_PROCESSORS)
            jobDescription.minNumberOfProcessors = classAd.getAttributeInt(NUMBER_OF_PROCESSORS)
            classAd.deleteAttribute(NUMBER_OF_PROCESSORS)
            classAd.deleteAttribute(MAX_NUMBER_OF_PROCESSORS)
            classAd.deleteAttribute(MIN_NUMBER_OF_PROCESSORS)
        else:
            if classAd.lookupAttribute(MAX_NUMBER_OF_PROCESSORS):
                jobDescription.maxNumberOfProcessors = classAd.getAttributeInt(MAX_NUMBER_OF_PROCESSORS)
                classAd.deleteAttribute(MAX_NUMBER_OF_PROCESSORS)
            if classAd.lookupAttribute(MIN_NUMBER_OF_PROCESSORS):
                jobDescription.minNumberOfProcessors = classAd.getAttributeInt(MIN_NUMBER_OF_PROCESSORS)
                classAd.deleteAttribute(MIN_NUMBER_OF_PROCESSORS)

        if classAd.lookupAttribute(OUTPUT_DATA):
            jobDescription.outputData = set(classAd.getListFromExpression(OUTPUT_DATA))
            classAd.deleteAttribute(OUTPUT_DATA)

        if classAd.lookupAttribute(OUTPUT_SANDBOX):
            jobDescription.outputSandbox = set(classAd.getListFromExpression(OUTPUT_SANDBOX))
            classAd.deleteAttribute(OUTPUT_SANDBOX)

        if classAd.lookupAttribute(OUTPUT_PATH):
            jobDescription.outputPath = classAd.getAttributeString(OUTPUT_PATH)
            classAd.deleteAttribute(OUTPUT_PATH)

        if classAd.lookupAttribute(OUTPUT_SE):
            jobDescription.outputSE = classAd.getAttributeString(OUTPUT_SE)
            classAd.deleteAttribute(OUTPUT_SE)

        if classAd.lookupAttribute(SITE):
            jobDescription.sites = classAd.getListFromExpression(SITE)
            classAd.deleteAttribute(SITE)

        if classAd.lookupAttribute(PLATFORM):
            jobDescription.platform = classAd.getAttributeString(PLATFORM)
            classAd.deleteAttribute(PLATFORM)

        if classAd.lookupAttribute(PRIORITY):
            jobDescription.priority = classAd.getAttributeInt(PRIORITY)
            classAd.deleteAttribute(PRIORITY)

        if classAd.lookupAttribute(STD_OUTPUT):
            jobDescription.stdout = classAd.getAttributeString(STD_OUTPUT)
            classAd.deleteAttribute(STD_OUTPUT)

        if classAd.lookupAttribute(STD_ERROR):
            jobDescription.stderr = classAd.getAttributeString(STD_ERROR)
            classAd.deleteAttribute(STD_ERROR)

        if classAd.lookupAttribute(TAGS):
            jobDescription.tags = classAd.getListFromExpression(TAGS)
            classAd.deleteAttribute(TAGS)

        # Remove credentials
        for attribute in CREDENTIALS_FIELDS:
            classAd.deleteAttribute(attribute)

        # Remove legacy attributes
        for attribute in {"DIRACSetup", "OwnerDN"}:
            classAd.deleteAttribute(attribute)

        for attribute in classAd.getAttributes():
            if not jobDescription.extraFields:
                jobDescription.extraFields = {}

            value = classAd.getAttributeString(attribute)
            if value.isdigit():
                value = int(value)
            else:
                try:
                    value = float(value)
                except ValueError:
                    pass

            jobDescription.extraFields[attribute] = value

    except ValidationError as e:
        return S_ERROR(f"Invalid JDL: {e}")

    return S_OK(jobDescription)
