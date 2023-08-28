"""Transformation classes around the JDL format."""

from diraccfg import CFG
from pydantic import ValidationError

from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Utilities import List
from DIRAC.Core.Utilities.ClassAd.ClassAdLight import ClassAd
from DIRAC.WorkloadManagementSystem.Utilities.JobModel import BaseJobDescriptionModel

ARGUMENTS = "Arguments"
BANNED_SITES = "BannedSites"
CPU_TIME = "CPUTime"
EXECUTABLE = "Executable"
EXECUTION_ENVIRONMENT = "ExecutionEnvironment"
GRID_CE = "GridCE"
INPUT_DATA = "InputData"
INPUT_DATA_POLICY = "InputDataPolicy"
INPUT_SANDBOX = "InputSandbox"
JOB_CONFIG_ARGS = "JobConfigArgs"
JOB_TYPE = "JobType"
JOB_GROUP = "JobGroup"
LOG_LEVEL = "LogLevel"
NUMBER_OF_PROCESSORS = "NumberOfProcessors"
MAX_NUMBER_OF_PROCESSORS = "MaxNumberOfProcessors"
MIN_NUMBER_OF_PROCESSORS = "MinNumberOfProcessors"
OUTPUT_DATA = "OutputData"
OUTPUT_PATH = "OutputPath"
OUTPUT_SE = "OutputSE"
PLATFORM = "Platform"
PRIORITY = "Priority"
STD_ERROR = "StdError"
STD_OUTPUT = "StdOutput"
OUTPUT_SANDBOX = "OutputSandbox"
JOB_NAME = "JobName"
SITE = "Site"
TAGS = "Tags"

OWNER = "Owner"
OWNER_GROUP = "OwnerGroup"
VO = "VirtualOrganization"

CREDENTIALS_FIELDS = {OWNER, OWNER_GROUP, VO}


def loadJDLAsCFG(jdl):
    """
    Load a JDL as CFG
    """

    def cleanValue(value):
        value = value.strip()
        if value[0] == '"':
            entries = []
            iPos = 1
            current = ""
            state = "in"
            while iPos < len(value):
                if value[iPos] == '"':
                    if state == "in":
                        entries.append(current)
                        current = ""
                        state = "out"
                    elif state == "out":
                        current = current.strip()
                        if current not in (",",):
                            return S_ERROR("value seems a list but is not separated in commas")
                        current = ""
                        state = "in"
                else:
                    current += value[iPos]
                iPos += 1
            if state == "in":
                return S_ERROR('value is opened with " but is not closed')
            return S_OK(", ".join(entries))
        else:
            return S_OK(value.replace('"', ""))

    def assignValue(key, value, cfg):
        key = key.strip()
        if len(key) == 0:
            return S_ERROR("Invalid key name")
        value = value.strip()
        if not value:
            return S_ERROR(f"No value for key {key}")
        if value[0] == "{":
            if value[-1] != "}":
                return S_ERROR("Value '%s' seems a list but does not end in '}'" % (value))
            valList = List.fromChar(value[1:-1])
            for i in range(len(valList)):
                result = cleanValue(valList[i])
                if not result["OK"]:
                    return S_ERROR(f"Var {key} : {result['Message']}")
                valList[i] = result["Value"]
                if valList[i] is None:
                    return S_ERROR(f"List value '{value}' seems invalid for item {i}")
            value = ", ".join(valList)
        else:
            result = cleanValue(value)
            if not result["OK"]:
                return S_ERROR(f"Var {key} : {result['Message']}")
            nV = result["Value"]
            if nV is None:
                return S_ERROR(f"Value '{value} seems invalid")
            value = nV
        cfg.setOption(key, value)
        return S_OK()

    if jdl[0] == "[":
        iPos = 1
    else:
        iPos = 0
    key = ""
    value = ""
    action = "key"
    insideLiteral = False
    cfg = CFG()
    while iPos < len(jdl):
        char = jdl[iPos]
        if char == ";" and not insideLiteral:
            if key.strip():
                result = assignValue(key, value, cfg)
                if not result["OK"]:
                    return result
            key = ""
            value = ""
            action = "key"
        elif char == "[" and not insideLiteral:
            key = key.strip()
            if not key:
                return S_ERROR("Invalid key in JDL")
            if value.strip():
                return S_ERROR(f"Key {key} seems to have a value and open a sub JDL at the same time")
            result = loadJDLAsCFG(jdl[iPos:])
            if not result["OK"]:
                return result
            subCfg, subPos = result["Value"]
            cfg.createNewSection(key, contents=subCfg)
            key = ""
            value = ""
            action = "key"
            insideLiteral = False
            iPos += subPos
        elif char == "=" and not insideLiteral:
            if action == "key":
                action = "value"
                insideLiteral = False
            else:
                value += char
        elif char == "]" and not insideLiteral:
            key = key.strip()
            if len(key) > 0:
                result = assignValue(key, value, cfg)
                if not result["OK"]:
                    return result
            return S_OK((cfg, iPos))
        else:
            if action == "key":
                key += char
            else:
                value += char
                if char == '"':
                    insideLiteral = not insideLiteral
        iPos += 1

    return S_OK((cfg, iPos))


def dumpCFGAsJDL(cfg, level=1, tab="  "):
    indent = tab * level
    contents = [f"{tab * (level - 1)}["]
    sections = cfg.listSections()

    for key in cfg:
        if key in sections:
            contents.append(f"{indent}{key} =")
            contents.append(f"{dumpCFGAsJDL(cfg[key], level + 1, tab)};")
        else:
            val = List.fromChar(cfg[key])
            # Some attributes are never lists
            if len(val) < 2 or key in [ARGUMENTS, EXECUTABLE, STD_OUTPUT, STD_ERROR]:
                value = cfg[key]
                try:
                    try_value = float(value)
                    contents.append(f"{tab * level}{key} = {value};")
                except Exception:
                    contents.append(f'{tab * level}{key} = "{value}";')
            else:
                contents.append(f"{indent}{key} =")
                contents.append("%s{" % indent)
                for iPos in range(len(val)):
                    try:
                        value = float(val[iPos])
                    except Exception:
                        val[iPos] = f'"{val[iPos]}"'
                contents.append(",\n".join([f"{tab * (level + 1)}{value}" for value in val]))
                contents.append("%s};" % indent)
    contents.append(f"{tab * (level - 1)}]")
    return "\n".join(contents)


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
