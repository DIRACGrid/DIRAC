from diraccfg import CFG
from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Utilities import List
from DIRAC.Core.Utilities.ClassAd.ClassAdLight import ClassAd
from DIRAC.Interfaces.API.Job import Job


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
            if len(val) < 2 or key in ["Arguments", "Executable", "StdOutput", "StdError"]:
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


def loadJDLasJob(job: Job, classAd: ClassAd) -> Job:
    """
    Loads a JDL string as a job
    """

    if classAd.lookupAttribute("Executable"):
        executable = classAd.getAttributeString("Executable")
        if classAd.lookupAttribute("Arguments"):
            job.setExecutable(executable, classAd.getAttributeString("Arguments"))
            classAd.deleteAttribute("Arguments")
        else:
            job.setExecutable(executable)
        classAd.deleteAttribute("Executable")

    if classAd.lookupAttribute("ExecutionEnvironment"):
        environmentDict = {}
        for element in classAd.getListFromExpression("ExecutionEnvironment"):
            key, value = element.replace(" ", "").split("=")
            environmentDict[key] = value
        job.setExecutionEnv(environmentDict)
        classAd.deleteAttribute("ExecutionEnvironment")

    if classAd.lookupAttribute("BannedSites"):
        job.setBannedSites(classAd.getListFromExpression("BannedSites"))
        classAd.deleteAttribute("BannedSites")

    if classAd.lookupAttribute("CPUTime"):
        job.setCPUTime(classAd.getAttributeInt("CPUTime"))
        classAd.deleteAttribute("CPUTime")

    if classAd.lookupAttribute("GridCE"):
        job.setDestinationCE(classAd.getAttributeString("GridCE"))
        classAd.deleteAttribute("GridCE")

    if classAd.lookupAttribute("InputData"):
        job.setInputData(classAd.getListFromExpression("InputData"))
        classAd.deleteAttribute("InputData")

    if classAd.lookupAttribute("InputDataPolicy"):
        job.setInputDataPolicy(classAd.getAttributeString("InputDataPolicy"))
        classAd.deleteAttribute("InputDataPolicy")

    if classAd.lookupAttribute("InputSandbox"):
        job.setInputSandbox(classAd.getListFromExpression("InputSandbox"))
        classAd.deleteAttribute("InputSandbox")

    if classAd.lookupAttribute("JobGroup"):
        job.setJobGroup(classAd.getAttributeString("JobGroup"))
        classAd.deleteAttribute("JobGroup")

    if classAd.lookupAttribute("JobName"):
        job.setName(classAd.getAttributeString("JobName"))
        classAd.deleteAttribute("JobName")

    if classAd.lookupAttribute("LogLevel"):
        job.setLogLevel(classAd.getAttributeString("LogLevel"))
        classAd.deleteAttribute("LogLevel")

    maxNumberOfProcessors = classAd.getAttributeInt("MaxNumberOfProcessors")
    minNumberOfProcessors = classAd.getAttributeInt("MinNumberOfProcessors")
    numberOfProcessors = classAd.getAttributeInt("NumberOfProcessors")
    job.setNumberOfProcessors(numberOfProcessors, minNumberOfProcessors, maxNumberOfProcessors)
    classAd.deleteAttribute("MinNumberOfProcessors")
    classAd.deleteAttribute("MaxNumberOfProcessors")
    classAd.deleteAttribute("NumberOfProcessors")

    if classAd.lookupAttribute("OutputSandbox"):
        job.setOutputSandbox(classAd.getListFromExpression("OutputSandbox"))
        classAd.deleteAttribute("OutputSandbox")

    if classAd.lookupAttribute("OutputData"):
        outputData = classAd.getListFromExpression("OutputData")

        outputSE = None
        if classAd.lookupAttribute("OutputSE"):
            outputSE = classAd.getListFromExpression("OutputSE")
            classAd.deleteAttribute("OutputSE")

        outputPath = None
        if classAd.lookupAttribute("OutputPath"):
            outputPath = classAd.getAttributeString("OutputPath")
            classAd.deleteAttribute("OutputPath")

        job.setOutputData(outputData, outputSE, outputPath)
        classAd.deleteAttribute("OutputData")

    if classAd.lookupAttribute("Parameters"):
        attributes = classAd.getAttributes()
        for attribute in attributes:
            if attribute.startswith("Parameters."):
                job.setParameterSequence(attribute.split(".")[1], classAd.getListFromExpression(attribute))
            classAd.deleteAttribute(attribute)
        classAd.deleteAttribute("Parameters")

    if classAd.lookupAttribute("Platform"):
        job.setPlatform(classAd.getAttributeString("Platform"))
        classAd.deleteAttribute("Platform")

    if classAd.lookupAttribute("Priority"):
        job.setPriority(classAd.getAttributeInt("Priority"))
        classAd.deleteAttribute("Priority")

    if classAd.lookupAttribute("Site"):
        job.setDestination(classAd.getListFromExpression("Site"))
        classAd.deleteAttribute("Site")

    if classAd.lookupAttribute("Tags"):
        job.setTag(list(set(classAd.getListFromExpression("Tags"))))
        classAd.deleteAttribute("Tags")

    # if the job contains errors, return them
    if job.errorDict:
        return S_ERROR(job.errorDict)

    for attribute in classAd.getAttributes():
        job._addParameter(
            job.workflow,
            attribute,
            "JDL",
            classAd.getAttributeInt(attribute),
            "Extra JDL attribute parsed by the loadJDLasJob function",
        )

    # TODO: Replace the two lines above by the following ones to let time for
    # VOs to implement their own parsing of the JDL (should be done in 8.2)
    # attributes = jobDescription.getAttributes()
    # if attributes:
    #     return S_ERROR(f"Some attributes have not been parsed : {', '.join(attributes)}")

    return S_OK(job)
