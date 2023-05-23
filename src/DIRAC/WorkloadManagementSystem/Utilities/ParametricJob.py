""" Utilities to process parametric job definitions and generate
    bunches of parametric jobs. It exposes the following functions:

    getParameterVectorLength() - to get the total size of the bunch of parametric jobs
    generateParametricJobs() - to get a list of expanded descriptions of all the jobs
"""
import re
from DIRAC.Core.Utilities.ClassAd.ClassAdLight import ClassAd
from DIRAC.Core.Utilities.ReturnValues import S_OK, S_ERROR
from DIRAC.Core.Utilities.DErrno import EWMSJDL


def __getParameterSequence(nPar: int, parStart: float, parStep: float, parFactor: float):
    """
    Get the parameter sequence from parStart, parStep and parFactor
    """

    # The first parameter must have the same type as the other ones even if not defined explicitly
    parameterList = [parStart * type(parFactor)(1) + type(parStep)(0)]
    for np in range(1, nPar):
        parameterList.append(parameterList[np - 1] * parFactor + parStep)

    return parameterList


def __updateAttribute(classAd, attribute, parName, parValue):
    # If there is something to do:
    pattern = r"%%\(%s\)s" % parName
    expr = classAd.get_expression(attribute)
    if not re.search(pattern, expr):
        return False

    parValue = parValue.strip()
    if classAd.isAttributeList(attribute):
        parValue = parValue.strip()
        if parValue.startswith("{"):
            parValue = parValue.lstrip("{").rstrip("}").strip()

    expr = classAd.get_expression(attribute)
    newexpr = expr.replace(f"%({parName})s", str(parValue))
    classAd.set_expression(attribute, newexpr)
    return True


def generateParametricJobs(jobClassAd: ClassAd):
    """Generate a series of ClassAd job descriptions expanding
        job parameters

    :param jobClassAd: ClassAd job description object
    :return: list of ClassAd job description objects
    """
    if not jobClassAd.lookupAttribute("Parameters"):
        return S_OK([jobClassAd.asJDL()])

    jobClassAd = transformParametricJobIntoParsableOne(jobClassAd)

    result = checkIfParametricJobIsCorrect(jobClassAd)
    if not result["OK"]:
        return result

    nParValues = jobClassAd.getAttributeInt("Parameters")
    jobClassAd.deleteAttribute("Parameters")

    parameterLists = {}
    for attribute in jobClassAd.getAttributes():
        if attribute.startswith("Parameters."):
            seqID = attribute.split(".")[1]
            parameterLists[seqID] = jobClassAd.getListFromExpression(attribute)

    jobDescList = []
    # Width of the sequential parameter number
    zLength = len(str(nParValues - 1))
    for n in range(nParValues):
        newJobDesc = jobClassAd.asJDL().replace("%n", str(n).zfill(zLength))
        newClassAd = ClassAd(newJobDesc)
        for seqID in parameterLists:
            parameter = parameterLists[seqID][n]
            for attribute in newClassAd.getAttributes():
                __updateAttribute(newClassAd, attribute, seqID, str(parameter))

        for seqID in parameterLists:
            parameter = parameterLists[seqID][n]
            attribute = f"Parameter.{seqID}"

            if isinstance(parameter, str) and parameter.startswith("{"):
                newClassAd.insertAttributeInt(attribute, str(parameter))
            else:
                newClassAd.insertAttributeString(attribute, str(parameter))

        newClassAd.insertAttributeInt("ParameterNumber", n)
        newJDL = newClassAd.asJDL()
        jobDescList.append(newJDL)

    return S_OK(jobDescList)


def transformParametricJobIntoParsableOne(jobDescription: ClassAd):
    """
    Transform a parametric job into a parsable one by the Job API
    """
    jobDescription = putDefaultNameOnNamelessParameterSequence(jobDescription)

    if jobDescription.lookupAttribute("Parameters"):
        numberOfParameters = jobDescription.getAttributeInt("Parameters")

        parameterDict = {}
        attributes = jobDescription.getAttributes()
        for attribute in attributes:
            for key in ["ParameterStart", "ParameterStep", "ParameterFactor"]:
                if attribute.startswith(key):
                    seqID = attribute.split(".")[1]
                    parameterDict.setdefault(seqID, {})

                    value = jobDescription.getAttributeInt(attribute)
                    if value is None:
                        value = jobDescription.getAttributeFloat(attribute)
                        if value is None:
                            value = jobDescription.get_expression(attribute)
                            return S_ERROR(f"Illegal value for {attribute} JDL field: {value}")
                    parameterDict[seqID][key] = value
                    jobDescription.deleteAttribute(attribute)

        for seqID in parameterDict:
            parameterSequence = __getParameterSequence(
                numberOfParameters,
                parStart=parameterDict[seqID].get("ParameterStart", 1),
                parStep=parameterDict[seqID].get("ParameterStep", 0),
                parFactor=parameterDict[seqID].get("ParameterFactor", 1),
            )
            jobDescription.insertAttributeVectorInt(f"Parameters.{seqID}", parameterSequence)

    return jobDescription


def putDefaultNameOnNamelessParameterSequence(jobClassAd: ClassAd):
    """
    Put a default name on nameless parameters, and make the field Parameters always an int
    """

    if jobClassAd.lookupAttribute("Parameters"):
        jobHasBeenModified = False

        if jobClassAd.isAttributeList("Parameters"):
            parameters = jobClassAd.getListFromExpression("Parameters")
            jobClassAd.insertAttributeInt("Parameters", len(parameters))
            jobClassAd.insertAttributeVectorString("Parameters.A", parameters)
            jobHasBeenModified = True
        else:
            attributes = jobClassAd.getAttributes()
            for attribute in attributes:
                if attribute in ("ParameterStart", "ParameterStep", "ParameterFactor"):
                    jobClassAd.insertAttributeInt(f"{attribute}.A", jobClassAd.getAttributeInt(attribute))
                    jobClassAd.deleteAttribute(attribute)
                    jobHasBeenModified = True

        if jobHasBeenModified:
            jobClassAd = ClassAd(jobClassAd.asJDL().replace("%s", "%(A)s"))

    return jobClassAd


def checkIfParametricJobIsCorrect(jobClassAd: ClassAd):
    """
    Check if a parametric job is correct

    :return: S_OK / S_ERROR
    """

    if jobClassAd.lookupAttribute("Parameters"):
        nParValues = jobClassAd.getAttributeInt("Parameters")
        if not nParValues or nParValues <= 0:
            return S_ERROR(EWMSJDL, "Illegal number of job parameters", nParValues)

        attributes = jobClassAd.getAttributes()
        for attribute in attributes:
            if attribute.startswith("Parameters."):
                if not jobClassAd.isAttributeList(attribute):
                    return S_ERROR(
                        EWMSJDL,
                        f"Attribute {attribute} must be a list",
                    )

                if len(jobClassAd.getListFromExpression(attribute)) != nParValues:
                    return S_ERROR(
                        EWMSJDL,
                        f"Different length of parameter vectors for {attribute}",
                    )

    return S_OK()
