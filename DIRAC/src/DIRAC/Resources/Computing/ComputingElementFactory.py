"""  The Computing Element Factory has one method that instantiates a given Computing Element
     from the CEUnique ID specified in the JobAgent configuration section.
"""
from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.Resources.Computing.ComputingElement import getCEConfigDict
from DIRAC.Core.Utilities.ObjectLoader import ObjectLoader


class ComputingElementFactory:
    #############################################################################
    def __init__(self, ceType=""):
        """Standard constructor"""
        self.ceType = ceType
        self.log = gLogger.getSubLogger(self.__class__.__name__)

    #############################################################################
    def getCE(self, ceType="", ceName="", ceParametersDict={}):
        """This method returns the CE instance corresponding to the supplied
        CEUniqueID.  If no corresponding CE is available, this is indicated.
        """
        if ceType:
            self.log.verbose(f"Creating CE of type {ceType}")
        if ceName:
            self.log.verbose(f"Creating CE for name {ceName}")
        ceTypeLocal = ceType if ceType else self.ceType
        ceNameLocal = ceName if ceName else ceType
        ceConfigDict = getCEConfigDict(f"/LocalSite/{ceNameLocal}")
        self.log.verbose("CEConfigDict", ceConfigDict)
        if "CEType" in ceConfigDict:
            ceTypeLocal = ceConfigDict["CEType"]
        if not ceTypeLocal:
            error = "Can not determine CE Type"
            self.log.error(error)
            return S_ERROR(error)
        subClassName = f"{ceTypeLocal}ComputingElement"

        result = ObjectLoader().loadObject(f"Resources.Computing.{subClassName}")
        if not result["OK"]:
            self.log.error("Failed to load object", f"{subClassName}: {result['Message']}")
            return result

        ceClass = result["Value"]
        try:
            computingElement = ceClass(ceNameLocal)
            # Always set the CEType parameter according to instantiated class
            ceDict = {"CEType": ceTypeLocal}
            if ceParametersDict:
                ceDict.update(ceParametersDict)
            result = computingElement.setParameters(ceDict)
            if not result["OK"]:
                return result

        except Exception as x:
            msg = f"ComputingElementFactory could not instantiate {subClassName} object"
            self.log.exception()
            self.log.warn(msg, repr(x))
            return S_ERROR(repr(x))

        return S_OK(computingElement)
