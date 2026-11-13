""" Class defining a production step """
import json

from DIRAC import S_OK, S_ERROR


class ProductionStep:

    """Define the Production Step object"""

    _STEP_ATTRIBUTES = {
        "Name",
        "Description",
        "LongDescription",
        "Type",
        "Plugin",
        "AgentType",
        "FileMask",
        "ParentStep",
        "Inputquery",
        "Outputquery",
        "GroupSize",
        "Body",
    }

    _TRANSFORMATION_ONLY_PARAMETER_NAMES = frozenset(
        {
            "TransformationID",
            "TransformationName",
            "Status",
            "TransformationGroup",
            "InheritedFrom",
            "MaxNumberOfTasks",
            "EventsPerTask",
        }
    )

    def __init__(self, **kwargs):
        """Simple constructor"""
        object.__setattr__(self, "customParameters", {})
        # Default values for transformation step parameters
        self.Name = ""
        self.Description = "description"
        self.LongDescription = "longDescription"
        self.Type = "MCSimulation"
        self.Plugin = "Standard"
        self.AgentType = "Manual"
        self.FileMask = ""
        #########################################
        self.ParentStep = None
        self.Inputquery = None
        self.Outputquery = None
        self.GroupSize = 1
        self.Body = "body"

    def __getattr__(self, name):
        try:
            return self.customParameters[name]
        except KeyError as exc:
            raise AttributeError(name) from exc

    def __setattr__(self, name, value):
        if name.startswith("_") or name in self._STEP_ATTRIBUTES or name in self.__dict__ or hasattr(type(self), name):
            object.__setattr__(self, name, value)
        elif name in self._TRANSFORMATION_ONLY_PARAMETER_NAMES:
            raise AttributeError(f"{name} is a transformation parameter, not a custom production-step parameter")
        else:
            self.customParameters[name] = value

    def getAsDict(self):
        """It returns the Step description as a dictionary"""
        prodStepDict = {}
        prodStepDict["name"] = self.Name
        prodStepDict["parentStep"] = []
        # check the ParentStep format
        if self.ParentStep:
            if isinstance(self.ParentStep, list):
                prodStepDict["parentStep"] = []
                for parentStep in self.ParentStep:  # pylint: disable=not-an-iterable
                    if not parentStep.Name:
                        return S_ERROR("Parent Step does not exist")
                    prodStepDict["parentStep"].append(parentStep.Name)
            elif isinstance(self.ParentStep, ProductionStep):
                if not self.ParentStep.Name:
                    return S_ERROR("Parent Step does not exist")
                prodStepDict["parentStep"] = [self.ParentStep.Name]
            else:
                return S_ERROR("Invalid Parent Step")

        prodStepDict["description"] = self.Description
        prodStepDict["longDescription"] = self.LongDescription
        prodStepDict["stepType"] = self.Type
        prodStepDict["plugin"] = self.Plugin
        prodStepDict["agentType"] = self.AgentType
        prodStepDict["fileMask"] = self.FileMask
        # Optional fields
        prodStepDict["inputquery"] = json.dumps(self.Inputquery)
        prodStepDict["outputquery"] = json.dumps(self.Outputquery)
        prodStepDict["groupsize"] = self.GroupSize
        prodStepDict["body"] = json.dumps(self.Body)

        return S_OK(prodStepDict)
