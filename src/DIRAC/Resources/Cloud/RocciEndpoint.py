""" RocciEndpoint class is the implementation of the rocci interface to
    a cloud endpoint via rOCCI-cli
"""
import os
import json
import subprocess
import tempfile

from DIRAC import gLogger, S_OK, S_ERROR
from DIRAC.Core.Utilities.File import makeGuid
from DIRAC.Resources.Cloud.Endpoint import Endpoint


class RocciEndpoint(Endpoint):
    def __init__(self, parameters=None):
        super().__init__(parameters=parameters)
        # logger
        self.log = gLogger.getSubLogger(self.__class__.__name__)
        self.valid = False
        result = self.initialize()
        if result["OK"]:
            self.log.debug("RocciEndpoint created and validated")
            self.valid = True
        else:
            self.log.error(result["Message"])

    def initialize(self):

        availableParams = {
            "EndpointUrl": "endpoint",
            "Timeout": "timeout",
            "Auth": "auth",
            "User": "username",
            "Password": "password",
            "UserCred": "user-cred",
            "VOMS": "voms",
        }

        self.__occiBaseCmd = ["occi", "--skip-ca-check", "--output-format", "json_extended"]
        for var in availableParams:
            if var in self.parameters:
                self.__occiBaseCmd += ["--%s" % availableParams[var], "%s" % self.parameters[var]]

        result = self.__checkConnection()
        return result

    def __filterCommand(self, cmd):
        filteredCmd = []
        mask = False
        for arg in cmd:
            if mask:
                filteredCmd.append("xxxxxx")
                mask = False
            else:
                filteredCmd.append(arg)

            if arg in ["--username", "--password"]:
                mask = True
        return " ".join(filteredCmd)

    def __occiCommand(self, actionArgs):
        try:
            finalCmd = self.__occiBaseCmd + actionArgs
            self.log.debug("Running command:", self.__filterCommand(finalCmd))
            p = subprocess.Popen(finalCmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            stdout, stderr = p.communicate()
            if p.returncode != 0:
                return S_ERROR(f"occi command exit with error {p.returncode}: {stderr}")
        except Exception as e:
            return S_ERROR("Can not run occi command")

        return S_OK(stdout)

    def __checkConnection(self):
        """
        Checks connection status by trying to list the images.

        :return: S_OK | S_ERROR
        """
        actionArgs = ["--action", "list", "--resource", "os_tpl"]
        result = self.__occiCommand(actionArgs)
        if not result["OK"]:
            return result

        return S_OK()

    def __getImageByName(self, imageName):
        """
        Given the imageName, returns the current image object from the server.

        :Parameters:
          **imageName** - `string`

        :return: S_OK( image ) | S_ERROR
        """
        # the libcloud library, throws Exception. Nothing to do.
        actionArgs = ["--action", "describe", "--resource", "os_tpl"]
        result = self.__occiCommand(actionArgs)
        if not result["OK"]:
            return result

        imageIds = []
        for image in json.loads(result["Value"]):
            if image["title"] == imageName:
                imageIds.append(image["term"])

        if not imageIds:
            return S_ERROR("Image %s not found" % imageName)

        if len(imageIds) > 1:
            self.log.warn("More than one image found", f'{len(imageIds)} images with name "{imageName}"')

        return S_OK(imageIds[-1])

    def createInstances(self, vmsToSubmit):
        outputDict = {}

        for nvm in range(vmsToSubmit):
            instanceID = makeGuid()[:8]
            result = self.createInstance(instanceID)
            if result["OK"]:
                occiId, nodeDict = result["Value"]
                self.log.debug(f"Created VM instance {occiId}/{instanceID}")
                outputDict[occiId] = nodeDict
            else:
                self.log.error("Create Rocci instance error:", result["Message"])
                break

        return S_OK(outputDict)

    def createInstance(self, instanceID=""):
        if not instanceID:
            instanceID = makeGuid()[:8]

        self.parameters["VMUUID"] = instanceID
        self.parameters["VMType"] = self.parameters.get("CEType", "EC2")

        actionArgs = ["--action", "create"]
        actionArgs += ["--resource", "compute"]

        # Image
        if "ImageID" in self.parameters and "ImageName" not in self.parameters:
            result = self.__getImageByName(self.parameters["ImageName"])
            if not result["OK"]:
                return result
            imageId = result["Value"]
        elif "ImageID" in self.parameters:
            result = self.__occiCommand(
                ["--action", "describe", "--resource", "os_tpl#%s" % self.parameters["ImageID"]]
            )
            if not result["OK"]:
                return S_ERROR("Failed to get image for ID %s" % self.parameters["ImageID"], result["Message"])
            imageId = self.parameters["ImageID"]
        else:
            return S_ERROR("No image specified")
        actionArgs += ["--mixin", "os_tpl#%s" % imageId]

        # Optional flavor name
        if "FlavorName" in self.parameters:
            result = self.__occiCommand(
                ["--action", "describe", "--resource", "resource_tpl#%s" % self.parameters["FlavorName"]]
            )
            if not result["OK"]:
                return S_ERROR("Failed to get flavor %s" % self.parameters["FlavorName"], result["Message"])
            actionArgs += ["--mixin", "resource_tpl#%s" % self.parameters["FlavorName"]]

        # Instance name
        actionArgs += ["--attribute", "occi.core.title=DIRAC_%s" % instanceID]

        # Other params
        for param in []:
            if param in self.parameters:
                actionArgs += ["--%s" % param, "%s" % self.parameters[param]]

        self.log.info("Creating node:")
        self.log.verbose(" ".join(actionArgs))

        # User data
        result = self._createUserDataScript()
        if not result["OK"]:
            return result
        #    actionArgs += ['--context', 'user_data=%s' % str( result['Value'] )]
        f = tempfile.NamedTemporaryFile(delete=False)
        f.write(str(result["Value"]))
        f.close()
        self.log.debug("Write user_data to temp file:", f.name)
        actionArgs += ["--context", "user_data=file://%s" % f.name]

        # Create the VM instance now
        result = self.__occiCommand(actionArgs)
        os.unlink(f.name)
        if not result["OK"]:
            errmsg = "Error in rOCCI create instances: %s" % result["Message"]
            self.log.error(errmsg)
            return S_ERROR(errmsg)

        occiId = result["Value"].strip()

        # Properties of the instance
        nodeDict = {}
        nodeDict["InstanceID"] = instanceID
        result = self.__occiCommand(["--action", "describe", "--resource", occiId])
        if result["OK"]:
            nodeInfo = json.loads(result["Value"])
            try:
                nodeDict["NumberOfProcessors"] = nodeInfo[0]["attributes"]["occi"]["compute"]["cores"]
                nodeDict["RAM"] = nodeInfo[0]["attributes"]["occi"]["compute"]["memory"]
            except Exception as e:
                nodeDict["NumberOfProcessors"] = 1
        else:
            nodeDict["NumberOfProcessors"] = 1

        return S_OK((occiId, nodeDict))

    def stopVM(self, nodeID, publicIP=""):
        actionArgs = ["--action", "delete", "--resource", nodeID]
        result = self.__occiCommand(actionArgs)
        if not result["OK"]:
            errmsg = "Can not terminate instance {}: {}".format(nodeID, result["Message"])
            self.log.error(errmsg)
            return S_ERROR(errmsg)

        return S_OK()
