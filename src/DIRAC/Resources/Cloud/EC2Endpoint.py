""" EC2Endpoint class is the implementation of the EC2 interface to
    a cloud endpoint
"""
import os
import json
import boto3

from DIRAC import gLogger, S_OK, S_ERROR
from DIRAC.Core.Utilities.File import makeGuid
from DIRAC.Resources.Cloud.Endpoint import Endpoint


class EC2Endpoint(Endpoint):
    def __init__(self, parameters=None):
        super().__init__(parameters=parameters)
        # logger
        self.log = gLogger.getSubLogger(self.__class__.__name__)
        self.valid = False
        result = self.initialize()
        if result["OK"]:
            self.log.debug("EC2Endpoint created and validated")
            self.valid = True
        else:
            self.log.error(result["Message"])

    def initialize(self):

        availableParams = {
            "RegionName": "region_name",
            "AccessKey": "aws_access_key_id",
            "SecretKey": "aws_secret_access_key",
            "EndpointUrl": "endpoint_url",  # EndpointUrl is optional
        }

        connDict = {}
        for var in availableParams:
            if var in self.parameters:
                connDict[availableParams[var]] = self.parameters[var]

        try:
            self.__ec2 = boto3.resource("ec2", **connDict)
        except Exception as e:
            self.log.exception("Failed to connect to EC2")
            errorStatus = "Can't connect to EC2: " + str(e)
            return S_ERROR(errorStatus)

        result = self.__loadInstanceType()
        if not result["OK"]:
            return result

        result = self.__checkConnection()
        return result

    def __loadInstanceType(self):
        currentDir = os.path.dirname(__file__)
        instanceTypeFile = os.path.join(currentDir, "ec2_instance_type.json")
        try:
            with open(instanceTypeFile) as f:
                self.__instanceTypeInfo = json.load(f)
        except Exception as e:
            self.log.exception("Failed to fetch EC2 instance details")
            errmsg = "Exception loading EC2 instance type info: %s" % e
            self.log.error(errmsg)
            return S_ERROR(errmsg)

        return S_OK()

    def __checkConnection(self):
        """
        Checks connection status by trying to list the images.

        :return: S_OK | S_ERROR
        """
        try:
            self.__ec2.images.filter(Owners=["self"])
        except Exception as e:
            self.log.exception("Failed to list EC2 images")
            return S_ERROR(e)

        return S_OK()

    def createInstances(self, vmsToSubmit):
        outputDict = {}

        for nvm in range(vmsToSubmit):
            instanceID = makeGuid()[:8]
            result = self.createInstance(instanceID)
            if result["OK"]:
                ec2Id, nodeDict = result["Value"]
                self.log.debug(f"Created VM instance {ec2Id}/{instanceID}")
                outputDict[ec2Id] = nodeDict
            else:
                self.log.error("Create EC2 instance error:", result["Message"])
                break

        return S_OK(outputDict)

    def createInstance(self, instanceID=""):
        if not instanceID:
            instanceID = makeGuid()[:8]

        self.parameters["VMUUID"] = instanceID
        self.parameters["VMType"] = self.parameters.get("CEType", "EC2")

        createNodeDict = {}

        # Image
        if "ImageID" in self.parameters and "ImageName" not in self.parameters:
            try:
                images = self.__ec2.images.filter(Filters=[{"Name": "name", "Values": [self.parameters["ImageName"]]}])
                imageId = None
                for image in images:
                    imageId = image.id
                    break
            except Exception as e:
                self.log.exception("Exception when get ID from image name %s:" % self.parameters["ImageName"])
                return S_ERROR("Failed to get image for Name %s" % self.parameters["ImageName"])
            if imageId is None:
                return S_ERROR("Image name %s not found" % self.parameters["ImageName"])
        elif "ImageID" in self.parameters:
            try:
                self.__ec2.images.filter(ImageIds=[self.parameters["ImageID"]])
            except Exception as e:
                self.log.exception("Failed to get EC2 image list")
                return S_ERROR("Failed to get image for ID %s" % self.parameters["ImageID"])
            imageId = self.parameters["ImageID"]
        else:
            return S_ERROR("No image specified")
        createNodeDict["ImageId"] = imageId

        # Instance type
        if "FlavorName" not in self.parameters:
            return S_ERROR("No flavor specified")
        instanceType = self.parameters["FlavorName"]
        createNodeDict["InstanceType"] = instanceType

        # User data
        result = self._createUserDataScript()
        if not result["OK"]:
            return result
        createNodeDict["UserData"] = str(result["Value"])

        # Other params
        for param in ["KeyName", "SubnetId", "EbsOptimized"]:
            if param in self.parameters:
                createNodeDict[param] = self.parameters[param]

        self.log.info("Creating node:")
        for key, value in createNodeDict.items():
            self.log.verbose(f"{key}: {value}")

        # Create the VM instance now
        try:
            instances = self.__ec2.create_instances(MinCount=1, MaxCount=1, **createNodeDict)
        except Exception as e:
            self.log.exception("Failed to create EC2 instance")
            return S_ERROR("Exception in ec2 create_instances: %s" % e)

        if len(instances) < 1:
            errmsg = "ec2 create_instances failed to create any VM"
            self.log.error(errmsg)
            return S_ERROR(errmsg)

        # Create the name in tags
        ec2Id = instances[0].id
        tags = [{"Key": "Name", "Value": "DIRAC_%s" % instanceID}]
        try:
            self.__ec2.create_tags(Resources=[ec2Id], Tags=tags)
        except Exception as e:
            self.log.exception("Failed to tag EC2 instance")
            return S_ERROR(f"Exception setup name for {ec2Id}: {e}")

        # Properties of the instance
        nodeDict = {}
        #    nodeDict['PublicIP'] = publicIP
        nodeDict["InstanceID"] = instanceID
        if instanceType in self.__instanceTypeInfo:
            nodeDict["NumberOfProcessors"] = self.__instanceTypeInfo[instanceType]["vCPU"]
            nodeDict["RAM"] = self.__instanceTypeInfo[instanceType]["Memory"]
        else:
            nodeDict["NumberOfProcessors"] = 1

        return S_OK((ec2Id, nodeDict))

    def stopVM(self, nodeID, publicIP=""):
        """
        Given the node ID it gets the node details, which are used to destroy the
        node making use of the libcloud.openstack driver. If three is any public IP
        ( floating IP ) assigned, frees it as well.

        :Parameters:
          **uniqueId** - `string`
            openstack node id ( not uuid ! )
          **public_ip** - `string`
            public IP assigned to the node if any

        :return: S_OK | S_ERROR
        """
        try:
            self.__ec2.Instance(nodeID).terminate()
        except Exception as e:
            self.log.exception("Failed to terminate EC2 instance")
            return S_ERROR(f"Exception terminate instance {nodeID}: {e}")

        return S_OK()
