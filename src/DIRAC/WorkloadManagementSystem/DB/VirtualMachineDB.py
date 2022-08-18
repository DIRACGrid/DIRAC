""" VirtualMachineDB class is a front-end to the virtual machines DB

  Life cycle of VMs Images in DB
    * New:       Inserted by Director (Name - Status = New ) if not existing when launching a new instance
    * Validated: Declared by VMMonitoring Server when an Instance reports back correctly
    * Error:     Declared by VMMonitoring Server when an Instance reports back wrong requirements

  Life cycle of VMs Instances in DB
    * New:       Inserted by Director before launching a new instance, to check if image is valid
    * Submitted: Inserted by Director (adding UniqueID) when launches a new instance
    * Wait_ssh_context: Declared by Director for submitted instance wich need later contextualization using ssh
                        (VirtualMachineContextualization will check)
    * Contextualizing:     on the waith_ssh_context path is the next status before Running
    * Running:   Declared by VMMonitoring Server when an Instance reports back correctly
                   (add LastUpdate, publicIP and privateIP)
    * Stopping:  Declared by VMManager Server when an Instance has been deleted outside of the VM
                   (f.e "Delete" button on Browse Instances)
    * Halted:    Declared by VMMonitoring Server when an Instance reports halting
    * Stalled:   Declared by VMManager Server when detects Instance no more running
    * Error:     Declared by VMMonitoring Server when an Instance reports back wrong requirements
                   or reports as running when Halted

  New Instances can be launched by Director if VMImage is not in Error Status.

  Instance UniqueID: for KVM it could be the MAC, for Amazon the returned InstanceID(i-5dec3236),
  or Occi returned the VMID

"""
import datetime

from DIRAC import S_ERROR, S_OK
from DIRAC.Core.Base.DB import DB


class VirtualMachineDB(DB):

    # When checking the Status on the DB it must be one of these values, if not, the last one (Error) is set
    # When declaring a new Status, it will be set to Error if not in the list
    validImageStates = ["New", "Validated", "Error"]
    validInstanceStates = ["New", "Submitted", "Running", "Stopping", "Halted", "Stalled", "Error"]

    # In seconds !
    stallingInterval = 60 * 40

    # When attempting a transition it will be checked if the current state is allowed
    allowedTransitions = {
        "Image": {
            "Validated": ["New", "Validated"],
        },
        "Instance": {
            "Submitted": ["New"],
            "Running": ["Submitted", "Running", "Stalled", "New"],
            "Stopping": ["Running", "Stalled"],
            "Halted": ["New", "Running", "Stopping", "Stalled", "Halted"],
            "Stalled": ["New", "Submitted", "Running"],
        },
    }

    tablesDesc = {}

    tablesDesc["vm_Images"] = {
        "Fields": {
            "VMImageID": "BIGINT UNSIGNED AUTO_INCREMENT NOT NULL",
            "Name": "VARCHAR(255) NOT NULL",
            "Status": "VARCHAR(16) NOT NULL",
            "LastUpdate": "DATETIME",
            "ErrorMessage": 'VARCHAR(255) NOT NULL DEFAULT ""',
        },
        "PrimaryKey": "VMImageID",
    }

    tablesDesc["vm_Instances"] = {
        "Fields": {
            "InstanceID": "BIGINT UNSIGNED AUTO_INCREMENT NOT NULL",
            "RunningPod": "VARCHAR(255) NOT NULL",
            "Name": "VARCHAR(255) NOT NULL",
            "Endpoint": "VARCHAR(255) NOT NULL",
            "UniqueID": 'VARCHAR(255) NOT NULL DEFAULT ""',
            "VMImageID": "INTEGER UNSIGNED NOT NULL",
            "Status": "VARCHAR(32) NOT NULL",
            "LastUpdate": "DATETIME",
            "PublicIP": 'VARCHAR(32) NOT NULL DEFAULT ""',
            "PrivateIP": 'VARCHAR(32) NOT NULL DEFAULT ""',
            "ErrorMessage": 'VARCHAR(255) NOT NULL DEFAULT ""',
            "MaxAllowedPrice": "FLOAT DEFAULT NULL",
            "Uptime": "INTEGER UNSIGNED DEFAULT 0",
            "Load": "FLOAT DEFAULT 0",
            "Jobs": "INTEGER UNSIGNED NOT NULL DEFAULT 0",
        },
        "PrimaryKey": "InstanceID",
        "Indexes": {"Status": ["Status"]},
    }

    tablesDesc["vm_History"] = {
        "Fields": {
            "InstanceID": "INTEGER UNSIGNED NOT NULL",
            "Status": "VARCHAR(32) NOT NULL",
            "Load": "FLOAT NOT NULL",
            "Jobs": "INTEGER UNSIGNED NOT NULL DEFAULT 0",
            "TransferredFiles": "INTEGER UNSIGNED NOT NULL DEFAULT 0",
            "TransferredBytes": "BIGINT UNSIGNED NOT NULL DEFAULT 0",
            "Update": "DATETIME",
        },
        "Indexes": {"InstanceID": ["InstanceID"]},
    }

    #######################
    # VirtualDB constructor
    #######################

    def __init__(self, maxQueueSize=10, parentLogger=None):
        super().__init__("VirtualMachineDB", "WorkloadManagement/VirtualMachineDB", parentLogger=parentLogger)
        result = self.__initializeDB()
        if not result["OK"]:
            raise Exception("Can't create tables: %s" % result["Message"])

    #######################
    # Public Functions
    #######################

    def checkImageStatus(self, imageName):
        """
        Check Status of a given image
        Will insert a new Image in the DB if it does not exits
        returns:
        S_OK(Status) if Status is valid and not Error
        S_ERROR(ErrorMessage) otherwise
        """
        ret = self.__getImageID(imageName)
        if not ret["OK"]:
            return ret
        return self.__getStatus("Image", ret["Value"])

    def insertInstance(self, uniqueID, imageName, instanceName, endpoint, runningPodName):
        """
        Check Status of a given image
        Will insert a new Instance in the DB
        returns:
        S_OK( InstanceID ) if new Instance is properly inserted
        S_ERROR(ErrorMessage) otherwise
        """
        imageStatus = self.checkImageStatus(imageName)
        if not imageStatus["OK"]:
            return imageStatus

        return self.__insertInstance(uniqueID, imageName, instanceName, endpoint, runningPodName)

    def setInstanceUniqueID(self, instanceID, uniqueID):
        """
        Assign a uniqueID to an instance
        """
        result = self.getInstanceID(uniqueID)
        if result["OK"]:
            return S_ERROR("UniqueID is not unique: %s" % uniqueID)

        result = self._escapeString(uniqueID)
        if not result["OK"]:
            return result
        uniqueID = result["Value"]

        try:
            instanceID = int(instanceID)
        except ValueError:
            return S_ERROR("instanceID has to be a number")

        tableName, _validStates, idName = self.__getTypeTuple("Instance")

        sqlUpdate = "UPDATE `%s` SET UniqueID = %s WHERE %s = %d" % (tableName, uniqueID, idName, instanceID)
        return self._update(sqlUpdate)

    def getInstanceParameter(self, pName, instanceID):
        """Get the instance parameter pName for the given instanceID

        :param str pName: parameter name
        :param str instanceID: instance unique identifier
        :return: S_OK/S_ERROR, parameter value
        """

        tableName, _validStates, idName = self.__getTypeTuple("Instance")

        if pName not in VirtualMachineDB.tablesDesc["vm_Instances"]["Fields"]:
            return S_ERROR("Invalid Instance parameter %s" % pName)

        sqlQuery = f"SELECT `{pName}` FROM `{tableName}` WHERE {idName} = {instanceID}"
        result = self._query(sqlQuery)

        if not result["OK"]:
            return result
        value = result["Value"][0][0]

        return S_OK(value)

    def getUniqueID(self, instanceID):
        """
        For a given dirac instanceID get the corresponding cloud endpoint uniqueID
        """
        return self.getInstanceParameter("UniqueID", instanceID)

    def getUniqueIDByName(self, instanceName):
        """Get the cloud provider unique ID corresponding to the DIRAC unique name

        :param str instanceName: VM name
        :return: S_OK/S_ERROR, cloud unique ID as value
        """
        tableName, _validStates, idName = self.__getTypeTuple("Instance")

        sqlQuery = f"SELECT UniqueID FROM `{tableName}` WHERE Name = '{instanceName}'"
        result = self._query(sqlQuery)
        if not result["OK"]:
            return result
        if not result["Value"]:
            return S_ERROR("Instance not found")
        uniqueID = result["Value"][0][0]

        return S_OK(uniqueID)

    def getInstanceID(self, uniqueID):
        """
        For a given uniqueID of an instance return associated internal InstanceID
        """
        tableName, _validStates, idName = self.__getTypeTuple("Instance")

        result = self.getFields(tableName, [idName], {"UniqueID": uniqueID})
        if not result["OK"]:
            return result
        if not result["Value"]:
            return S_ERROR("Unknown {} = {}".format("UniqueID", uniqueID))
        return S_OK(result["Value"][0][0])

    def declareInstanceSubmitted(self, uniqueID):
        """
        After submission of the instance the Director should declare the submitted Status
        """
        instanceID = self.getInstanceID(uniqueID)
        if not instanceID["OK"]:
            return instanceID
        instanceID = instanceID["Value"]

        status = self.__setState("Instance", instanceID, "Submitted")
        if status["OK"]:
            self.__addInstanceHistory(instanceID, "Submitted")

        return status

    def declareInstanceRunning(self, uniqueID, publicIP, privateIP=""):
        """
        Declares an instance Running and sets its associated info (uniqueID, publicIP, privateIP)
        Returns S_ERROR if:
        - instanceName does not have a "Submitted" or "Contextualizing" entry
        - uniqueID is not unique
        """
        instanceID = self.getInstanceID(uniqueID)
        if not instanceID["OK"]:
            return instanceID
        instanceID = instanceID["Value"]

        # No IPv6 prefix
        publicIP = publicIP.replace("::ffff:", "")

        self.__setInstanceIPs(instanceID, publicIP, privateIP)

        status = self.__setState("Instance", instanceID, "Running")
        if status["OK"]:
            self.__addInstanceHistory(instanceID, "Running")

        return self.getAllInfoForUniqueID(uniqueID)

    def declareInstanceStopping(self, instanceID):
        """
        Mark a VM instance to be stopped.

        Next time the instance's VirtualMachineMonitor checks in for an update
        it will be told to halt.

        :return: S_OK if instance updated, S_ERROR otherwise.
        """
        status = self.__setState("Instance", instanceID, "Stopping")
        if status["OK"]:
            self.__addInstanceHistory(instanceID, "Stopping")
        return status

    def getInstanceStatus(self, instanceID):
        """
        By dirac instanceID
        """
        tableName, validStates, idName = self.__getTypeTuple("Instance")
        if not tableName:
            return S_ERROR("Unknown DB object Instance")

        ret = self.__getStatus("Instance", instanceID)
        if not ret["OK"]:
            return ret

        if not ret["Value"]:
            return S_ERROR("Unknown InstanceID = %s" % (instanceID))

        status = ret["Value"]
        if status not in validStates:
            return self.__setError("Instances", instanceID, "Invalid Status: %s" % status)

        return S_OK(status)

    def recordDBHalt(self, instanceID, load):
        """
        Insert the heart beat info from a halting instance
        Declares "Halted" the instance and the image
        It returns S_ERROR if the status is not OK
        """
        status = self.__setState("Instance", instanceID, "Halted")
        if status["OK"]:
            self.__addInstanceHistory(instanceID, "Halted", load)

        return status

    def declareInstanceHalting(self, uniqueID, load):
        """
        Insert the heart beat info from a halting instance
        Declares "Halted" the instance and the image
        It returns S_ERROR if the status is not OK
        """
        instanceID = self.getInstanceID(uniqueID)
        if not instanceID["OK"]:
            return instanceID
        instanceID = instanceID["Value"]

        status = self.__setState("Instance", instanceID, "Halted")
        if status["OK"]:
            self.__addInstanceHistory(instanceID, "Halted", load)

        return status

    def declareStalledInstances(self):
        """
        Check last Heart Beat for all Running instances and declare them Stalled if older than interval
        """
        oldInstances = self.__getOldInstanceIDs(self.stallingInterval, self.allowedTransitions["Instance"]["Stalled"])
        if not oldInstances["OK"]:
            return oldInstances

        stallingInstances = []

        if not oldInstances["Value"]:
            return S_OK(stallingInstances)

        for instanceID in oldInstances["Value"]:
            instanceID = instanceID[0]
            stalled = self.__setState("Instance", instanceID, "Stalled")
            if not stalled["OK"]:
                continue

            self.__addInstanceHistory(instanceID, "Stalled")
            stallingInstances.append(instanceID)

        return S_OK(stallingInstances)

    def instanceIDHeartBeat(self, uniqueID, load, jobs, transferredFiles, transferredBytes, uptime):
        """
        Insert the heart beat info from a running instance
        It checks the status of the instance and the corresponding image
        Declares "Running" the instance and the image
        It returns S_ERROR if the status is not OK
        """
        instanceID = self.getInstanceID(uniqueID)
        if not instanceID["OK"]:
            return instanceID
        instanceID = instanceID["Value"]

        result = self.__runningInstance(instanceID, load, jobs, transferredFiles, transferredBytes)
        if not result["OK"]:
            return result

        self.__setLastLoadJobsAndUptime(instanceID, load, jobs, uptime)

        status = self.__getStatus("Instance", instanceID)
        if not status["OK"]:
            return result
        status = status["Value"]

        if status == "Stopping":
            return S_OK("stop")
        return S_OK()

    def getEndpointFromInstance(self, uniqueId):
        """
        For a given instance uniqueId it returns the asociated Endpoint in the instance
        table, thus the ImageName of such instance
        """
        tableName, _validStates, _idName = self.__getTypeTuple("Instance")

        endpoint = self.getFields(tableName, ["Endpoint"], {"UniqueID": uniqueId})
        if not endpoint["OK"]:
            return endpoint
        endpoint = endpoint["Value"]

        if not endpoint:
            return S_ERROR("Unknown {} = {}".format("UniqueID", uniqueId))

        return S_OK(endpoint[0][0])

    def getInstancesByStatus(self, status):
        """
        Get dictionary of Image Names with InstanceIDs in given status
        """
        if status not in self.validInstanceStates:
            return S_ERROR("Status %s is not known" % status)

        # InstanceTuple
        tableName, _validStates, _idName = self.__getTypeTuple("Instance")

        runningInstances = self.getFields(tableName, ["VMImageID", "UniqueID"], {"Status": status})
        if not runningInstances["OK"]:
            return runningInstances
        runningInstances = runningInstances["Value"]

        instancesDict = {}
        imagesDict = {}

        # ImageTuple
        tableName, _validStates, idName = self.__getTypeTuple("Image")

        for imageID, uniqueID in runningInstances:

            if imageID not in imagesDict:

                imageName = self.getFields(tableName, ["Name"], {idName: imageID})
                if not imageName["OK"]:
                    continue
                imagesDict[imageID] = imageName["Value"][0][0]

            if not imagesDict[imageID] in instancesDict:
                instancesDict[imagesDict[imageID]] = []
            instancesDict[imagesDict[imageID]].append(uniqueID)

        return S_OK(instancesDict)

    def getAllInfoForUniqueID(self, uniqueID):
        """
        Get all fields for a uniqueID
        """
        instanceID = self.getInstanceID(uniqueID)
        if not instanceID["OK"]:
            return instanceID
        instanceID = instanceID["Value"]

        instData = self.__getInfo("Instance", instanceID)
        if not instData["OK"]:
            return instData
        instData = instData["Value"]

        imgData = self.__getInfo("Image", instData["VMImageID"])
        if not imgData["OK"]:
            return imgData
        imgData = imgData["Value"]

        return S_OK({"Image": imgData, "Instance": instData})

    #############################
    # Monitoring Public Functions
    #############################

    def getInstancesContent(self, selDict, sortList, start=0, limit=0):
        """
        Function to get the contents of the db
          parameters are a filter to the db
        """
        # Main fields
        tables = ("`vm_Images` AS img", "`vm_Instances` AS inst")
        imageFields = ("VMImageID", "Name")
        instanceFields = (
            "RunningPod",
            "InstanceID",
            "Endpoint",
            "Name",
            "UniqueID",
            "VMImageID",
            "Status",
            "PublicIP",
            "Status",
            "ErrorMessage",
            "LastUpdate",
            "Load",
            "Uptime",
            "Jobs",
        )

        fields = ["img.%s" % f for f in imageFields] + ["inst.%s" % f for f in instanceFields]
        sqlQuery = "SELECT {} FROM {}".format(", ".join(fields), ", ".join(tables))
        sqlCond = ["img.VMImageID = inst.VMImageID"]
        for field in selDict:
            if field in instanceFields:
                sqlField = "inst.%s" % field
            elif field in imageFields:
                sqlField = "img.%s" % field
            elif field in fields:
                sqlField = field
            else:
                continue
            value = selDict[field]
            if isinstance(value, str):
                value = [str(value)]
            sqlCond.append(
                " OR ".join(
                    ["{}={}".format(sqlField, self._escapeString(str(value))["Value"]) for value in selDict[field]]
                )
            )
        sqlQuery += " WHERE %s" % " AND ".join(sqlCond)
        if sortList:
            sqlSortList = []
            for sorting in sortList:
                if sorting[0] in instanceFields:
                    sqlField = "inst.%s" % sorting[0]
                elif sorting[0] in imageFields:
                    sqlField = "img.%s" % sorting[0]
                elif sorting[0] in fields:
                    sqlField = sorting[0]
                else:
                    continue
                direction = sorting[1].upper()
                if direction not in ("ASC", "DESC"):
                    continue
                sqlSortList.append(f"{sqlField} {direction}")
            if sqlSortList:
                sqlQuery += " ORDER BY %s" % ", ".join(sqlSortList)
        if limit:
            sqlQuery += " LIMIT %d,%d" % (start, limit)
        retVal = self._query(sqlQuery)
        if not retVal["OK"]:
            return retVal
        data = []
        # Total records
        for record in retVal["Value"]:
            record = list(record)
            data.append(record)
        totalRecords = len(data)
        sqlQuery = "SELECT COUNT( InstanceID ) FROM {} WHERE {}".format(", ".join(tables), " AND ".join(sqlCond))
        retVal = self._query(sqlQuery)
        if retVal["OK"]:
            totalRecords = retVal["Value"][0][0]
        # return
        return S_OK({"ParameterNames": fields, "Records": data, "TotalRecords": totalRecords})

    def getHistoryForInstanceID(self, instanceId):
        try:
            instanceId = int(instanceId)
        except ValueError:
            return S_ERROR("Instance Id has to be a number!")

        fields = ("Status", "Load", "Update", "Jobs", "TransferredFiles", "TransferredBytes")
        sqlFields = ["`%s`" % f for f in fields]

        sqlQuery = "SELECT %s FROM `vm_History` WHERE InstanceId=%d" % (", ".join(sqlFields), instanceId)
        retVal = self._query(sqlQuery)
        if not retVal["OK"]:
            return retVal
        return S_OK({"ParameterNames": fields, "Records": retVal["Value"]})

    def getInstanceCounters(self, groupField="Status", selDict=None):
        if not selDict:
            selDict = {}
        validFields = VirtualMachineDB.tablesDesc["vm_Instances"]["Fields"]
        if groupField not in validFields:
            return S_ERROR("%s is not a valid field" % groupField)
        sqlCond = []
        for field in selDict:
            if field not in validFields:
                return S_ERROR("%s is not a valid field" % field)
            value = selDict[field]
            if not isinstance(value, (dict, tuple)):
                value = (value,)
            value = [self._escapeString(str(v))["Value"] for v in value]
            sqlCond.append("`{}` in ({})".format(field, ", ".join(value)))
        sqlQuery = f"SELECT `{groupField}`, COUNT( `{groupField}` ) FROM `vm_Instances`"

        if sqlCond:
            sqlQuery += " WHERE %s" % " AND ".join(sqlCond)
        sqlQuery += " GROUP BY `%s`" % groupField

        result = self._query(sqlQuery)
        if not result["OK"]:
            return result
        return S_OK(dict(result["Value"]))

    def getHistoryValues(self, averageBucket, selDict=None, fields2Get=False, timespan=0):
        if not selDict:
            selDict = {}
        try:
            timespan = max(0, int(timespan))
        except ValueError:
            return S_ERROR("Timespan has to be an integer")

        cumulativeFields = ["Jobs", "TransferredFiles", "TransferredBytes"]
        validDataFields = ["Load", "Jobs", "TransferredFiles", "TransferredBytes"]
        allValidFields = VirtualMachineDB.tablesDesc["vm_History"]["Fields"]

        if not fields2Get:
            fields2Get = list(validDataFields)
        for field in fields2Get:
            if field not in validDataFields:
                return S_ERROR("%s is not a valid data field" % field)

        # paramFields = fields2Get
        try:
            bucketSize = int(averageBucket)
        except ValueError:
            return S_ERROR("Average bucket has to be an integer")

        sqlGroup = "FROM_UNIXTIME(UNIX_TIMESTAMP( `Update` ) - UNIX_TIMESTAMP( `Update` ) mod %d)" % bucketSize
        sqlFields = ["`InstanceID`", sqlGroup]  # + [ "SUM(`%s`)/COUNT(`%s`)" % ( f, f ) for f in fields2Get ]
        for field in fields2Get:
            if field in cumulativeFields:
                sqlFields.append("MAX(`%s`)" % field)
            else:
                sqlFields.append(f"SUM(`{field}`)/COUNT(`{field}`)")

        sqlGroup = "%s, InstanceID" % sqlGroup
        paramFields = ["Update"] + fields2Get
        sqlCond = []

        for field in selDict:
            if field not in allValidFields:
                return S_ERROR("%s is not a valid field" % field)
            value = selDict[field]
            if not isinstance(value, (list, tuple)):
                value = (value,)
            value = [self._escapeString(str(v))["Value"] for v in value]
            sqlCond.append("`{}` in ({})".format(field, ", ".join(value)))
        if timespan > 0:
            sqlCond.append("TIMESTAMPDIFF( SECOND, `Update`, UTC_TIMESTAMP() ) < %d" % timespan)
        sqlQuery = "SELECT %s FROM `vm_History`" % ", ".join(sqlFields)
        if sqlCond:
            sqlQuery += " WHERE %s" % " AND ".join(sqlCond)
        sqlQuery += " GROUP BY %s ORDER BY `Update` ASC" % sqlGroup
        result = self._query(sqlQuery)
        if not result["OK"]:
            return result
        dbData = result["Value"]
        # Need ext?
        requireExtension = set()
        for i in range(len(fields2Get)):
            f = fields2Get[i]
            if f in cumulativeFields:
                requireExtension.add(i)

        if requireExtension:
            rDates = []
            for row in dbData:
                if row[1] not in rDates:
                    rDates.append(row[1])
            vmData = {}
            for row in dbData:
                vmID = row[0]
                if vmID not in vmData:
                    vmData[vmID] = {}
                vmData[vmID][row[1]] = row[2:]
            rDates.sort()

            dbData = []
            for vmID in vmData:
                prevValues = []
                for rDate in rDates:
                    if rDate not in vmData[vmID]:
                        if prevValues:
                            instValues = [rDate]
                            instValues.extend(prevValues)
                            dbData.append(instValues)
                    else:
                        row = vmData[vmID][rDate]
                        prevValues = []
                        for i in range(len(row)):
                            if i in requireExtension:
                                prevValues.append(row[i])
                            else:
                                prevValues.append(0)

                        instValues = [rDate]
                        for i in range(len(row)):
                            instValues.extend(row)
                        dbData.append(instValues)
        else:
            # If we don't require extension just strip vmName
            dbData = [row[1:] for row in dbData]

        # Final sum
        sumData = {}
        for record in dbData:
            recDate = record[0]
            rawData = record[1:]
            if recDate not in sumData:
                sumData[recDate] = [0.0 for f in rawData]
            for i in range(len(rawData)):
                sumData[recDate][i] += float(rawData[i])
        finalData = []
        if len(sumData) > 0:
            firstValues = sumData[sorted(sumData)[0]]
            for date in sorted(sumData):
                finalData.append([date])
                values = sumData[date]
                for i in range(len(values)):
                    if i in requireExtension:
                        finalData[-1].append(max(0, values[i] - firstValues[i]))
                    else:
                        finalData[-1].append(values[i])

        return S_OK({"ParameterNames": paramFields, "Records": finalData})

    def getRunningInstancesHistory(self, timespan=0, bucketSize=900):

        try:
            bucketSize = max(300, int(bucketSize))
        except ValueError:
            return S_ERROR("Bucket has to be an integer")

        try:
            timespan = max(0, int(timespan))
        except ValueError:
            return S_ERROR("Timespan has to be an integer")

        groupby = "FROM_UNIXTIME(UNIX_TIMESTAMP( `Update` ) - UNIX_TIMESTAMP( `Update` ) mod %d )" % bucketSize
        sqlFields = [groupby, "COUNT( DISTINCT( `InstanceID` ) )"]
        sqlQuery = "SELECT %s FROM `vm_History`" % ", ".join(sqlFields)
        sqlCond = ["`Status` = 'Running'"]

        if timespan > 0:
            sqlCond.append("TIMESTAMPDIFF( SECOND, `Update`, UTC_TIMESTAMP() ) < %d" % timespan)
        sqlQuery += " WHERE %s" % " AND ".join(sqlCond)
        sqlQuery += " GROUP BY %s ORDER BY `Update` ASC" % groupby

        return self._query(sqlQuery)

    def getRunningInstancesBEPHistory(self, timespan=0, bucketSize=900):
        try:
            bucketSize = max(300, int(bucketSize))
        except ValueError:
            return S_ERROR("Bucket has to be an integer")
        try:
            timespan = max(0, int(timespan))
        except ValueError:
            return S_ERROR("Timespan has to be an integer")

        groupby = "FROM_UNIXTIME(UNIX_TIMESTAMP( h.`Update` ) - UNIX_TIMESTAMP( h.`Update` ) mod %d )" % bucketSize
        sqlFields = [groupby, " i.Endpoint, COUNT( DISTINCT( h.`InstanceID` ) ) "]
        sqlQuery = "SELECT %s FROM `vm_History` h, `vm_Instances` i" % ", ".join(sqlFields)
        sqlCond = [" h.InstanceID = i.InstanceID AND h.`Status` = 'Running'"]

        if timespan > 0:
            sqlCond.append("TIMESTAMPDIFF( SECOND, `Update`, UTC_TIMESTAMP() ) < %d" % timespan)
        sqlQuery += " WHERE %s" % " AND ".join(sqlCond)
        sqlQuery += " GROUP BY %s , EndPoint ORDER BY `Update` ASC" % groupby

        return self._query(sqlQuery)

    def getRunningInstancesByRunningPodHistory(self, timespan=0, bucketSize=900):
        try:
            bucketSize = max(300, int(bucketSize))
        except ValueError:
            return S_ERROR("Bucket has to be an integer")
        try:
            timespan = max(0, int(timespan))
        except ValueError:
            return S_ERROR("Timespan has to be an integer")

        groupby = "FROM_UNIXTIME(UNIX_TIMESTAMP( h.`Update` ) - UNIX_TIMESTAMP( h.`Update` ) mod %d )" % bucketSize
        sqlFields = [groupby, " i.RunningPod, COUNT( DISTINCT( h.`InstanceID` ) ) "]
        sqlQuery = "SELECT %s FROM `vm_History` h, `vm_Instances` i" % ", ".join(sqlFields)
        sqlCond = [" h.InstanceID = i.InstanceID AND h.`Status` = 'Running'"]

        if timespan > 0:
            sqlCond.append("TIMESTAMPDIFF( SECOND, `Update`, UTC_TIMESTAMP() ) < %d" % timespan)
        sqlQuery += " WHERE %s" % " AND ".join(sqlCond)
        sqlQuery += " GROUP BY %s , RunningPod ORDER BY `Update` ASC" % groupby

        return self._query(sqlQuery)

    def getRunningInstancesByImageHistory(self, timespan=0, bucketSize=900):
        try:
            bucketSize = max(300, int(bucketSize))
        except ValueError:
            return S_ERROR("Bucket has to be an integer")
        try:
            timespan = max(0, int(timespan))
        except ValueError:
            return S_ERROR("Timespan has to be an integer")

        groupby = "FROM_UNIXTIME(UNIX_TIMESTAMP( h.`Update` ) - UNIX_TIMESTAMP( h.`Update` ) mod %d )" % bucketSize
        sqlFields = [groupby, " ins.Name, COUNT( DISTINCT( h.`InstanceID` ) ) "]
        sqlQuery = "SELECT %s FROM `vm_History` h, `vm_Images` img, `vm_Instances` ins" % ", ".join(sqlFields)
        sqlCond = [" h.InstanceID = ins.InstanceID AND img.VMImageID = ins.VMImageID AND h.`Status` = 'Running'"]

        if timespan > 0:
            sqlCond.append("TIMESTAMPDIFF( SECOND, `Update`, UTC_TIMESTAMP() ) < %d" % timespan)
        sqlQuery += " WHERE %s" % " AND ".join(sqlCond)
        sqlQuery += " GROUP BY %s , ins.Name ORDER BY `Update` ASC" % groupby

        return self._query(sqlQuery)

    #######################
    # Private Functions
    #######################

    def __initializeDB(self):
        """
        Create the tables
        """
        tables = self._query("show tables")
        if not tables["OK"]:
            return tables

        tablesInDB = [table[0] for table in tables["Value"]]

        tablesToCreate = {}
        for tableName in self.tablesDesc:
            if tableName not in tablesInDB:
                tablesToCreate[tableName] = self.tablesDesc[tableName]

        return self._createTables(tablesToCreate)

    def __getTypeTuple(self, element):
        """
        return tuple of (tableName, validStates, idName) for object
        """
        # defaults
        tableName, validStates, idName = "", [], ""

        if element == "Image":
            tableName = "vm_Images"
            validStates = self.validImageStates
            idName = "VMImageID"
        elif element == "Instance":
            tableName = "vm_Instances"
            validStates = self.validInstanceStates
            idName = "InstanceID"

        return (tableName, validStates, idName)

    def __insertInstance(self, uniqueID, imageName, instanceName, endpoint, runningPodName):
        """
        Attempts to insert a new Instance for the given Image in a given Endpoint of a runningPodName
        """
        image = self.__getImageID(imageName)
        if not image["OK"]:
            return image
        imageID = image["Value"]

        tableName, validStates, _idName = self.__getTypeTuple("Instance")

        if uniqueID:
            status = "Submitted"
        else:
            status = validStates[0]

        fields = ["UniqueID", "RunningPod", "Name", "Endpoint", "VMImageID", "Status", "LastUpdate"]
        values = [uniqueID, runningPodName, instanceName, endpoint, imageID, status, str(datetime.datetime.utcnow())]

        instance = self.insertFields(tableName, fields, values)
        if not instance["OK"]:
            return instance

        if "lastRowId" in instance:
            self.__addInstanceHistory(instance["lastRowId"], status)
            return S_OK(instance["lastRowId"])

        return S_ERROR("Failed to insert new Instance")

    def __runningInstance(self, instanceID, load, jobs, transferredFiles, transferredBytes):
        """
        Checks image status, set it to running and set instance status to running
        """
        # Check the Image is OK
        imageID = self.__getImageForRunningInstance(instanceID)
        if not imageID["OK"]:
            self.__setError("Instance", instanceID, imageID["Message"])
            return imageID
        imageID = imageID["Value"]

        # Update Instance to Running
        stateInstance = self.__setState("Instance", instanceID, "Running")
        if not stateInstance["OK"]:
            return stateInstance

        # Update Image to Validated
        stateImage = self.__setState("Image", imageID, "Validated")
        if not stateImage["OK"]:
            self.__setError("Instance", instanceID, stateImage["Message"])
            return stateImage

        # Add History record
        self.__addInstanceHistory(instanceID, "Running", load, jobs, transferredFiles, transferredBytes)
        return S_OK()

    def __getImageForRunningInstance(self, instanceID):
        """
        Looks for imageID for a given instanceID.
        Check image Transition to Running is allowed
        Returns:
          S_OK( imageID )
          S_ERROR( Reason )
        """
        info = self.__getInfo("Instance", instanceID)
        if not info["OK"]:
            return info
        info = info["Value"]

        _tableName, _validStates, idName = self.__getTypeTuple("Image")

        imageID = info[idName]

        imageStatus = self.__getStatus("Image", imageID)
        if not imageStatus["OK"]:
            return imageStatus

        return S_OK(imageID)

    def __getOldInstanceIDs(self, secondsIdle, states):
        """
        Return list of instance IDs that have not updated after the given time stamp
        they are required to be in one of the given states
        """
        tableName, _validStates, idName = self.__getTypeTuple("Instance")

        sqlCond = []
        sqlCond.append("TIMESTAMPDIFF( SECOND, `LastUpdate`, UTC_TIMESTAMP() ) > % d" % secondsIdle)
        sqlCond.append('Status IN ( "%s" )' % '", "'.join(states))

        sqlSelect = "SELECT {} from `{}` WHERE {}".format(idName, tableName, " AND ".join(sqlCond))

        return self._query(sqlSelect)

    def __getSubmittedInstanceID(self, imageName):
        """
        Retrieve and InstanceID associated to a submitted Instance for a given Image
        """
        tableName, _validStates, idName = self.__getTypeTuple("Image")

        imageID = self.getFields(tableName, [idName], {"Name": imageName})
        if not imageID["OK"]:
            return imageID
        imageID = imageID["Value"]

        if not imageID:
            return S_ERROR("Unknown Image = %s" % imageName)

        # FIXME: <> is obsolete
        if len(imageID) != 1:
            return S_ERROR('Image name "%s" is not unique' % imageName)

        imageID = imageID[0][0]
        imageIDName = idName

        tableName, _validStates, idName = self.__getTypeTuple("Instance")

        instanceID = self.getFields(tableName, [idName], [imageIDName, "Status"], {imageID: "Submitted"})
        if not instanceID["OK"]:
            return instanceID
        instanceID = instanceID["Value"]

        if not instanceID:
            return S_ERROR('No Submitted instance of "%s" found' % imageName)

        return S_OK(instanceID[0][0])

    def __setState(self, element, iD, state):
        """
        Attempt to set element in state, checking if transition is allowed
        """

        knownStates = self.allowedTransitions[element].keys()
        if state not in knownStates:
            return S_ERROR("Transition to %s not possible" % state)

        allowedStates = self.allowedTransitions[element][state]

        currentState = self.__getStatus(element, iD)
        if not currentState["OK"]:
            return currentState
        currentState = currentState["Value"]

        if currentState not in allowedStates:
            msg = f"Transition ( {currentState} -> {state} ) not allowed"
            if currentState == "Halted":
                val_state = "halt"
            elif currentState == "Stopping":
                val_state = "stop"
            else:
                val_state = currentState
            return {"OK": False, "Message": msg, "State": val_state}

        tableName, _validStates, idName = self.__getTypeTuple(element)

        if currentState == state:
            sqlUpdate = f"UPDATE `{tableName}` SET LastUpdate = UTC_TIMESTAMP() WHERE {idName} = {iD}"

        else:
            sqlUpdate = 'UPDATE `{}` SET Status = "{}", LastUpdate = UTC_TIMESTAMP() WHERE {} = {}'.format(
                tableName,
                state,
                idName,
                iD,
            )

        ret = self._update(sqlUpdate)
        if not ret["OK"]:
            return ret
        return S_OK(state)

    def __setInstanceIPs(self, instanceID, publicIP, privateIP):
        """
        Update parameters for an instanceID reporting as running
        """
        values = self._escapeValues([publicIP, privateIP])
        if not values["OK"]:
            return S_ERROR("Cannot escape values: %s" % str(values))
        publicIP, privateIP = values["Value"]

        tableName, _validStates, idName = self.__getTypeTuple("Instance")
        sqlUpdate = "UPDATE `{}` SET PublicIP = {}, PrivateIP = {} WHERE {} = {}".format(
            tableName,
            publicIP,
            privateIP,
            idName,
            instanceID,
        )

        return self._update(sqlUpdate)

    def __getImageID(self, imageName):
        """
        For a given imageName return corresponding ID
        Will insert the image in New Status if it does not exits,
        """
        tableName, validStates, idName = self.__getTypeTuple("Image")
        imageID = self.getFields(tableName, [idName], {"Name": imageName})
        if not imageID["OK"]:
            return imageID
        imageID = imageID["Value"]

        if len(imageID) > 1:
            return S_ERROR('Image name "%s" is not unique' % imageName)
        if len(imageID) == 0:
            # The image does not exits in DB, has to be inserted
            imageID = 0
        else:
            # The image exits in DB, has to match
            imageID = imageID[0][0]

        if imageID:
            ret = self.getFields(tableName, [idName], {"Name": imageName})
            if not ret["OK"]:
                return ret
            if not ret["Value"]:
                return S_ERROR('Image "%s" in DB but it does not match' % imageName)
            else:
                return S_OK(imageID)

        ret = self.insertFields(
            tableName, ["Name", "Status", "LastUpdate"], [imageName, validStates[0], str(datetime.datetime.utcnow())]
        )

        if ret["OK"] and "lastRowId" in ret:

            rowID = ret["lastRowId"]

            ret = self.getFields(tableName, [idName], {"Name": imageName})
            if not ret["OK"]:
                return ret

            if not ret["Value"] or rowID != ret["Value"][0][0]:
                result = self.__getInfo("Image", rowID)
                if result["OK"]:
                    image = result["Value"]
                    self.log.error('Trying to insert Name: "%s"' % (imageName))
                    self.log.error('But inserted     Name: "%s"' % (image["Name"]))
                return self.__setError("Image", rowID, "Failed to insert new Image")
            return S_OK(rowID)

        return S_ERROR("Failed to insert new Image")

    def __addInstanceHistory(self, instanceID, status, load=0.0, jobs=0, transferredFiles=0, transferredBytes=0):
        """
        Insert a History Record
        """
        try:
            load = float(load)
        except ValueError:
            return S_ERROR("Load has to be a float value")
        try:
            jobs = int(jobs)
        except ValueError:
            return S_ERROR("Jobs has to be an integer value")
        try:
            transferredFiles = int(transferredFiles)
        except ValueError:
            return S_ERROR("Transferred files has to be an integer value")

        self.insertFields(
            "vm_History",
            ["InstanceID", "Status", "Load", "Update", "Jobs", "TransferredFiles", "TransferredBytes"],
            [instanceID, status, load, str(datetime.datetime.utcnow()), jobs, transferredFiles, transferredBytes],
        )
        return

    def __setLastLoadJobsAndUptime(self, instanceID, load, jobs, uptime):
        if not uptime:
            sqlQuery = (
                "SELECT MAX( UNIX_TIMESTAMP( `Update` ) ) - MIN( UNIX_TIMESTAMP( `Update` ) )"
                " FROM `vm_History` WHERE InstanceID = %d GROUP BY InstanceID" % instanceID
            )
            result = self._query(sqlQuery)
            if result["OK"] and len(result["Value"]) > 0:
                uptime = int(result["Value"][0][0])
        sqlUpdate = "UPDATE `vm_Instances` SET `Uptime` = %d, `Jobs`= %d, `Load` = %f WHERE `InstanceID` = %d" % (
            uptime,
            jobs,
            load,
            instanceID,
        )
        self._update(sqlUpdate)
        return S_OK()

    def __getInfo(self, element, iD):
        """
        Return dictionary with info for Images and Instances by ID
        """
        tableName, _validStates, idName = self.__getTypeTuple(element)
        if not tableName:
            return S_ERROR("Unknown DB object: %s" % element)

        fields = self.tablesDesc[tableName]["Fields"]
        ret = self.getFields(tableName, fields, {idName: iD})
        if not ret["OK"]:
            return ret
        if not ret["Value"]:
            return S_ERROR(f"Unknown {idName} = {iD}")

        values = ret["Value"][0]
        fields = list(fields.keys())
        return S_OK(dict(zip(fields, values)))

    def __getStatus(self, element, iD):
        """
        Check and return status of Images and Instances by ID
        :return: S_OK(tuple(status(int), message(str))) or S_ERROR(error(str))
        """
        tableName, validStates, idName = self.__getTypeTuple(element)
        if not tableName:
            return S_ERROR("Unknown DB object: %s" % element)

        ret = self.getFields(tableName, ["Status", "ErrorMessage"], {idName: iD})
        if not ret["OK"]:
            return ret

        if not ret["Value"]:
            return S_ERROR(f"Unknown {idName} = {iD}")

        status, msg = ret["Value"][0]
        if status not in validStates:
            return self.__setError(element, iD, "Invalid Status: %s" % status)
        if status == validStates[-1]:
            return S_ERROR(msg)

        return S_OK(status)

    def __setError(self, element, iD, reason):
        """ """
        (tableName, validStates, idName) = self.__getTypeTuple(element)
        if not tableName:
            return S_ERROR("Unknown DB object: %s" % element)

        sqlUpdate = 'UPDATE `%s` SET Status = "%s", ErrorMessage = "%s", LastUpdate = UTC_TIMESTAMP() WHERE %s = %s'
        sqlUpdate = sqlUpdate % (tableName, validStates[-1], reason, idName, iD)
        ret = self._update(sqlUpdate)
        if not ret["OK"]:
            return ret

        return S_ERROR(reason)
