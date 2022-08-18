"""
Accounting agent to consume perfSONAR network metrics received via a message queue.

.. literalinclude:: ../ConfigTemplate.cfg
  :start-after: ##BEGIN NetworkAgent
  :end-before: ##END
  :dedent: 2
  :caption: NetworkAgent options

"""
from datetime import datetime

from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.Core.Base.AgentModule import AgentModule
from DIRAC.AccountingSystem.Client.Types.Network import Network
from DIRAC.ConfigurationSystem.Client.Config import gConfig
from DIRAC.ConfigurationSystem.Client.CSAPI import CSAPI
from DIRAC.Resources.MessageQueue.MQCommunication import createConsumer


class NetworkAgent(AgentModule):
    """
    AccountingSystem agent to processes messages containing perfSONAR network metrics.
    Results are stored in the accounting database.
    """

    #: default number of seconds after which network accounting
    #: objects are removed from the temporary buffer
    BUFFER_TIMEOUT = 3600

    def initialize(self):

        self.log = gLogger.getSubLogger(self.__class__.__name__)

        # API initialization is required to get an up-to-date configuration from the CS
        self.csAPI = CSAPI()
        self.csAPI.initialize()

        # temporary buffer for network accounting objects + some parameters
        self.buffer = {}  # { {addTime: datetime.now(), object: Network() }, ... }
        self.bufferTimeout = self.am_getOption("BufferTimeout", NetworkAgent.BUFFER_TIMEOUT)

        # internal list of message queue consumers
        self.consumers = []

        # host-to-dirac name dictionary
        self.nameDictionary = {}

        # statistics
        self.messagesCount = 0  # number of received messages
        self.messagesCountOld = 0  # previous number of received messages (used to check connection status)

        self.skippedMessagesCount = 0  # number of skipped messages (errors, unsupported metrics, etc.)
        self.PLRMetricCount = 0  # number of received packet-loss-rate metrics
        self.OWDMetricCount = 0  # number of received one-way-delay metrics
        self.skippedMetricCount = 0  # number of skipped metrics (errors, invalid data, etc.)
        self.insertedCount = 0  # number of properly inserted accounting objects
        self.removedCount = 0  # number of removed accounting objects (missing data)

        return S_OK()

    def finalize(self):
        """
        Gracefully close all consumer connections and commit last records to the DB.
        """

        for consumer in self.consumers:
            consumer.close()

        self.commitData()

        return S_OK()

    def execute(self):
        """
        During each cycle update the internal host-to-dirac name dictionary,
        check the consumers status (restart them if necessary),
        commit data stored in the buffer and show statistics.
        """

        self.updateNameDictionary()
        self.checkConsumers()
        self.commitData()
        self.showStatistics()

        return S_OK()

    def updateNameDictionary(self):
        """
        Update the internal host-to-dirac name dictionary.
        """

        result = gConfig.getConfigurationTree("/Resources/Sites", "Network/", "/Enabled")
        if not result["OK"]:
            self.log.error("getConfigurationTree() failed with message: %s" % result["Message"])
            return S_ERROR("Unable to fetch perfSONAR endpoints from CS.")

        tmpDict = {}
        for path, value in result["Value"].items():
            if value == "True":
                elements = path.split("/")
                diracName = elements[4]
                hostName = elements[6]
                tmpDict[hostName] = diracName

        self.nameDictionary = tmpDict

    def checkConsumers(self):
        """
        Check whether consumers exist and work properly.
        (Re)create consumers if needed.
        """

        # recreate consumers if there are any problems
        if not self.consumers or self.messagesCount == self.messagesCountOld:

            for consumer in self.consumers:
                consumer.close()

            for uri in self.am_getOption("MessageQueueURI", "").replace(" ", "").split(","):
                result = createConsumer(uri, self.processMessage)
                if not result["OK"]:
                    self.log.error("Failed to create a consumer from URI: %s" % uri)
                    continue
                else:
                    self.log.info("Successfully created a consumer from URI: %s" % uri)

                self.consumers.append(result["Value"])

            if self.consumers:
                return S_OK("Successfully created at least one consumer")
            return S_ERROR("Failed to create at least one consumer")

        # if everything is OK just update the counter
        else:
            self.messagesCountOld = self.messagesCount

    def processMessage(self, headers, body):
        """
        Process a message containing perfSONAR data and store the result in the Accounting DB.
        Supports packet-loss-rate and one-way-delay metrics send in raw data streams.

        Function is designed to be an MQConsumer callback function.
        """

        self.messagesCount += 1
        metadata = {
            "SourceIP": body["meta"]["source"],
            "SourceHostName": body["meta"]["input_source"],
            "DestinationIP": body["meta"]["destination"],
            "DestinationHostName": body["meta"]["input_destination"],
        }

        try:
            metadata["Source"] = self.nameDictionary[body["meta"]["input_source"]]
            metadata["Destination"] = self.nameDictionary[body["meta"]["input_destination"]]
        except KeyError as error:
            # messages with unsupported source or destination host name can be safely skipped
            self.skippedMessagesCount += 1
            self.log.debug('Host "%s" does not exist in the host-to-dirac name dictionary (message skipped)' % error)
            return S_OK()

        metadataKey = ""
        for value in metadata.values():
            metadataKey += value

        timestamps = sorted(body["datapoints"])
        for timestamp in timestamps:
            try:
                date = datetime.utcfromtimestamp(float(timestamp))

                # create a key that allows to join packet-loss-rate and one-way-delay
                # metrics in one network accounting record
                networkAccountingObjectKey = f"{metadataKey}{str(date)}"

                # use existing or create a new temporary accounting
                # object to store the data in DB
                if networkAccountingObjectKey in self.buffer:
                    net = self.buffer[networkAccountingObjectKey]["object"]

                    timeDifference = datetime.now() - self.buffer[networkAccountingObjectKey]["addTime"]
                    if timeDifference.total_seconds() > 60:
                        self.log.warn("Object was taken from buffer after %s" % (timeDifference))
                else:
                    net = Network()
                    net.setStartTime(date)
                    net.setEndTime(date)
                    net.setValuesFromDict(metadata)

                # get data stored in metric
                metricData = body["datapoints"][timestamp]

                # look for supported event types
                if headers["event-type"] == "packet-loss-rate":
                    self.PLRMetricCount += 1
                    if metricData < 0 or metricData > 1:
                        raise Exception("Invalid PLR metric (%s)" % (metricData))

                    net.setValueByKey("PacketLossRate", metricData * 100)
                elif headers["event-type"] == "histogram-owdelay":
                    self.OWDMetricCount += 1

                    # calculate statistics from histogram
                    OWDMin = 999999
                    OWDMax = 0
                    total = 0
                    count = 0
                    for value, items in metricData.items():
                        floatValue = float(value)
                        total += floatValue * items
                        count += items
                        OWDMin = min(OWDMin, floatValue)
                        OWDMax = max(OWDMax, floatValue)
                    OWDAvg = float(total) / count

                    # skip metrics with invalid data
                    if OWDAvg < 0 or OWDMin < 0 or OWDMax < 0:
                        raise Exception(f"Invalid OWD metric ({OWDMin}, {OWDAvg}, {OWDMax})")
                    else:
                        # approximate jitter value
                        net.setValueByKey("Jitter", OWDMax - OWDMin)
                        net.setValueByKey("OneWayDelay", OWDAvg)

                else:
                    self.skippedMetricCount += 1
                    continue

                self.buffer[networkAccountingObjectKey] = {"addTime": datetime.now(), "object": net}

            # suppress all exceptions to protect the listener thread
            except Exception as e:
                self.skippedMetricCount += 1
                self.log.warn("Metric skipped because of an exception: %s" % e)

        return S_OK()

    def commitData(self):
        """
        Iterates through all object in the temporary buffer and commit objects to DB
        if both packet-loss-rate and one-way-delay values are set.

        Objects in the buffer older than self.bufferTimeout seconds which still have
        missing data are removed permanently (a warning is issued).
        """

        now = datetime.now()
        removed = False

        for key, value in self.buffer.items():
            result = value["object"].checkValues()
            if not result["OK"]:
                if (now - value["addTime"]).total_seconds() > self.bufferTimeout:
                    del self.buffer[key]
                    self.removedCount += 1
                    removed = True
            else:
                value["object"].delayedCommit()
                del self.buffer[key]
                self.insertedCount += 1

        if removed:
            self.log.warn("Network accounting object(s) has been removed because of missing data")

        return S_OK()

    def showStatistics(self):
        """Display different statistics as info messages in the log file."""

        self.log.info("\tReceived messages:           %s" % self.messagesCount)
        self.log.info("\tSkipped messages:            %s" % self.skippedMessagesCount)
        self.log.info("\tPacket-Loss-Rate metrics:    %s" % self.PLRMetricCount)
        self.log.info("\tOne-Way-Delay metrics:       %s" % self.OWDMetricCount)
        self.log.info("\tSkipped metrics:             %s" % self.skippedMetricCount)
        self.log.info("")
        self.log.info("\tObjects in the buffer:       %s" % len(self.buffer))
        self.log.info("\tObjects inserted to DB:      %s" % self.insertedCount)
        self.log.info("\tPermanently removed objects: %s" % self.removedCount)

        return S_OK()
