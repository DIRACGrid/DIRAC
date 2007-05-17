

class MonitoringClient:

  #Different types of operations
  OP_MEAN = "mean"
  OP_SUM  = "sum"
  OP_RATE = "rate"

  #Predefined components that can be registered
  COMPONENT_SERVICE = "service"
  COMPONENT_AGENT   = "agent"

  def setComponentLocation( self, componentLocation ):
      """
      Set the location of the component reporting.

      @type  componentLocation: string
      @param componentLocation: Location of the component reporting
      """
      pass

  def setComponentName( self, componentName ):
      """
      Set the name of the component reporting.

      @type  componentName: string
      @param componentName: Name of the component reporting
      """
      pass

  def setComponentType( self, componentType ):
      """
      Define the type of component reporting data.

      @type  componentType: string
      @param componentType: Defines the grouping of the host by type. All the possibilities
                              are defined in the Constants.py file
      """
      pass

  def registerActivity( self, name, category, unit, operation, timeQuantums ):
      """
      Register new activity. Before reporting information to the server, the activity
      must be registered.

      @type  name: string
      @param name: Name of the activity to report
      @type  category: string
      @param category: Grouping of the activity
      @type  unit: string
      @param unit: String representing the unit that will be printed in the plots
      @type  operation: string
      @param operation: Type of data operation to represent data. All the possibilities
                          are defined in the Constants.py file
      @type  timeQuantums: number
      @param timeQuantums: Time resolution for the activity. Each quantum is 300s. The
                            final time resolution will be timeQuantums x 300 seconds
      """
      pass

  def addMark( self, name, value = 1 ):
      """
      Add a new mark to the specified activity

      @type  name: string
      @param name: Name of the activity to report
      @type  value: number
      @param value: Weight of the mark. By default it's one.
      """
      pass

gMonitor = MonitoringClient()