# $HeadURL$
""" GGUSTicketsClient class is a client for the GGUS Tickets DB.
"""
from suds        import WebFault
from suds.client import Client

from DIRAC import S_OK
from DIRAC.ConfigurationSystem.Client.Helpers import CSGlobals

__RCSID__ = "$Id$"

class GGUSTicketsClient:
  """ Just a class for dealing with the GGUS portal
  """

  def __init__( self ):
    """ c'tor
    """
    self.statusCount = {}
    self.shortDescription = {}

################################################################################

  def getTicketsList( self, name, startDate = None, endDate = None ):
    """ Return tickets of entity in name
       @param name: should be the name of the site
       @param startDate: starting date (optional)
       @param endDate: end date (optional)
    """
    self.statusCount = {}
    self.shortDescription = {}

    # create client instance using GGUS wsdl:
    gclient = Client( "https://prod-ars.ggus.eu/arsys/WSDL/public/prod-ars/GGUS" )
    authInfo = gclient.factory.create( "AuthenticationInfo" )
    authInfo.userName = "ticketinfo"
    authInfo.password = "TicketInfo"
    gclient.set_options( soapheaders = authInfo )
    # prepare the query string:
    extension = CSGlobals.getCSExtensions()[0].lower()
    query = '\'GHD_Affected Site\'=\"' + name + '\" AND \'GHD_Affected VO\'="%s"' % extension
    if startDate is not None:
      query = query + ' AND \'GHD_Date Of Creation\'>' + str( startDate )
    if endDate is not None:
      query = query + ' AND \'GHD_Date Of Creation\'<' + str( endDate )

    # create the URL to get tickets relative to the site:
    # Updated from https://gus.fzk.de to https://ggus.eu 
    ggusURL = "https://ggus.eu/ws/ticket_search.php?show_columns_check[]=REQUEST_ID&"\
                                                      "show_columns_check[]=TICKET_TYPE&"\
                                                      "show_columns_check[]=AFFECTED_VO&"\
                                                      "show_columns_check[]=AFFECTED_SITE&"\
                                                      "show_columns_check[]=RESPONSIBLE_UNIT&"\
                                                      "show_columns_check[]=STATUS&"\
                                                      "show_columns_check[]=DATE_OF_CREATION&"\
                                                      "show_columns_check[]=LAST_UPDATE&"\
                                                      "show_columns_check[]=SHORT_DESCRIPTION&"\
                                                      "ticket=&"\
                                                      "supportunit=all&"\
                                                      "vo=%s&"\
                                                      "user=&"\
                                                      "keyword=&"\
                                                      "involvedsupporter=&"\
                                                      "assignto=&"\
                                                      "affectedsite=" + name + "&"\
                                                      "specattrib=0&"\
                                                      "status=open&"\
                                                      "priority=all&"\
                                                      "typeofproblem=all&"\
                                                      "mouarea=&"\
                                                      "radiotf=1&"\
                                                      "timeframe=any&"\
                                                      "tf_date_day_s=&"\
                                                      "tf_date_month_s=&"\
                                                      "tf_date_year_s=&"\
                                                      "tf_date_day_e=&"\
                                                      "tf_date_month_e=&"\
                                                      "tf_date_year_e=&"\
                                                      "lm_date_day=12&"\
                                                      "lm_date_month=2&"\
                                                      "lm_date_year=2010&"\
                                                      "orderticketsby=GHD_INT_REQUEST_ID&"\
                                                      "orderhow=descending" % ( extension, extension )

    # the query must be into a try block. Empty queries, though formally correct, raise an exception
    try:
      self.ticketList = gclient.service.TicketGetList( query )
      self.globalStatistics()
    except WebFault:
      self.statusCount['terminal'] = 0
      self.statusCount['open'] = 0

    return S_OK( ( self.statusCount, ggusURL, self.shortDescription ) )

################################################################################
  def globalStatistics( self ):
    '''
        Get some statistics about the tickets for the site: total number
        of tickets and number of ticket in different status
    '''
    selectedTickets = {} # initialize the dictionary of tickets to return
    for ticket in self.ticketList:
      id_ = ticket[3][0]
      if id_ not in selectedTickets.keys():
        selectedTickets[id_] = {}
        selectedTickets[id_]['status'] = ticket[0][0]
        selectedTickets[id_]['shortDescription'] = ticket[1][0]
        selectedTickets[id_]['responsibleUnit'] = ticket[2][0]
        selectedTickets[id_]['site'] = ticket[4][0]
    count = {}
    # group tickets in only 2 categories: open and terminal states
    # create a dictionary to store the short description only for tickets in open states:
    openStates = ['assigned', 'in progress', 'new', 'on hold',
                  'reopened', 'waiting for reply']
    terminalStates = ['solved', 'unsolved', 'verified']
    self.statusCount['open'] = 0
    self.statusCount['terminal'] = 0
    for id_ in selectedTickets.keys():
      status = selectedTickets[id_]['status']
      if status not in count.keys():
        count[status] = 0
      count[status] += 1
      if status in terminalStates:
        self.statusCount['terminal'] += 1
      elif status in openStates:
        self.statusCount['open'] += 1
        if id_ not in self.shortDescription.keys():
          self.shortDescription[str( id_ )] = selectedTickets[id_]['shortDescription']
      else:
        pass
# st = 'ERROR! GGUS status unknown: ', status
# gLogger.error(st)

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF
