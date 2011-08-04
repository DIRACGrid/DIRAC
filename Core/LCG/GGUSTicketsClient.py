# $HeadURL$
""" GGUSTicketsClient class is a client for the GGUS Tickets DB.
"""
__RCSID__ = "$Id$"

from suds import WebFault
from suds.client import Client

from DIRAC import S_OK

class GGUSTicketsClient:
  # FIXME: Why is this a class and not just few methods?


  def __init__( self ):
    # FIXME: Why all these are Attributes of the class and not local variables?
    self.count = {}
    self.endDate = None
    self.gclient = None
    self.query = ''
    self.selectedTickets = {}
    self.siteName = ''
    self.startDate = None
    self.statusCount = {}


#############################################################################

  def getTicketsList( self, name, startDate = None, endDate = None ):
    """ Return tickets of entity in name
       @param name: should be the name of the site
       @param startDate: starting date (optional)
       @param endDate: end date (optional)  
    """
    self.siteName = name
    self.statusCount = {}
    self.shortDescription = {}

    # create client instance using GGUS wsdl:
    self.gclient = Client( "https://gusiwr.fzk.de/arsys/WSDL/public/gusiwr/Grid_HelpDesk" )
    authInfo = self.gclient.factory.create( "AuthenticationInfo" )
    authInfo.userName = "ticketinfo"
    authInfo.password = "TicketInfo"
    self.gclient.set_options( soapheaders = authInfo )
    # prepare the query string:
    self.query = '\'GHD_Affected Site\'=\"' + self.siteName + '\" AND \'GHD_Affected VO\'="lhcb"'
    #self.query = '\'GHD_Affected Site\'=\"'+ self.siteName + '\"'
    self.startDate = startDate
    if self.startDate is not None:
#      st = 'set the starting date as ', self.startDate
#      gLogger.info(st)
      self.query = self.query + ' AND \'GHD_Date Of Creation\'>' + str( self.startDate )
    self.endDate = endDate
    if self.endDate is not None:
#      st = 'set the end date as ', self.endDate
#      gLogger.info(st)
      self.query = self.query + ' AND \'GHD_Date Of Creation\'<' + str( self.endDate )

    # create the URL to get tickets relative to the site:
    ggusURL = "https://gus.fzk.de/ws/ticket_search.php?show_columns_check[]=REQUEST_ID&"\
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
                                                      "vo=lhcb&"\
                                                      "user=&"\
                                                      "keyword=&"\
                                                      "involvedsupporter=&"\
                                                      "assignto=&"\
                                                      "affectedsite=" + self.siteName + "&"\
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
                                                      "orderhow=descending"

    # the query must be into a try block. Empty queries, though formally correct, raise an exception
    try:
      self.ticketList = self.gclient.service.TicketGetList( self.query )
      self.globalStatistics()
    except WebFault:
      self.statusCount['terminal'] = 0
      self.statusCount['open'] = 0

    return S_OK( ( self.statusCount, ggusURL, self.shortDescription ) )

#############################################################################
  def globalStatistics( self ):
    '''
        Get some statistics about the tickets for the site: total number 
        of tickets and number of ticket in different status
    '''
    self.selectedTickets = {} # initialize the dictionary of tickets to return
    for ticket in self.ticketList:
      id = ticket[3]
      if id not in self.selectedTickets.keys():
        self.selectedTickets[id] = {}
        self.selectedTickets[id]['status'] = str( ticket[0] )
        self.selectedTickets[id]['shortDescription'] = str( ticket[1] )
        self.selectedTickets[id]['responsibleUnit'] = str( ticket[2] )
        self.selectedTickets[id]['site'] = str( ticket[4] )
#    print 'total number of tickets: ', len(self.selectedTickets.keys()) 
    self.count = {}
    # group tickets in only 2 categories: open and terminal states   
    # create a dictionary to store the short description only for tickets in open states:
    openStates = ['assigned', 'in progress', 'new', 'on hold',
                  'reopened', 'waiting for reply']
    terminalStates = ['solved', 'unsolved', 'verified']
    self.statusCount['open'] = 0
    self.statusCount['terminal'] = 0
    for id in self.selectedTickets.keys():
      status = self.selectedTickets[id]['status']
      if status not in self.count.keys():
        self.count[status] = 0
      self.count[status] += 1
      if status in terminalStates:
        self.statusCount['terminal'] += 1
      elif status in openStates:
        self.statusCount['open'] += 1
        if id not in self.shortDescription.keys():
          self.shortDescription[str( id )] = self.selectedTickets[id]['shortDescription']
      else:
        pass
#        st = 'ERROR! GGUS status unknown: ', status
#        gLogger.error(st)

#############################################################################

