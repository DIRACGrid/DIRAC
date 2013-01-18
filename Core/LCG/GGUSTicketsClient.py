# $HeadURL$
""" GGUSTicketsClient class is a client for the GGUS Tickets DB.
"""

import urllib2

from suds        import WebFault
from suds.client import Client

from DIRAC import S_OK, S_ERROR

__RCSID__ = "$Id$"

class GGUSTicketsClient:

  # create the URL to get tickets relative to the site ( opened only ! ): 
  ggusURL = 'https://ggus.eu/ws/ticket_search.php?show_columns_check[]=REQUEST_ID&'
  ggusURL += 'show_columns_check[]=TICKET_TYPE&show_columns_check[]=AFFECTED_VO&show_columns_check[]='
  ggusURL += 'AFFECTED_SITE&show_columns_check[]=PRIORITY&show_columns_check[]=RESPONSIBLE_UNIT&show_'
  ggusURL += 'columns_check[]=STATUS&show_columns_check[]=DATE_OF_CREATION&show_columns_check[]=LAST_UPDATE&'
  ggusURL += 'show_columns_check[]=TYPE_OF_PROBLEM&show_columns_check[]=SUBJECT&ticket=&supportunit=all&su_'
  ggusURL += 'hierarchy=all&vo=lhcb&user=&keyword=&involvedsupporter=&assignto=&affectedsite=%s' #% siteName
  ggusURL += '&specattrib=0&status=open&priority=all&typeofproblem=all&ticketcategory=&mouarea=&technology_'
  ggusURL += 'provider=&date_type=creation+date&radiotf=1&timeframe=any&untouched_date=&orderticketsby=GHD_'
  ggusURL += 'INT_REQUEST_ID&orderhow=descending' 

  def __init__( self ):
    
    # create client instance using GGUS wsdl:
    self.gclient          = Client( "https://prod-ars.ggus.eu/arsys/WSDL/public/prod-ars/GGUS" )
    authInfo = self.gclient.factory.create( "AuthenticationInfo" )
    authInfo.userName     = "ticketinfo"
    authInfo.password     = "TicketInfo"
    self.gclient.set_options( soapheaders = authInfo )

################################################################################

  def getTicketsList( self, siteName = None, startDate = None, endDate = None ):
    """ Return tickets of entity in name
       @param name: should be the name of the site
       @param startDate: starting date (optional)
       @param endDate: end date (optional)
    """

    # prepare the query string:
    
    #query = '\'GHD_Affected Site\'=\"' + siteName + '\" AND \'GHD_Affected VO\'="lhcb"'
    
    query = '\'GHD_Affected VO\'="lhcb"'
    if siteName is not None:
      query += ' AND \'GHD_Affected Site\'=\"' + siteName + '\"'
    
    if startDate is not None:
      query = query + ' AND \'GHD_Date Of Creation\'>' + str( startDate )
    
    if endDate is not None:
      query = query + ' AND \'GHD_Date Of Creation\'<' + str( endDate )
    
    # the query must be into a try block. Empty queries, though formally correct, raise an exception
    try:
      ticketList = self.gclient.service.TicketGetList( query )
    except WebFault, e:
      return S_ERROR( e )
    except urllib2.URLError, e:
      return S_ERROR( e )
    
    return self.globalStatistics( ticketList )

################################################################################
  
  def globalStatistics( self, ticketList ):
    '''
      Get some statistics about the tickets for the site: total number
      of tickets and number of ticket in different status
    '''
    
    # initialize the dictionary of tickets to return
    selectedTickets = {} 

    #openStates = [ 'assigned', 'in progress', 'new', 'on hold', 'reopened', 'waiting for reply' ]    
    terminalStates = [ 'solved', 'unsolved', 'verified', 'closed' ]
    
    for ticket in ticketList:
      
      _id               = ticket.GHD_Request_ID
      _status           = ticket.GHD_Status
      _shortDescription = ticket.GHD_Short_Description
      _priority         = ticket.GHD_Priority
      if not hasattr( ticket, 'GHD_Affected_Site' ):
        continue
      _site             = ticket.GHD_Affected_Site
        
      # We do not want closed tickets
      if _status in terminalStates:
        continue
    
      if not _site in selectedTickets:
        selectedTickets[ _site ] = { 'URL' : self.ggusURL % _site }
      
      if not _priority in selectedTickets[ _site ]:
        selectedTickets[ _site ][ _priority ] = []  
       
      selectedTickets[ _site ][ _priority ].append( ( _id, _shortDescription ) ) 
    
    return S_OK( selectedTickets )
        
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF
