""" GGUSTicketsClient class is a client for the GGUS Tickets DB.
"""


from suds.client import Client
from DIRAC import gLogger
from DIRAC.ResourceStatusSystem.Utilities.Utils import *

class GGUSTicketsClient:
  
#############################################################################

  def getTicketsList(self, name, startDate = None, endDate = None, ticketStatus = 'all'):
    """  Return tickets of entity in name

       :params:
         :attr:`name`: should be the name of the site

         :attr:`startDate`: starting date (optional)
          
        :attr:`endDate`: end date (optional)  
          
        :attr:`ticketStatus`: ticket status (default is all)  
    """
    self.siteName = name
    
    # create client instance using test GGUS wsdl
    self.gclient = Client( "https://iwrgustrain.fzk.de/arsys/WSDL/public/iwrgustrain/Grid_HelpDesk" )
    authInfo = self.gclient.factory.create( "AuthenticationInfo" )
    authInfo.userName = "ticketinfo"
    authInfo.password = "TicketInfo" 
    self.gclient.set_options(soapheaders=authInfo)
    # prepare the query string:
    self.query = '\'GHD_Affected Site\'=\"'+ self.siteName + '\"'
    self.startDate = startDate
    if self.startDate is not None:
      print 'set the starting date as ', self.startDate
      self.query = self.query + ' AND \'GHD_Date Of Creation\'>' + str(self.startDate) 
    self.endDate = endDate
    if self.endDate is not None:
      print 'set the end date as ', self.endDate
      self.query = self.query + ' AND \'GHD_Date Of Creation\'<' + str(self.endDate)

#    print 'the query string is ', self.query
    # the query must be into a try block. Empty queries, though formally correct, raise an exception

    try: 
      ticket_list = self.gclient.service.TicketGetList( self.query )
    except:
      print 'ERROR querying tickets for site ' , self.siteName
      return
    # select tickets by status
    self.ticketStatus = ticketStatus
    if self.ticketStatus == 'all':
      return ticket_list
    elif self.ticketStatus == 'open':
      print 'return only open tickets (to be implemented..)'
    elif self.ticketStatus == 'solved':
      print 'return only solved tickets (to be implemented..)'
    elif self.ticketStatus == 'assigned':
      print 'return only assigned tickets (to be implemented..)'
    else: 
      print 'ticket unknown status'
      return

#############################################################################

