""" GGUSTicketsClient class is a client for the GGUS Tickets DB.
"""


from suds.client import Client
from DIRAC import gLogger
from DIRAC.ResourceStatusSystem.Utilities.Utils import *

class GGUSTicketsClient:
  
#############################################################################

  def getTicketsList(self, name, startDate = None, endDate = None, ticketStatus = 'open'):
    """  Return tickets of entity in name

       :params:
         :attr:`name`: should be the name of the site

         :attr:`startDate`: starting date (optional)
          
        :attr:`endDate`: end date (optional)  
          
        :attr:`ticketStatus`: ticket status (default is open)  
    """
    self.siteName = name
    
    # create client instance using GGUS wsdl:
    self.gclient = Client( "https://gusiwr.fzk.de/arsys/WSDL/public/gusiwr/Grid_HelpDesk" )
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

    # the query must be into a try block. Empty queries, though formally correct, raise an exception
    try: 
      self.ticketList = self.gclient.service.TicketGetList( self.query )
    except:
      print 'ERROR querying tickets for site ' , self.siteName
      return
    self.globalStatistics()
    # create the URL to get tickets relative to the site:
    GGUSURL = "https://gus.fzk.de/ws/ticket_search.php?show_columns_check[]=REQUEST_ID&show_columns_check[]=TICKET_TYPE&show_columns_check[]=AFFECTED_VO&show_columns_check[]=AFFECTED_SITE&show_columns_check[]=RESPONSIBLE_UNIT&show_columns_check[]=STATUS&show_columns_check[]=DATE_OF_CREATION&show_columns_check[]=LAST_UPDATE&show_columns_check[]=SHORT_DESCRIPTION&ticket=&supportunit=all&vo=all&user=&keyword=&involvedsupporter=&assignto=&affectedsite=" + self.siteName + "&specattrib=0&status=open&priority=all&typeofproblem=all&mouarea=&radiotf=1&timeframe=lastweek&tf_date_day_s=&tf_date_month_s=&tf_date_year_s=&tf_date_day_e=&tf_date_month_e=&tf_date_year_e=&lm_date_day=12&lm_date_month=2&lm_date_year=2010&orderticketsby=GHD_INT_REQUEST_ID&orderhow=descending"
    return self.count, GGUSURL

#############################################################################
  def globalStatistics(self):
    '''print some statistics about the tickets for the site: total number of tickets and number of ticket in different status'''
    self.selectedTickets = {} # initialize the dictionary of tickets to return
    for ticket in self.ticketList:
      id = ticket[3]
      if id not in self.selectedTickets.keys():
        self.selectedTickets[id] = {}
        self.selectedTickets[id]['status'] = str(ticket[0])
        self.selectedTickets[id]['shortDescription'] = str(ticket[1])
        self.selectedTickets[id]['responsibleUnit'] = str(ticket[2])
        self.selectedTickets[id]['site'] = str(ticket[4])
    print 'total number of tickets: ', len(self.selectedTickets.keys()) 
    self.count = {}
    for id in self.selectedTickets.keys():
      status = self.selectedTickets[id]['status']
      if status not in self.count.keys():
        self.count[status] = 0
      self.count[status] += 1
    for status in self.count.keys():
      print 'in status ', status, '->', self.count[status]   
    return

#############################################################################

