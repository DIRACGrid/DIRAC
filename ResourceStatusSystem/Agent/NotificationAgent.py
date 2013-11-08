# $HeadURL:  $
""" NotificationAgent

  This agents inspects the complete resources tree in the CS, and if there have
  been changes, notifies by email.

"""

from datetime import datetime, timedelta

from DIRAC                                                  import gConfig, gLogger, S_OK
from DIRAC.Core.Base.AgentModule                            import AgentModule
from DIRAC.Interfaces.API.DiracAdmin                        import DiracAdmin
from DIRAC.ResourceStatusSystem.Client.ResourceStatusClient import ResourceStatusClient
from DIRAC.ResourceStatusSystem.Utilities                   import CSHelpers


__RCSID__  = '$Id:  $'
AGENT_NAME = 'ResourceStatus/NotificationAgent'


class NotificationAgent( AgentModule ):
  """ NotificationAgent
  
  This agents sends periodically emails per Site with the changes in the Site
  resources. Note, if there are multiple changes per element, the email will
  reflect only the last change.
    
  """
  
  
  def __init__( self, *args, **kwargs ):
    """ c'tor
    """
    
    AgentModule.__init__( self, *args, **kwargs )
    
    self.rsClient  = None
    self.emailFrom = None
    self.emailTo   = []
    self.webPortal = ''
    
    
  def initialize( self ):
    """ initialize
    
    We need to specify who sends the email ( otherwise, eGroups will filter out
    our emails if the destination is an eGroup ). If we are not sending emails
    to an eGroup, then it really does not matter.
    
    FIXME:
    WebPortal, probably there is a better way to get this info ( url of the
    web portal ), but I do not know how (yet).
    
    emailTo, recipients of the email ( one email per recipient, as DiracAdmin
    cannot send emails in cc ).
    
    """

    self.rsClient  = ResourceStatusClient()
    self.emailFrom = self.am_getOption( 'emailFrom', self.emailFrom )
    self.emailTo   = self.am_getOption( 'emailTo'  , self.emailTo )
    self.webPortal = self.am_getOption( 'webPortal', self.webPortal )
   
    return S_OK()

  
  def execute( self ):
    """ execute
    
    This is the main agent method.
    NOTE: it will have to be rewritten with v7r0 to profit from the ResourcesHelper.
    
    """
    
    # Gets resources
    resources = self.getUpdatedResources()
    if not resources[ 'OK' ]:
      self.log.error( resources[ 'Message' ] )
    resources, timeWindow = resources[ 'Value' ]
    resourcesKeys = resources.keys()  
    
    # Gets ALL sites ( from CS )     
    sites = CSHelpers.getSites()
    if not sites[ 'OK' ]:
      self.log.error( sites[ 'Message' ] )
      return sites
    sites = sites[ 'Value' ]
    
    # Instatiate object to send emails
    notificationEmail = NotificationEmail( self.emailFrom, self.emailTo, 
                                           resources, timeWindow, self.webPortal )
    
    # Let's see the matches per site     
    for site in sites:
      
      #FIXME: with v7r0 will have to be changed to use the ResourcesHelper
      # and get all the elements, not just SEs and CEs.
      
      # Computing and StorageElements per Site
      ces = CSHelpers.getSiteComputingElements( site )
      ses = CSHelpers.getSiteStorageElements( site )
      
      matches = {}
      
      cmatches = set( ces ).intersection( resourcesKeys )
      if cmatches:
        matches[ 'CE' ] = cmatches
      smatches = set( ses ).intersection( resourcesKeys )
      if smatches:
        matches[ 'SE' ] = smatches
            
      if matches:
        notificationEmail.send( site, matches )
        
    return S_OK()


  def getUpdatedResources( self ):
    """ getUpdatedResources
    
    This method gets from the RSS DB the elements that were modified in the last
    <pollingTime> seconds. The format is a dictionary, where the keys are the
    names of the elements and the values are the respective entries ( there could
    be more than one if they have several StatusTypes ).
    
    """
    
    # We introduce a little overlap between emails, so that we do not miss any 
    # change between agent cycles.
    pollingTime = self.am_getPollingTime()  * 1.1
    # get time window
    timeWindow  = ( datetime.utcnow() - timedelta( seconds = pollingTime ) )

    resources = self.rsClient.selectStatusElement( 'Resource', 'Status',
                                                   meta = { 'newer' : ( 'DateEffective', timeWindow ) } )
    if not resources[ 'OK' ]:
      return resources
    
    resourcesDict = {}
    for resource in resources[ 'Value' ]:
      
      resourceDict = dict( zip( resources[ 'Columns' ], resource ) )
      if not resourceDict[ 'Name' ] in resourcesDict:
        resourcesDict[ resourceDict[ 'Name' ] ] = []
      resourcesDict[ resourceDict[ 'Name' ] ].append( resourceDict )

    if not resourcesDict:
      self.log.verbose( 'No updates since %s' % timeWindow )
    else:
      self.log.verbose( '%s updates since %s' % ( len( resourcesDict ), timeWindow ) )  

    return S_OK( ( resourcesDict, timeWindow ) ) 
    

class NotificationEmail( object ):
  """ NotificationEmail
  
  This class builds and formats the email to be sent regarding the last changes
  in the RSS Resources table. 
  
  NOTE: with v7r0 will have to be updated to handle Site updates as well.
  
  """


  def __init__( self, emailFrom, emailTo, resources, timeWindow, webPortal ):
    """ c'tor
    
    :Parameters:
      **emailFrom** - `string`
        from field in the email
      **emailTo** - `list`
        list with recipients of the email ( one email per item in the list ).
      **resources** - `dict`
        dictionary with all the resource information, with the format:
        { 'ResourceName1' : [ { dictInfoFromRSS1 }, { ... }, ... ], ... }    
      **webPortal** - `string`  
        URL of the web portal
      **timeWindow** - `datetime`
        point in time since changes in the resources considered
                  
    """   
    
    self.emailFrom  = emailFrom
    self.emailTo    = emailTo
    self.resources  = resources
    self.timeWindow = timeWindow.replace( microsecond = 0 )
    self.webPortal  = webPortal
    
    self.diracAdmin = DiracAdmin()
    self.setup      = gConfig.getValue( 'DIRAC/Setup' )
    self.log        = gLogger.getSubLogger( self.__class__.__name__ )

    self.log.verbose( 'NotficationEmail from: %s' % self.emailFrom )
    self.log.verbose( 'NotficationEmail to: %s' % ','.join( self.emailTo ) )


  def send( self, site, matches ):
    """ send
    
    Method that actually sends the emails to self.emailTo
    
    :Parameters:
      **site** - `string`
        SiteName for which we have matches
      **matches** - `dict`
        dictionary of the form { ResourceType : [ ResourceName1, ResourceName2,... ] }  
    
    """
    
    self.log.debug( 'Sending email for Site %s' % site )
    
    subject = '[RSS](%s) %s' % ( self.setup, site ) 
    content = self.buildEmail( site, matches )
    
    for emailTo in self.emailTo:
      sentEmail = self.diracAdmin.sendMail( emailTo, subject, content, fromAddress = self.emailFrom )
      if not sentEmail[ 'OK' ]:
        self.log.warn( sentEmail[ 'Message' ] )
    
    
  def buildEmail( self, site, matches ):
    """ buildEmail
    
    This method builds the content of the email to be sent.
    
    :Parameters:
      **site** - `string`
        SiteName for which we have matches
      **matches** - `dict`  
        dictionary of the form { ResourceType : [ ResourceName1, ResourceName2,... ] }
    
    """
    
    href    = '%s/DIRAC/%s/undefined/grid/SiteStatus/display?name=%s' % ( self.webPortal, self.setup, site )
    content = [ '=' * 120, site, '', 'Since:', str( self.timeWindow ), 'See more:','%s' % href, '=' * 120 ]
    
    for resourceType, changes in matches.iteritems():
      content.append( '' )
      content.append( '' )

      content.append( '-' * 80 )
      content.append( '%s (%s elements with updates)' % ( resourceType, len( changes ) ) )
      content.append( '-' * 80 )
      content.append( '' )
      for resourceName in changes:
        content.append( resourceName )
        for change in self.resources[ resourceName ]:
          content.append( '  %s -> %s' % ( change[ 'StatusType' ], change[ 'Status' ] ) )  
          content.append( '    %s : %s' % ( change[ 'DateEffective' ], change[ 'Reason' ] ) )
          content.append( '' )
    
    content.append( '' )
    content.append( 'Have a good day,' )
    content.append( 'Mss. RSS.' )
          
    return '\n'.join( content )      


#...............................................................................
#EOF
