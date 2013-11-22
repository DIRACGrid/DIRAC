""" DIRAC.AccountingSystem.Client.Types.SpaceToken

   SpaceToken.__bases__:
     DIRAC.AccountingSystem.Client.Types.SpaceToken.BaseAccountingType.BaseAccountingType
  
"""


from DIRAC.AccountingSystem.Client.Types.BaseAccountingType import BaseAccountingType


__RCSID__ = '$Id: $'


class SpaceToken( BaseAccountingType ):
  """SpaceToken as extension of BaseAccountingType
  
  It is filled from the RSS.Command.SpaceTokenCommand every time the command
  is executed ( see RSS.Agent.CacheFeederAgent ).
  
  """

  def __init__( self ):
    """ constructor
    
    """
     
    BaseAccountingType.__init__( self )
    
    self.definitionKeyFields = [ 
                                 ( 'Site'      , 'VARCHAR(64)' ),
                                 ( 'Endpoint'  , 'VARCHAR(256)' ),
                                 ( 'SpaceToken', 'VARCHAR(64)' ),
                                 ( 'SpaceType' , 'VARCHAR(64)')
                               ]
    
    self.definitionAccountingFields = [ ( 'Space', 'BIGINT UNSIGNED' )
                                      ]

    self.bucketsLength = [ ( 86400 * 2     , 3600    ), #<2d  = 1h
                           ( 86400 * 10    , 3600*6  ), #<10d = 6h
                           ( 86400 * 40    , 3600*12 ), #<40d = 12h
                           ( 86400 * 30 * 6, 86400*2 ), #<6m  = 2d
                           ( 86400 * 600   , 86400*7 ), #>6m  = 1w
                         ]
    
    self.checkType()

#...............................................................................
#EOF
