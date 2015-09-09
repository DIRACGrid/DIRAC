from pyparsing import infixNotation, opAssoc, Keyword, Word, alphas, alphanums, printables, Literal, Suppress

from DIRAC.Core.Utilities.ObjectLoader import ObjectLoader

import sys


# Just to print nicely
class bcolors( object ):
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'



class FCConditionParser(object):
  
  
  # Some characters are reserved for the grammar
  __forbidenChars = ( '[', ']', '!', '&', '|', '=' , ' ' )
  __allowedChars = ''.join( set( printables ) - set( __forbidenChars ) )

  # Defines the basic shape of a rule : pluginName=whateverThatWillBePassedToThePlugin
  __pluginOperand = Word( __allowedChars ) + Literal( '=' ) + Word( __allowedChars )


  # define classes to be built at parse time, as each matching
  # expression type is parsed

  # Binary operator base class
  class _BoolBinOp( object ):
    """ Abstract object to represent a binary operator

    """

    reprsymbol = None  # Sign to represent the boolean operation

    # This is the boolean operation to apply
    # Could be None, but it should be callable
    evalop = lambda _x : None

    def __init__( self, token ):
      """
           :param token : the token matching a binary operator
                         it is a list with only one element which itself is a list
                         [ [ Arg1, Operator, Arg2] ]
                         The arguments themselves can be of any type, but one need
                         to be able to evaluate them as bool
      """

      # Keep the two arguments
      self.args = token[0][0::2]

    def __str__( self ):
      """ String representation """

      sep = " %s " % self.reprsymbol
      return "(" + sep.join( map( str, self.args ) ) + ")"


    def __bool__( self ):
      """ Perform the evaluation of the boolean value"""
      return self.evalop( bool( a ) for a in self.args )

    def eval( self, **kwargs ):
      """ Perform the evaluation of the boolean value"""
      return self.evalop( a.eval( **kwargs ) for a in self.args )

    __nonzero__ = __bool__
    __repr__ = __str__


  class _BoolAnd( _BoolBinOp ):
    """ Represents the 'and' operator """

    reprsymbol = '&'
    evalop = all

  class _BoolOr( _BoolBinOp ):
    """ Represents the 'or' operator """
    reprsymbol = '|'
    evalop = any

  class _BoolNot( object ):
    """ Represents the "not" unitary operator """

    def __init__( self, t ):
      """
         :param token : the token matching a unitary operator
                       it is a list with only one element which itself is a list
                       [ [ !, Arg1] ]
                       The argument itself can be of any type, but one need
                      to be able to evaluate it as bool
      """

      # We just keep the argument
      self.arg = t[0][1]

    def __bool__( self ):
      """ Perform the evaluation of the boolean value"""
      return not bool( self.arg )

    def eval( self, **kwargs ):
      """ Perform the evaluation of the boolean value"""
      return not self.arg.eval( **kwargs )

    def __str__( self ):
      return "!" + str( self.arg )

    __repr__ = __str__
    __nonzero__ = __bool__


  # We can combine the pluginOperand with boolean expression,
  # and prioritized by squared bracket
  __boolExpr = infixNotation( __pluginOperand,
      [ ( '!', 1, opAssoc.RIGHT, _BoolNot ),
        ( '&', 2, opAssoc.LEFT, _BoolAnd ),
        ( '|', 2, opAssoc.LEFT, _BoolOr ),
      ],
      lpar = Suppress( '[' ),
      rpar = Suppress( ']' )
    )


  # Wrapper that will call the plugin
  class PluginOperand( object ):
    """ This class is a wrapper for a plugin
        and it's condition
        It is instantiated by pyparsing every time
        it encounters "plugin=condition"

    """

    def __init__( self, tokens, **kwargs ):
      """
          :param tokens [ pluginName, =, conditions ]
          :param kwargs contains all the information given to the plugin
                        namely the lfns
      """

      self.pluginName = tokens[0]
      self.conditions = tokens[2]

      # Load the plugin, and give it the condition
      objLoader = ObjectLoader()
      _class = objLoader.loadObject( 'Resources.Catalog.ConditionPlugins.%s' % self.pluginName, self.pluginName,
                                       hideExceptions = False )

      if not _class['OK']:
        raise Exception( _class['Message'] )


      self._pluginInst = _class['Value']( self.conditions )

      # call the plugin and evaluate it
      self.value = self._pluginInst.eval( **kwargs )

    def __bool__( self ):
      return self.value


    def eval( self, **kwargs ):
      return self._pluginInst.eval( **kwargs )

    def __str__( self ):
      return  self.pluginName

    __repr__ = __str__
    __nonzero__ = __bool__




  def testConditions( self, lfn, conditionString ):
    print bcolors.OKBLUE + "Testing %s against %s" % ( conditionString, lfn ) + bcolors.ENDC
  
    # Parse the various plugins and give as attribute the lfn, and whatever else (method name, catalog name...)
    self.__pluginOperand.setParseAction( lambda tokens : self.PluginOperand( tokens, lfn = lfn ) )
    # Parse all the condition and evaluate it
    res0 = self.__boolExpr.parseString( conditionString )
    print "type %s" % type( res0[0] )
    res = bool( res0[0] )
    res2 = res0[0].eval( lfn = lfn )
    print "%s" % ( bcolors.OKGREEN if res else bcolors.FAIL ) + "\t--> %s" % res + bcolors.ENDC
    print "%s" % ( bcolors.OKGREEN if res2 else bcolors.FAIL ) + "\t--> %s" % res2 + bcolors.ENDC

  def __call__(self, lfns, catalogName, operationName):
    tests = [ "[ FilenamePlugin=startswith('/lhcb') ] & ![ LenPlugin= >8 ] ",
              "FilenamePlugin=endswith('lfn')",
            ]
    lfns = [ '/nogood/lfn', '/lhcb/lfn', '/lhcb/a' ]
    for t in tests:
      for lfn in lfns:
        self.testConditions( lfn, t )
