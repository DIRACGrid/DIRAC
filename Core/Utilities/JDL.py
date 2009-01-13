from DIRAC import S_OK, S_ERROR
from DIRAC.ConfigurationSystem.Client.CFG import CFG
from DIRAC.Core.Utilities import List

def loadJDLAsCFG( jdl ):
  """
  Load a JDL as CFG
  """
  def cleanValue( value ):
    value = value.strip()
    if value[0] == '"':
      if value[-1] == '"':
        return value[1:-1]
    else:
      return value

  def assignValue( key, value, cfg ):
    key = key.strip()
    if len( key ) == 0:
      return S_ERROR( "Invalid key name" )
    value = value.strip()
    if value[0] == "{":
      if value[-1 ] != "}":
        return S_ERROR( "Value '%s' seems a list but does not end in '}'" % ( value ) )
      valList = List.fromChar( value[1:-1] )
      for i in range( len( valList ) ):
        valList[i] = cleanValue( valList[i] )
        if valList[ i ] == None:
          return S_ERROR( "List value '%s' seems invalid for item %s" % ( value, i ) )
      value = ", ".join( valList )
    else:
      nV = cleanValue( value )
      if nV == None:
        return S_ERROR( "Value '%s seems invalid" % ( value ) )
      value = nV
    cfg.setOption( key, value )
    return S_OK()

  if jdl[ 0 ] == "[":
    iPos = 1
  else:
    iPos = 0
  key = ""
  value = ""
  action = "key"
  cfg = CFG()
  while iPos < len( jdl ):
    c = jdl[ iPos ]
    if c == ";":
      if key.strip():
        result = assignValue( key, value, cfg )
        if not result[ 'OK' ]:
          return result
      key = ""
      value = ""
      action = "key"
    elif c == "[":
      key = key.strip()
      if not key:
        return S_ERROR( "Invalid key" )
      result = loadJDLAsCFG( jdl[ iPos: ] )
      if not result[ 'OK' ]:
        return result
      subCfg, subPos = result[ 'Value' ]
      cfg.createNewSection( key, contents = subCfg )
      key = ""
      value = ""
      action = "key"
      iPos += subPos
    elif c == "=":
      if action == "key":
        action = "value"
      else:
        value += c
    elif c == "]":
      key = key.strip()
      if len( key ) > 0:
        result = assignValue( key, value, cfg )
        if not result[ 'OK' ]:
          return result
      return S_OK( ( cfg, iPos ) )
    else:
      if action == "key":
        key += c
      else:
        value += c
    iPos += 1

  return S_OK( ( cfg, iPos ) )

def dumpCFGAsJDL( cfg, level = 1, tab = "  " ):
  indent = tab * level
  contents = [ "%s[" % ( tab * ( level - 1 ) ) ]
  sections = cfg.listSections()

  for key in cfg:
    if key in sections:
      contents.append( "%s%s =" % ( indent, key ) )
      contents.append( "%s;" % dumpCFGAsJDL( cfg[ key ], level + 1, tab ) )
    else:
      val = List.fromChar( cfg[ key ] )
      if len( val ) < 2:
        v = cfg[ key ]
        try:
          d = float( v )
          contents.append( '%s%s = %s;' % ( tab*level, key, v ) )
        except:
          contents.append( '%s%s = "%s";' % ( tab*level, key, v ) )
      else:
        contents.append( "%s%s =" % ( indent, key ) )
        contents.append( "%s{" % indent )
        for iPos in range( len( val ) ):
          try:
            d = float( val[iPos] )
          except:
            val[ iPos ] = '"%s"' % val[ iPos ]
        contents.append( ",\n".join( [ '%s%s' % ( tab * (level +1 ), v ) for v in val ] ) )
        contents.append( "%s};" % indent )
  contents.append( "%s]" % ( tab * ( level - 1 ) ) )
  return "\n".join( contents )