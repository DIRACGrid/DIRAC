
import types


def MutableStruct( name, fields ):

  if type( fields ) in types.StringTypes:
    fields = [ f.strip() for f in fields.split( "," ) if f.strip() ]

  def sinit( self, *values ):
    for field, value in zip( fields, values ):
      self.__dict__[ field ] = value

  def srepr( self ):
    data = [ "%s=%s" % ( k, self.__dict__[ k ] ) for k in fields ]
    return '<MutableStruct:%s %s>' % ( name, " ".join( data ) )

  def sname( self ):
    return name

  def snonzero( self ):
    return True

  def slen( self ):
    return len( fields )

  def siter( self ):
    for f in fields:
      yield f

  def seq( self, other ):
    try:
      if not other._name() == name:
        return False
      for f in fields:
        if not getattr( other, f ) == getattr( self, f ):
          return False
    except:
      return False
    return True

  def sne( self, other ):
    return not self == other

  methods = { '__init__' : sinit,
              '__repr__' : srepr,
              '_name' : sname,
              '__len__' : slen,
              '__iter__' : siter,
              '__eq__' : seq,
              '__ne__' : sne,
              '__nonzero__' : snonzero
              }
  cls = type( name, (object,), methods )
  return cls


if __name__=="__main__":
  test = Struct( "test", [ 'a', 'b' ] )
  o1 = test( 1, 2 )
  o2 = test( 1, 2 )
  o3 = test( 1, 3 )
  assert o1.a == 1
  assert o2.b == 2
  assert o1 == o2
  assert o1 != o3
  assert o1
  print o1
  print o2
  print o3
