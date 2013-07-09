# $HeadURL$
"""  The CountryMapping module performs the necessary CS gymnastics to resolve country codes  """
__RCSID__ = "$Id$"

from DIRAC import S_OK, S_ERROR
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations

def getCountryMapping( country ):
  """ Determines the associated country from the country code"""
  mappedCountries = [country]
  opsHelper = Operations()
  while True:
    mappedCountry = opsHelper.getValue( '/Countries/%s/AssignedTo' % country, country )
    if mappedCountry == country:
      break
    elif mappedCountry in mappedCountries:
      return S_ERROR( 'Circular mapping detected for %s' % country )
    else:
      country = mappedCountry
      mappedCountries.append( mappedCountry )
  return S_OK( mappedCountry )

def getCountryMappingTier1( country ):
  """ Returns the Tier1 site mapped to a country code """
  opsHelper = Operations()
  res = getCountryMapping( country )
  if not res['OK']:
    return res
  mappedCountry = res['Value']
  tier1 = opsHelper.getValue( '/Countries/%s/Tier1' % mappedCountry, '' )
  if not tier1:
    return S_ERROR( "No Tier1 assigned to %s" % mappedCountry )
  return S_OK( tier1 )
