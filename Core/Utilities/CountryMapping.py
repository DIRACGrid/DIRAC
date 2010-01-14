# $HeadURL: svn+ssh://svn.cern.ch/reps/dirac/DIRAC/trunk/DIRAC/Core/Utilities/CountryMapping.py $
__RCSID__ = "$Id: CountryMapping.py 18436 2009-11-20 14:49:10Z acsmith $"

"""  The CountryMapping module performs the necessary CS gymnastics to resolve country codes  """

import string,re
from DIRAC import gConfig, gLogger, S_OK, S_ERROR

def getCountryMapping(country):
  """ Determines the associated country from the country code"""
  mappedCountries = [country]
  while True:
    mappedCountry = gConfig.getValue('/Resources/Countries/%s/AssignedTo' % country, country)
    if mappedCountry == country:
      break
    elif mappedCountry in mappedCountries:
      return S_ERROR('Circular mapping detected for %s' % country)
    else:
      country = mappedCountry
      mappedCountries.append(mappedCountry)
  return S_OK(mappedCountry)

def getCountryMappingTier1(country):
  """ Returns the Tier1 site mapped to a country code """
  res = getCountryMapping(country)
  if not res['OK']:
    return res
  mappedCountry = res['Value']
  tier1 = gConfig.getValue('/Resources/Countries/%s/Tier1' % mappedCountry, '') 
  if not tier1:
    return S_ERROR("No Tier1 assigned to %s" % mappedCountry)
  return S_OK(tier1)
