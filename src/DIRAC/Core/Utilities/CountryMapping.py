"""  The CountryMapping module performs the necessary CS gymnastics to resolve country codes  """
from DIRAC import gConfig, S_OK, S_ERROR


def getCountryMapping(country):
    """Determines the associated country from the country code"""
    mappedCountries = [country]
    while True:
        mappedCountry = gConfig.getValue(f"/Resources/Countries/{country}/AssignedTo", country)
        if mappedCountry == country:
            break
        elif mappedCountry in mappedCountries:
            return S_ERROR(f"Circular mapping detected for {country}")
        else:
            country = mappedCountry
            mappedCountries.append(mappedCountry)
    return S_OK(mappedCountry)


def getCountryMappingTier1(country):
    """Returns the Tier1 site mapped to a country code"""
    res = getCountryMapping(country)
    if not res["OK"]:
        return res
    mappedCountry = res["Value"]
    tier1 = gConfig.getValue(f"/Resources/Countries/{mappedCountry}/Tier1", "")
    if not tier1:
        return S_ERROR(f"No Tier1 assigned to {mappedCountry}")
    return S_OK(tier1)
