########################################################################
# $HeadURL $
# File: RequestUtils.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2013/04/16 13:13:20
########################################################################

""" :mod: RequestUtils 
    ==================
 
    .. module: RequestUtils
    :synopsis: utilities for RMS
    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    utilities for RMS

    TODO: not used, OBSOLETE

"""

__RCSID__ = "$Id $"

##
# @file RequestUtils.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2013/04/16 13:13:29
# @brief Definition of RequestUtils class.

## imports 

def escapeStr( aStr, len = 255 ):
  return str( aStr ).replace( "'", "\'" )[:len]
