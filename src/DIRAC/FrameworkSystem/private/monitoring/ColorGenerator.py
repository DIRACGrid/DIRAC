from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"


class ColorGenerator:

  sHexDigits = "0123456789ABCDEF"

  def __init__(self):
    self.lUsedColors = []

  def __equalColors(self, c1, c2):
    return c1[0] == c2[0] and c1[1] == c2[1] and c1[2] == c2[2]

  def __equalColorInList(self, c1, cl2):
    for c2 in cl2:
      if self.__equalColors(c1, c2):
        return True
    return False

  def __toHex(self, n):
    d1 = int(n / 16)
    d2 = (n - d1 * 16)
    return "%s%s" % (self.sHexDigits[d1], self.sHexDigits[d2])

  def reset(self):
    self.lUsedColors = []

  def __generateColor(self):
    iNumColorsUsed = len(self.lUsedColors)
    if iNumColorsUsed == 0:
      self.lUsedColors.append((0, 0, 255))
    elif iNumColorsUsed == 1:
      self.lUsedColors.append((0, 255, 0))
    elif iNumColorsUsed == 2:
      self.lUsedColors.append((255, 0, 0))
    elif iNumColorsUsed == 3:
      self.lUsedColors.append((0, 255, 255))
    elif iNumColorsUsed == 4:
      self.lUsedColors.append((255, 0, 255))
    elif iNumColorsUsed == 5:
      self.lUsedColors.append((255, 255, 0))
    else:
      im = 0
      iM = 1
      rC = self.lUsedColors[0]
      while self.__equalColorInList(rC, self.lUsedColors):
        stC1 = self.lUsedColors[im]
        stC2 = self.lUsedColors[iM]
        rC = ((stC1[0] + stC2[0]) / 2, (stC1[1] + stC2[1]) / 2, (stC1[2] + stC2[2]) / 2)
        if self.__equalColorInList(rC, self.lUsedColors):
          im += 1
          if im == iM:
            iM += 1
            im = 0
      self.lUsedColors.append(rC)
    fC = self.lUsedColors[iNumColorsUsed]
    return fC

  def getFloatColor(self):
    fC = self.__generateColor()
    return (float(fC[0]) / 255, float(fC[1]) / 255, float(fC[2]) / 255)

  def getHexColor(self):
    fC = self.__generateColor()
    return "%s%s%s" % (self.__toHex(fC[0]), self.__toHex(fC[1]), self.__toHex(fC[2]))
