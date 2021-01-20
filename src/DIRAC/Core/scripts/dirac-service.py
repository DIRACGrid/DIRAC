#!/usr/bin/env python
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

# pylint fails as it doesn't realise this file is only used as a script
import dirac_service  # pylint: disable=import-error

print("NOTE:", __file__, "is deprecated and will be removed in v7r3, for details see",
      "https://github.com/DIRACGrid/DIRAC/wiki/DIRAC-v7r2#rename-of-scripts")


if __name__ == "__main__":
  dirac_service.main()
