# Consistency check

this script is here to help compare storage and DFC dumps.

## What you need

### SE definitions

A CSV file containing its name and base path. Like

```
CSCS-DST;/pnfs/lcg.cscs.ch/lhcb
CSCS_MC-DST;/pnfs/lcg.cscs.ch/lhcb
```

You can obtain it with something like

```python
from DIRAC import initialize
initialize()
from DIRAC import gConfig
from DIRAC.Resources.Storage.StorageElement import StorageElement

for se in gConfig.getSections("/Resources/StorageElements")["Value"]:
    print(f"{se};{list(StorageElement(se).storages.values())[0].basePath}")
```

### StorageElement dump

This is typically provided by the site, and we expect just a flat list of the files

```
/pnfs/lcg.cscs.ch/lhcb/generated/2013-07-07/fileeed071eb-1aa0-4d00-8775-79624737224e
/pnfs/lcg.cscs.ch/lhcb/generated/2013-07-10/fileed08b040-196c-46d9-b4d6-37d80cba27eb
/pnfs/lcg.cscs.ch/lhcb/lhcb/test/SAM/testfile-put-LHCb-Disk-1494915199-61e6d085bb84.txt
```

### Catalog dump(s)

Ideally, you should have two catalog dumps for the SE that you are concerned about: one before the SE dump, and one after. Having only one of the two only allows to get partial comparison

You could get it with a script like

```python
import sys
from datetime import datetime,timezone
from DIRAC import initialize
initialize()
from DIRAC import gConfig
from DIRAC.Resources.Catalog.FileCatalogClient import FileCatalogClient
dfc = FileCatalogClient()

# Something like LCG.CERN.ch
site_name = sys.argv[1]

ses = gConfig.getOption(f"/Resources/Sites/{site_name.split('.')[0]}/{site_name}/SE",[])["Value"]

timestamp = datetime.now(tz=timezone.utc).strftime("%Y%m%d_%H%M%S")
output_file = f"{site_name}_dfc_{timestamp}.dump"
print(f"Getting FC dump for {ses} in {output_file}")
res = dfc.getSEDump(ses, output_file)
print(res)
```


Or from a `BaseSE`

```python
#!/usr/bin/env python3

import sys
from datetime import datetime,timezone
from DIRAC import initialize
initialize()
from DIRAC import gConfig
from DIRAC.Resources.Catalog.FileCatalogClient import FileCatalogClient
dfc = FileCatalogClient()

# Something like RAL-ECHO
base_se_name = sys.argv[1]

ses = []
ses_data = gConfig.getOptionsDictRecursively(f"/Resources/StorageElements")["Value"]
for key, val in ses_data.items():
    try:
        if val['BaseSE'] == base_se_name:
            ses.append(key)
    except (KeyError, TypeError):
        pass

timestamp = datetime.now(tz=timezone.utc).strftime("%Y%m%d_%H%M%S")
output_file = f"{base_se_name}_dfc_{timestamp}.dump"
print(f"Getting FC dump for {ses} in {output_file}")
res = dfc.getSEDump(ses, output_file)
print(res)
```

## How it works

We look at the differences and the intersections between the dump of the old catalog, the new catalog, and the storage element.

For example, you find dark data by looking at files that are in the SE dump, but not in any of the catalog dump. Lost data is data that is in both catalog dump, but not in the SE dump.


| Old FC | New FC | SE | Status           |
|--------|--------|----|------------------|
| 0      | 0      | 1  | Dark data        |
| 0      | 1      | 0  | Very new         |
| 0      | 1      | 1  | New              |
| 1      | 0      | 0  | Deleted          |
| 1      | 0      | 1  | Recently deleted |
| 1      | 1      | 0  | Lost file        |
| 1      | 1      | 1  | OK               |

## How to use

Although you probably need DIRAC to be able to get the DFC dump or the SE config, you do not need DIRAC installed once you have all the `csv` files.
You will however need `pandas` and `typer`


The `consistency` script has 3 commands:
* `threeways`: do a proper comparison of 1 old DFC dump, one SE dump, one new DFC dump. Results are as good as it gets
* `possibly-dark-data`: Tries to find dark data but be careful of the result (see `help`).
* `possibly-lost-data`: Tries to find lost data but be careful of the result (see `help`).

In any case, you should check  the output with commands like `dirac-dms-replica-stats` or `dirac-dms-pfn-exists`.
