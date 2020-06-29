from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

# len must be >= repPerFile
users = [ 'user0', 'user1', 'user2', 'user3', 'user4']
groups = [ 'group0', 'group1']
storageElements = ['se0', 'se1', 'se2', 'se3', 'se4', 'se5', 'se6', 'se7', 'se8', 'se9']
status = ['AprioriGood', 'Trash', 'Removing', 'Probing']

# UNTESTED OPTIONS
# The numbers are taken from the working script that generates 22M dirs, 73M files and 365M replicas

# How many sub directories at a given depth
# With a depth of 12, it makes 22 M directories
# 4(1 + 4( 1 + 4(1+4(1+4(1+4(1+4(1+4(1+4(1+4(1+4(1+4)))))))))) ) = 22 369 620
hierarchySize = [ 4 ] * 12

# In practice, we observed a different average depth for user files and production files
# and a different amount of files per directories for both use case.
# So we simulate it as well.

prodFileDepth = 6
prodFilesPerDir = 10000

userFileDepth = 12
userFilesPerDir = 2


# We generate a random number of replicas between mix and max
minReplicasPerFile = 3
maxReplicasPerFile = 7
