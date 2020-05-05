#!/bin/python

from __future__ import print_function
from __future__ import absolute_import
from __future__ import division
import os,commands,time,sys

print('**************************')
print('START myPythonScript.py')
print('**************************')
sys.stdout.flush()
time.sleep(30)
print('Hi this is a test')
print('hope it works...')
sys.stdout.flush()
root = os.getcwd()
print('we are here: ', root)
print('the files in this directory are:')
status,result = commands.getstatusoutput('ls -al')
print(result)
#time.sleep(80)
sys.stdout.flush()
print('trying to see the local environment:')
status,result = commands.getstatusoutput('env')
time.sleep(30)
print(result)
print('bye.')
print('**************************')
print('END myPythonScript.py')
print('**************************')


