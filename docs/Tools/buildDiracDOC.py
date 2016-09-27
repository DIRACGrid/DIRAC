# OLD ! ( Kept for a while )

##!/usr/bin/env python
#
#import urllib
#import os
#import tarfile
#import sys
#import shutil
#import getpass
#import tarfile
#import re
#from optparse import OptionParser
#
#
#def getDocSources(package, user, version):
#  """ Download sources for the Sphinx and Epydoc documenters
#  """
#
#  #Donwloading rst and conf files from SVN
#  os.system("svn export svn+ssh://" + user + "@svn.cern.ch/reps/dirac/Docs/trunk/Docs/DIRAC/source")
#  os.system("svn export svn+ssh://" + user + "@svn.cern.ch/reps/dirac/Docs/trunk/Docs/DIRAC/Makefile")
#
#  # If no code package documentation is requested, return
#  if not package:
#    return
#
#  #Download DIRAC from SVN, delete code that do not need to be documented
#  if version.lower() in ['head','trunk']:
#    os.system("svn export svn+ssh://" + user +"@svn.cern.ch/reps/dirac/DIRAC/trunk/DIRAC DIRAC")
#  else:  
#    os.system("svn export svn+ssh://" + user +"@svn.cern.ch/reps/dirac/DIRAC/tags/DIRAC/%s DIRAC" %version)
#
#  #os.rename('./%s' %version, './DIRAC')
#  try:
#    os.mkdir('./DIRAC-sources/')
#  except:
#    pass
#
#  for package in os.listdir('./DIRAC'):
#        
#    if package in ('.svn', 'cmt', 'versions.cfg'):
#      try:
#        shutil.rmtree( './DIRAC/%s' %( package ) )
#      except OSError:
#        os.remove( './DIRAC/%s' %( package ) )
#        
#    else:
#      try:
#        subPackages = os.listdir('./DIRAC/' + package)
#      except OSError:
#        continue
#    
#      for subPackage in subPackages:
#    
#        if package == 'Core':
#          if subPackage not in ('Utilities', 'scripts', '__init__.py'):
#            try:
#              shutil.rmtree( './DIRAC/%s/%s' %( package, subPackage ) )
#            except OSError:
#              os.remove( './DIRAC/%s/%s' %( package, subPackage ) )
#    
#        elif package == 'Interfaces':
#          if subPackage not in ('API', 'scripts', '__init__.py'):
#            try:
#              shutil.rmtree( './DIRAC/%s/%s' %( package, subPackage ) )
#            except OSError:
#              os.remove( './DIRAC/%s/%s' %( package, subPackage ) )
#    
#        else:
#          if subPackage not in ('Client', 'Service', 'scripts', '__init__.py'):
#            try:
#              shutil.rmtree( './DIRAC/%s/%s' %( package, subPackage ) )
#            except OSError:
#              os.remove( './DIRAC/%s/%s' %( package, subPackage ) )
#    
#  try:
#    os.mkdir('./DIRAC-sources/')
#  except:
#    pass
#    
#  for package in os.listdir('./DIRAC'):
#    try:
#      subPackages = os.listdir('./DIRAC/%s' %package)
#    except OSError:
#      continue
#    for subPackage in subPackages:
#      if subPackage in ('Client', 'Service', 'Utilities', 'API', 'scripts'):
#        try:
#          os.mkdir('./DIRAC-sources/%s/' %subPackage)
#        except:
#          pass
#        try:
#          shutil.move('./DIRAC/%s/%s/' %(package, subPackage), './DIRAC-sources/%s/%s/%s/' %(subPackage, package, subPackage) )
#        except:
#          pass
#        fileHandle = open('./DIRAC-sources/%s/__init__.py' %subPackage, 'w')
#        fileHandle.close()
#        fileHandle = open('./DIRAC-sources/%s/%s/__init__.py' %(subPackage, package), 'w')
#        fileHandle.close()
#        fileHandle = open('./DIRAC-sources/%s/%s/%s/__init__.py' %(subPackage, package, subPackage), 'w')
#        fileHandle.close()
#        
#  print "\n*** Configuration READY ***\n"
# 
##############################################################################
#
#def buildGuides():
#  """ Build various guide documentation invoking Sphinx
#  """
#  os.system('cd ' + home + '/ && make html')
#  os.system('cd ' + home + '/ && make latex && cd ' + home + '/build/latex/ && make all-pdf')
#  if os.path.exists(os.path.join(home,'source','diracindex.html')):
#    shutil.copyfile(os.path.join(home,'source','diracindex.html'),
#                    os.path.join(home,'build','html','diracindex.html') )
#  
#  print "\n*** Guide Documentation READY ***\n"
#  
##############################################################################  
#
#def buildCodeDocumentation():
#  """ Build selected codes documentation
#  """
#
#  try:
#    os.mkdir(home + '/build/html/CodeDocumentation/')
#  except:
#    pass
#
#  for source in ('Client', 'Service', 'API', 'Utilities', 'scripts'):
#    os.system('cd ' + home + '/ && epydoc -v --name CodeReference -o build/html/CodeDocumentation/%s/ %s' %( source, home + '/DIRAC-sources/%s/' %source ) )
#    if not os.path.exists('build/html/CodeDocumentation/%s/' %source):
#      os.makedirs('build/html/CodeDocumentation/%s/' %source)
#    shutil.copy('source/_static/epydoc.css', 'build/html/CodeDocumentation/%s/' %source)
#
#  print "\n*** Code Documentation READY ***\n"
#
##############################################################################
#
#def buildTarball():
#  """ build tarball
#  """
#  
#  tar = tarfile.open("DIRAC_Docs.tgz", "w:gz")
#  tar.add('build')
#  tar.add('source')
#  tar.close()
#  
#  for file in os.listdir('.'):
#    if file != 'DIRAC_Docs.tgz':
#      try:
#        os.remove(file)
#      except OSError:
#        shutil.rmtree(file)
#  
#
#  print "\n*** Tarball READY, rest cleaned ***\n"
#
##############################################################################
#
#if __name__ == '__main__':
#  
#  parser = OptionParser()
#  
#  usage = "usage: %prog [options] arg1 arg2"
#  parser = OptionParser(usage=usage)
#  
#  parser.add_option("-a", "--all", action="store_true", dest="all", 
#                    help="Do everything: get svn files, make RSTs, and compile", 
#                    default=False)
#  
#  parser.add_option("-c", "--compile", action="store_true", dest="compile", 
#                    help="Compile the rst (invoke sphinx)", default=True)
#  
#  parser.add_option("-t", "--tarball", action="store_true", dest="tarball", 
#                    help="Build a tarball os source and build dir", default=False)
#  
#  parser.add_option("-u", "--user", action="store", dest="user", 
#                    help="UserName (for SVN). If not given, the actual username will be used",  
#                    type="string")
#
#  parser.add_option("-p", "--package", action="store", dest="package", 
#                    help="Package for which you want to prepare the documentation ('All' for everything)",  
#                    type="string")
#
#  parser.add_option("-v", "--version", action="store", dest="version", 
#                    help="DIRAC version for which to build the doc (default HEAD)",  type="string",
#                    default='trunk')
#
#  (options, args) = parser.parse_args()
#
#  if options.all:
#    # Do everything you can imagine
#    options.package = 'All'
#    options.compile = True
#    options.tarball = True
#  
#  if options.compile is None:
#    parser.print_help()
#    exit() 
#
#  print "##############################"
#  print "###### DIRAC documenter ######"
#  print "##############################"
#  
#  if options.user is None:
#    options.user = getpass.getuser()
#  
#  home = os.getcwd()
#  
#  if options.compile:
#    if options.package and type(options.package) is not list:
#      options.package = [options.package]
#         
#    getDocSources(options.package, options.user, options.version)
#    buildGuides()    
#    if options.package:
#      buildCodeDocumentation()
#
#  if options.tarball:
#    buildTarball()
#    
##############################################################################
