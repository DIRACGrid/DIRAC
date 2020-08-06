#!/usr/bin/env python
""" create rst files for documentation of DIRAC """
import os
import shutil
import socket
import sys
import logging
import glob


from diracdoctools.Utilities import writeLinesToFile, mkdir, makeLogger
from diracdoctools.Config import Configuration, CLParser as clparser


LOG = makeLogger('CodeReference')

# global used inside the CustomizedDocs modules
CUSTOMIZED_DOCSTRINGS = {}


class CLParser(clparser):
  """Extension to CLParser to also parse buildType."""

  def __init__(self):
    super(CLParser, self).__init__()
    self.log = LOG.getChild('CLParser')
    self.clean = False

    self.parser.add_argument('--buildType', action='store', default='full',
                             choices=['full', 'limited'],
                             help='Build full or limited code reference',
                             )

    self.parser.add_argument('--clean', action='store_true',
                             help='Remove rst files and exit',
                             )

  def parse(self):
    super(CLParser, self).parse()
    self.log.info('Parsing options')
    self.buildType = self.parsed.buildType
    self.clean = self.parsed.clean

  def optionDict(self):
    oDict = super(CLParser, self).optionDict()
    oDict['buildType'] = self.buildType
    oDict['clean'] = self.clean
    return oDict


class CodeReference(object):
  """Module to create rst files containing autodoc for sphinx."""

  def __init__(self, configFile='docs.conf'):
    self.config = Configuration(configFile, sections=['Code'])
    self.orgWorkingDir = os.getcwd()

  def end(self):
    """Make sure we are back in the original working directory."""
    LOG.info('Done with creating code reference')
    os.chdir(self.orgWorkingDir)

  def getCustomDocs(self):
    """Import the dynamically created docstrings from the files in CustomizedDocs.

    Use 'exec' to avoid a lot of relative import, pylint errors, etc.
    """
    customizedPath = os.path.join(self.config.code_customDocsPath, '*.py')
    LOG.info('Looking for custom strings in %s', customizedPath)
    for filename in glob.glob(customizedPath):
      LOG.info('Found customization: %s', filename)
      try:
        exec(open(filename).read(), globals())  # pylint: disable=exec-used
      except Exception as e:
        LOG.error('Failed to parse customdocs: %r', e)

  def mkPackageRst(self, filename, modulename, fullmodulename, subpackages=None, modules=None):
    """Make a rst file for module containing other modules."""
    if modulename == 'scripts':
      return
    else:
      modulefinal = modulename

    lines = []
    lines.append('%s' % modulefinal)
    lines.append('=' * len(modulefinal))
    lines.append('.. automodule:: %s ' % fullmodulename)
    lines.append('   :members:')
    lines.append('')

    if subpackages or modules:
      lines.append('.. toctree::')
      lines.append('   :maxdepth: 1')
      lines.append('')

    subpackages = [s for s in subpackages if not s.endswith(('scripts', ))]
    if subpackages:
      LOG.info('Module %r with subpackages: %r', fullmodulename, ', '.join(subpackages))
      lines.append('SubPackages')
      lines.append('...........')
      lines.append('')
      lines.append('.. toctree::')
      lines.append('   :maxdepth: 1')
      lines.append('')
      for package in sorted(subpackages):
        lines.append('   %s/%s_Module.rst' % (package, package.split('/')[-1]))
      lines.append('')

    # remove CLI etc. because we drop them earlier
    modules = [m for m in modules if not m.endswith('CLI') and '-' not in m]
    if modules:
      lines.append('Modules')
      lines.append('.......')
      lines.append('')
      lines.append('.. toctree::')
      lines.append('   :maxdepth: 1')
      lines.append('')
      for module in sorted(modules):
        lines.append('   %s.rst' % (module.split('/')[-1],))
      lines.append('')

    writeLinesToFile(filename, lines)

  def mkDummyRest(self, classname, fullclassname):
    """Create a dummy rst file for files that behave badly."""
    filename = classname + '.rst'

    lines = []
    lines.append('%s' % classname)
    lines.append('=' * len(classname))
    lines.append('')
    lines.append('.. py:module:: %s' % fullclassname)
    lines.append('')
    lines.append('This is an empty file, because we cannot parse this file correctly or it causes problems.')
    lines.append('Please look at the source code directly')
    writeLinesToFile(filename, lines)

  def mkModuleRst(self, classname, fullclassname, buildtype='full'):
    """Create rst file for module."""
    LOG.info('Creating rst file for %r, aka %r', classname, fullclassname)
    filename = classname + '.rst'

    lines = []
    lines.append('%s' % classname)
    lines.append('=' * len(classname))

    lines.append('.. automodule:: %s' % fullclassname)
    if buildtype == 'full':
      lines.append('   :members:')
      if classname not in self.config.code_noInherited:
        lines.append('   :inherited-members:')
      lines.append('   :undoc-members:')
      lines.append('   :show-inheritance:')
      if classname in self.config.code_privateMembers:
        lines.append('   :special-members:')
        lines.append('   :private-members:')
      else:
        lines.append('   :special-members: __init__')
      if classname.startswith('_'):
        lines.append('   :private-members:')

    if fullclassname in CUSTOMIZED_DOCSTRINGS:
      ds = CUSTOMIZED_DOCSTRINGS[fullclassname]
      if ds.replace:
        lines = ds.doc_string
      else:
        lines.append(ds.doc_string)

    writeLinesToFile(filename, lines)

  def getsubpackages(self, abspath, direc):
    """return list of subpackages with full path"""
    packages = []
    for dire in direc:
      if dire.lower() == 'test' or dire.lower() == 'tests' or '/test' in dire.lower():
        LOG.debug('Skipping test directory: %s/%s', abspath, dire)
        continue
      if dire.lower() == 'docs' or '/docs' in dire.lower():
        LOG.debug('Skipping docs directory: %s/%s', abspath, dire)
        continue
      if os.path.exists(os.path.join(self.config.sourcePath, abspath, dire, '__init__.py')):
        packages.append(os.path.join(dire))
    return packages

  def getmodules(self, abspath, _direc, files):
    """Return list of subpackages with full path."""
    packages = []
    for filename in files:
      if filename.lower().startswith('test') or filename.lower().endswith('test') or \
         any(f.lower() in filename.lower() for f in self.config.code_ignoreFiles):
        LOG.debug('Skipping file: %s/%s', abspath, filename)
        continue
      if 'test' in filename.lower():
        LOG.warn("File contains 'test', but is kept: %s/%s", abspath, filename)

      if filename != '__init__.py':
        packages.append(filename.split('.py')[0])

    return packages

  def cleanDoc(self):
    """Remove the code output folder."""
    LOG.info('Removing existing code documentation: %r', self.config.code_targetPath)
    if os.path.exists(self.config.code_targetPath):
      shutil.rmtree(self.config.code_targetPath)

  def createDoc(self, buildtype="full"):
    """create the rst files for all the things we want them for"""
    LOG.info('self.config.sourcePath: %s', self.config.sourcePath)
    LOG.info('self.config.targetPath: %s', self.config.code_targetPath)
    LOG.info('Host: %s', socket.gethostname())

    # we need to replace existing rst files so we can decide how much code-doc to create
    if os.path.exists(self.config.code_targetPath) and os.environ.get('READTHEDOCS', 'False') == 'True':
      self.cleanDoc()
    mkdir(self.config.code_targetPath)
    os.chdir(self.config.code_targetPath)

    self.getCustomDocs()

    LOG.info('Now creating rst files: starting in %r', self.config.sourcePath)
    firstModule = True
    for root, direc, files in os.walk(self.config.sourcePath):
      configTemplate = [os.path.join(root, _) for _ in files if _ == 'ConfigTemplate.cfg']
      files = [_ for _ in files if _.endswith('.py')]

      if '__init__.py' not in files:
        continue

      elif any(f.lower() in root.lower() for f in self.config.code_ignoreFolders):
        LOG.debug('Skipping folder: %s', root)
        continue

      modulename = root.split('/')[-1].strip('.')
      codePath = root.split(self.config.sourcePath)[1].strip('/.')
      docPath = codePath
      if docPath.startswith(self.config.moduleName):
        docPath = docPath[len(self.config.moduleName) + 1:]
      fullmodulename = '.'.join(codePath.split('/')).strip('.')
      if not fullmodulename.startswith(self.config.moduleName):
        fullmodulename = ('.'.join([self.config.moduleName, fullmodulename])).strip('.')
      packages = self.getsubpackages(codePath, direc)
      if docPath:
        LOG.debug('Trying to create folder: %s', docPath)
        mkdir(docPath)
        os.chdir(docPath)
      if firstModule:
        firstModule = False
        self.createCodeDocIndex(
            subpackages=packages,
            modules=self.getmodules(
                codePath,
                direc,
                files),
            buildtype=buildtype)
      elif buildtype == 'limited':
        os.chdir(self.config.code_targetPath)
        return 0
      else:
        self.mkPackageRst(
            modulename + '_Module.rst',
            modulename,
            fullmodulename,
            subpackages=packages,
            modules=self.getmodules(
                docPath,
                direc,
                files))

      for filename in files:
        # Skip things that call parseCommandLine or similar issues
        fullclassname = '.'.join(docPath.split('/') + [filename])
        if not fullclassname.startswith(self.config.moduleName):
          fullclassname = '.'.join([self.config.moduleName, fullclassname])
        if any(f in filename for f in self.config.code_dummyFiles):
          LOG.debug('Creating dummy for  file %r', filename)
          self.mkDummyRest(filename.split('.py')[0], fullclassname.split('.py')[0])
          continue
        elif not filename.endswith('.py') or \
                filename.endswith('CLI.py') or \
                filename.lower().startswith('test') or \
                filename == '__init__.py' or \
                any(f in filename for f in self.config.code_ignoreFiles) or \
                '-' in filename:  # not valid python identifier, e.g. dirac-pilot
          LOG.debug('Ignoring file %r', filename)
          continue

        self.mkModuleRst(filename.split('.py')[0], fullclassname.split('.py')[0], buildtype)

      # copy configTemplate files to code doc so we can import them in the agent docstrings
      if configTemplate:
        shutil.copy(configTemplate[0], os.path.join(self.config.code_targetPath, docPath))

      os.chdir(self.config.code_targetPath)

    return 0

  def createCodeDocIndex(self, subpackages, modules, buildtype="full"):
    """create the main index file"""
    LOG.info('Creating base index file')
    filename = 'index.rst'
    lines = []
    lines.append('.. _code_documentation:')
    lines.append('')
    lines.append('Code Documentation (|release|)')
    lines.append('------------------------------')

    # for limited builds we only create the most basic code documentation so
    # we let users know there is more elsewhere
    if buildtype == 'limited':
      lines.append('')
      lines.append('.. warning::')
      lines.append(
          '  This a limited build of the code documentation, for the full code documentation '
          'please look at the website')
      lines.append('')
    else:
      if subpackages or modules:
        lines.append('.. toctree::')
        lines.append('   :maxdepth: 1')
        lines.append('')

      if subpackages:
        systemPackages = sorted([pck for pck in subpackages if pck.endswith('System')])
        otherPackages = sorted([pck for pck in subpackages if not pck.endswith('System')])

        lines.append('=======')
        lines.append('Systems')
        lines.append('=======')
        lines.append('')
        lines.append('.. toctree::')
        lines.append('   :maxdepth: 1')
        lines.append('')
        for package in systemPackages:
          lines.append('   %s/%s_Module.rst' % (package, package.split('/')[-1]))

        lines.append('')
        lines.append('=====')
        lines.append('Other')
        lines.append('=====')
        lines.append('')
        lines.append('.. toctree::')
        lines.append('   :maxdepth: 1')
        lines.append('')
        for package in otherPackages:
          lines.append('   %s/%s_Module.rst' % (package, package.split('/')[-1]))

      if modules:
        for module in sorted(modules):
          lines.append('   %s.rst' % (module.split('/')[-1],))

    writeLinesToFile(filename, lines)

  def checkBuildTypeAndRun(self, buildType='full'):
    """Check for input argument and then create the doc rst files."""
    buildTypes = ('full', 'limited')
    if buildType not in buildTypes:
      LOG.error('Unknown build type: %s use %s ', buildType, ' '.join(buildTypes))
      return 1
    LOG.info('Buildtype: %s', buildType)
    return self.createDoc(buildType)


def run(configFile='docs.conf', logLevel=logging.INFO, debug=False, buildType='full', clean=False):
  """Create the code reference.

  :param str configFile: path to the configFile
  :param logLevel: logging level to use
  :param bool debug: if true even more debug information is printed
  :param str buildType: 'full' or 'limited', use limited only when memory is limited
  :param bool clean: Remove rst files and exit
  :returns: return value 1 or 0
  """
  try:
    logging.getLogger().setLevel(logLevel)
    code = CodeReference(configFile=configFile)
    if clean:
      code.cleanDoc()
      return 0
    retVal = code.checkBuildTypeAndRun(buildType=buildType)
    code.end()
    return retVal
  except ImportError:
    return 1

if __name__ == '__main__':
  sys.exit(run(**(CLParser().optionDict())))
