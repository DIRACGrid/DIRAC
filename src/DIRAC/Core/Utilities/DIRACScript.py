from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

from collections import defaultdict
import functools

import six


class DIRACScript(object):
  """Decorator for providing command line executables

  All console-scripts entrypoints in DIRAC and downstream extensions should be
  wrapped in this decorator to allow extensions to override any entry_point.
  """
  def __init__(self):
    """ c'tor
    """
    pass

  def __call__(self, func=None):
    """Set the wrapped function or call the script

    This function is either called with a decorator or directly to call the
    underlying function. When running with Python 2 the raw function will always
    be called however in Python 3 the priorities will be applied from the
    dirac.extension_metadata entry_point.
    """
    # If func is provided then the decorator is being applied to a function
    if func is not None:
      self._func = func
      return functools.wraps(func)(self)

    # Setuptools based installations aren't supported with Python 2
    if six.PY2:
      return self._func()  # pylint: disable=not-callable

    # This is only available in Python 3.8+ so it has to be here for now
    from importlib import metadata  # pylint: disable=no-name-in-module

    # Iterate through all known entry_points looking for DIRACScripts
    matches = defaultdict(list)
    function_name = None
    for entrypoint in metadata.entry_points()['console_scripts']:
      if not entrypoint.name.startswith("dirac-"):
        continue
      entrypointFunc = entrypoint.load()
      if not isinstance(entrypointFunc, DIRACScript):
        raise ImportError(
            "Invalid dirac- console_scripts entry_point: " + repr(entrypoint) + "\n" +
            "All dirac- console_scripts should be wrapped in the DiracScript " +
            "decorator to ensure extension overlays are applied correctly."
        )
      matches[entrypoint.name].append(entrypoint)
      # If the function is self then we've found the currently called function
      if entrypointFunc is self:
        function_name = entrypoint.name

    if function_name is None:
      # TODO: This should an error once the integration tests modified to use pip install
      return self._func()  # pylint: disable=not-callable
      # raise NotImplementedError("Something is very wrong")

    # Call the entry_point from the extension with the highest priority
    rankedExtensions = _extensionsByPriority()
    entrypoint = max(
        matches[function_name],
        key=lambda e: rankedExtensions.index(_entrypointToExtension(e)),
    )

    return entrypoint.load()._func()


def _entrypointToExtension(entrypoint):
  """"Get the extension name from an EntryPoint object"""
  # In Python 3.9 this can be "entrypoint.module"
  module = entrypoint.pattern.match(entrypoint.value).groupdict()["module"]
  extensionName = module.split(".")[0]
  return extensionName


def _extensionsByPriority():
  """Discover extensions using the setuptools metadata

  TODO: This should move into a function which can also be called to fill the CS
  """
  # This is only available in Python 3.8+ so it has to be here for now
  from importlib import metadata  # pylint: disable=no-name-in-module

  priorties = defaultdict(list)
  for entrypoint in metadata.entry_points()['dirac']:
    extensionName = _entrypointToExtension(entrypoint)
    extension_metadata = entrypoint.load()()
    priorties[extension_metadata["priority"]].append(extensionName)

  extensions = []
  for priority, extensionNames in sorted(priorties.items()):
    if len(extensionNames) != 1:
      print(
          "WARNING: Found multiple extensions with priority",
          "{} ({})".format(priority, extensionNames),
      )
    # If multiple are passed, sort the extensions so things are deterministic at least
    extensions.extend(sorted(extensionNames))
  return extensions


def _getExtensionMetadata(extensionName):
  """Get the metadata for a given extension name"""
  # This is only available in Python 3.8+ so it has to be here for now
  from importlib import metadata  # pylint: disable=no-name-in-module

  for entrypoint in metadata.entry_points()['dirac']:
    if extensionName == _entrypointToExtension(entrypoint):
      return entrypoint.load()()
