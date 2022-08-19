"""Helpers for working with extensions to DIRAC"""
import argparse
from collections import defaultdict
import fnmatch
from importlib.machinery import PathFinder
import functools
import importlib
import os
import pkgutil
import sys

import importlib_metadata as metadata
import importlib_resources


def iterateThenSort(func):
    """Convenience decorator the find* functions

    Converts a function that takes a python module into one which accepts one or
    modules/module names. The returned values are converted to a sorted list of
    unique values.
    """

    @functools.wraps(func)
    def newFunc(modules, *args, **kwargs):
        if not isinstance(modules, (list, tuple)):
            modules = [modules]

        results = set()
        for module in modules:
            if isinstance(module, str):
                module = importlib.import_module(module)
            results |= set(func(module, *args, **kwargs))
        return sorted(results)

    return newFunc


@iterateThenSort
def findSystems(module):
    """Find the systems for one or more DIRAC extension(s)

    :param list/str/module module: One or more Python modules or Python module names
    :returns: list of system names
    """
    return {x.name for x in _findSystems(module)}


def findAgents(modules):
    """Find the agents for one or more DIRAC extension(s)

    :param list/str/module module: One or more Python modules or Python module names
    :returns: list of tuples of the form (SystemName, AgentName)
    """
    return findModules(modules, "Agent", "*Agent")


def findExecutors(modules):
    """Find the executors for one or more DIRAC extension(s)

    :param list/str/module module: One or more Python modules or Python module names
    :returns: list of tuples of the form (SystemName, ExecutorName)
    """
    return findModules(modules, "Executor")


def findServices(modules):
    """Find the services for one or more DIRAC extension(s)

    :param list/str/module module: One or more Python modules or Python module names
    :returns: list of tuples of the form (SystemName, ServiceName)
    """
    return findModules(modules, "Service", "*Handler")


@iterateThenSort
def findDatabases(module):
    """Find the DB SQL schema defintions for one or more DIRAC extension(s)

    :param list/str/module module: One or more Python modules or Python module names
    :returns: list of tuples of the form (SystemName, dbSchemaFilename)
    """
    # This can be "fn.name" when DIRAC is Python 3 only
    return {(system.name, os.path.basename(str(fn))) for system, fn in _findFile(module, "DB", "*DB.sql")}


@iterateThenSort
def findModules(module, submoduleName, pattern="*"):
    """Find the direct submodules from one or more DIRAC extension(s) that match a pattern

    :param list/str/module module: One or more Python modules or Python module names
    :param str submoduleName: The submodule under ``module`` in which to look
    :param str pattern: A ``fnmatch``-style pattern that the submodule must match
    :returns: list of tuples of the form (SystemName, ServiceName)
    """
    for system in _findSystems(module):
        agentModule = PathFinder.find_spec(submoduleName, path=system.submodule_search_locations)
        if not agentModule:
            continue
        for _, name, _ in pkgutil.iter_modules(agentModule.submodule_search_locations):
            if fnmatch.fnmatch(name, pattern):
                yield system.name, name


def entrypointToExtension(entrypoint):
    """ "Get the extension name from an EntryPoint object"""
    # In Python 3.9 this can be "entrypoint.module"
    module = entrypoint.pattern.match(entrypoint.value).groupdict()["module"]
    return module.split(".")[0]


def extensionsByPriority():
    """Discover extensions using the setuptools metadata"""

    priorties = defaultdict(list)
    for entrypoint in set(metadata.entry_points().select(group="dirac")):
        extensionName = entrypointToExtension(entrypoint)
        extension_metadata = entrypoint.load()()
        priorties[extension_metadata["priority"]].append(extensionName)

    extensions = []
    for priority, extensionNames in sorted(priorties.items(), reverse=True):
        if len(extensionNames) != 1:
            print(
                "WARNING: Found multiple extensions with priority",
                f"{priority} ({extensionNames})",
                file=sys.stderr,
            )
        # If multiple are passed, sort the extensions so things are deterministic at least
        extensions.extend(sorted(extensionNames))
    return extensions


def getExtensionMetadata(extensionName):
    """Get the metadata for a given extension name"""
    for entrypoint in metadata.entry_points().select(group="dirac"):
        if extensionName == entrypointToExtension(entrypoint):
            return entrypoint.load()()


def recurseImport(modName, parentModule=None, hideExceptions=False):
    from DIRAC import S_OK, S_ERROR, gLogger

    if parentModule is not None:
        raise NotImplementedError(parentModule)
    try:
        return S_OK(importlib.import_module(modName))
    except ImportError as excp:
        # name of the module reported as not found
        notFoundModule = excp.name
        # We make sure that the module not found is the one we are trying to import,
        # and not for example a missing dependency in the handler we are trying to import
        if isinstance(excp, ModuleNotFoundError) and modName.startswith(notFoundModule):
            return S_OK()
        errMsg = "Can't load %s" % modName
        if not hideExceptions:
            gLogger.exception(errMsg)
        return S_ERROR(errMsg)


def _findSystems(module):
    """Implementation of _findSystems that returns a generator of system names"""
    for _, name, _ in pkgutil.iter_modules(module.__path__):
        if name.endswith("System"):
            yield PathFinder.find_spec(name, path=module.__path__)


def _findFile(module, submoduleName, pattern="*"):
    """Implementation of findDatabases"""
    for system in _findSystems(module):
        try:
            dbModule = importlib_resources.files(".".join([module.__name__, system.name, submoduleName]))
        except ImportError:
            continue
        for file in dbModule.iterdir():
            if fnmatch.fnmatch(file.name, pattern):
                yield system, file


def parseArgs():
    """CLI interface for use with the DIRAC integration tests"""
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(required=True, dest="function")
    defaultExtensions = extensionsByPriority()
    for func in [findSystems, findAgents, findExecutors, findServices, findDatabases]:
        subparser = subparsers.add_parser(func.__name__)
        subparser.add_argument("--extensions", nargs="+", default=defaultExtensions)
        subparser.set_defaults(func=func)
    args = parser.parse_args()
    # Get the result and print it
    extensions = [importlib.import_module(e) for e in args.extensions]
    for result in args.func(extensions):
        if not isinstance(result, str):
            result = " ".join(result)
        print(result)


if __name__ == "__main__":
    parseArgs()
