"""Utilities to resolve container images for Apptainer/Singularity
based on machine architecture and CVMFS multiarch layout.
"""
from __future__ import annotations

import platform
from pathlib import Path, PurePosixPath

from DIRAC import gConfig, gLogger
from DIRAC.Core.Utilities.Os import safe_exists, safe_listdir

BASE_PATH_DEFAULT = "/cvmfs/unpacked.cern.ch/.multiarch"
# Legacy default used before multiarch support
CONTAINER_DEFROOT = "/cvmfs/cernvm-prod.cern.ch/cvm4"
# Architecture the legacy (single architecture) container roots are built for
LEGACY_ARCH = "amd64"
# Images live on lazily mounted CVMFS: never block a job slot forever on a stat()
EXISTS_TIMEOUT = 60

# Candidate directory names in the multiarch repository for a given
# platform.machine() value, in the order they should be tried.
#
# DUCC, the tool publishing unpacked.cern.ch, writes one directory per manifest
# entry named after the OCI image spec architecture (the GOARCH value), with the
# variant appended after a colon when the manifest declares one -- see
# GetNameWithArch() in ducc/lib/conversion.go. The same logical architecture can
# therefore appear under more than one name: an image whose manifest declares
# arm64 with no variant lands in "arm64", one declaring variant "v8" lands in
# "arm64:v8".
#
# DUCC also publishes alias symlinks for the usual uname spellings (aarch64,
# x86_64, x86-64, i386, armel, armhf), but only when the alias name does not
# already exist as a real directory, and pointing at the first of an ordered
# list of candidates -- see archAliases and createMultiarchAliasSymlinksWithLogger()
# in the same file. A repository holding both "arm64" and "arm64:v8" as real
# directories thus has no symlink bridging the two, and an image published under
# only one of them cannot be reached through the other. We mirror DUCC's own
# preference lists and try every candidate in turn.
#
# Unknown values are passed through unchanged, and stand a fair chance of
# resolving through one of those alias symlinks.
# See https://github.com/opencontainers/image-spec/blob/main/image-index.md
_OCI_ARCH_CANDIDATES = {
    "x86_64": ("amd64",),
    "x86-64": ("amd64",),
    "amd64": ("amd64",),
    "i386": ("386",),
    "i486": ("386",),
    "i586": ("386",),
    "i686": ("386",),
    "386": ("386",),
    # The OCI spec treats arm64 with no variant as equivalent to v8, and many
    # registries publish only the variant form
    "aarch64": ("arm64", "arm64:v8"),
    "arm64": ("arm64", "arm64:v8"),
    "armv7l": ("arm:v7", "arm"),
    "armhf": ("arm:v7", "arm"),
    "armv6l": ("arm:v6", "arm"),
    "armel": ("arm:v6", "arm"),
    "armv5tel": ("arm:v5", "arm"),
    "arm": ("arm",),
    "ppc64le": ("ppc64le",),
    "s390x": ("s390x",),
    "riscv64": ("riscv64",),
}

# The resolver runs on every single payload submission, so a misconfigured site
# would otherwise repeat the same advice once per payload. Configuration notes
# (a deprecated option, a fallback taken) are therefore emitted once per process.
# Failures are NOT routed through this: a condition that kills the payload must be
# logged every time it kills one, or only the first casualty leaves a trace.
_emittedMessages = set()

# The resolver is called once per payload, and its answer cannot change under a
# running pilot: the configuration is fixed and the CVMFS layout is not rewritten
# beneath it. Successful resolutions are therefore cached, so that a pilot stats
# CVMFS once instead of once per payload.
# Failures are deliberately NOT cached: a miss can be a transient mount problem,
# and retrying costs only the path that is already slow and already loud.
_resolvedPaths = {}


def getArchCandidates(arch: str) -> list[str]:
    """Return the multiarch directory names to try for ``arch``, in order.

    Accepts ``platform.machine()`` values (e.g. ``x86_64``, ``aarch64``) as well
    as OCI names (e.g. ``amd64``, ``arm64``).  More than one name is returned
    where the repository may publish the same architecture either plain or
    variant-qualified (e.g. ``arm64`` and ``arm64:v8``).  Unknown values yield a
    single candidate, as-is, lowercased.
    """
    arch = arch.lower()
    return list(_OCI_ARCH_CANDIDATES.get(arch, (arch,)))


def normalizeArch(arch: str) -> str:
    """Normalise an architecture string to its preferred multiarch directory name.

    This is the first of :func:`getArchCandidates`, and the name used to
    describe the node in log messages.
    """
    return getArchCandidates(arch)[0]


def _warnOnce(message: str, log) -> None:
    """Emit ``message`` at most once per process."""
    if message in _emittedMessages:
        return
    _emittedMessages.add(message)
    log.warn(message)


def _remember(cacheKey: tuple, path: Path) -> Path:
    """Cache a successful resolution and return it; see :data:`_resolvedPaths`."""
    _resolvedPaths[cacheKey] = path
    return path


def _isKnownIncompatibleArch(arch: str) -> bool:
    """True when this node is an architecture the legacy roots demonstrably cannot run.

    Only architectures this release recognises are judged. An unrecognised
    ``platform.machine()`` value may well be a spelling of the legacy architecture
    that we simply do not know, so it is warned about rather than refused: a node
    that works today must not stop working because its uname is unusual.
    """
    return bool(arch) and arch.lower() in _OCI_ARCH_CANDIDATES and normalizeArch(arch) != LEGACY_ARCH


def _isSafeImageRef(imageRef: str, log) -> bool:
    """Check that an image reference can be appended to the multiarch base path.

    An absolute reference would make ``pathlib`` discard both the base path and
    the architecture directory, and a reference containing ``..`` would escape
    the base path: in either case the resolved path is not the documented
    ``<basePath>/<arch>/<imageRef>`` and the architecture is silently ignored.
    """
    refPath = PurePosixPath(imageRef)
    if refPath.is_absolute():
        log.error(
            "ImageReference must be a relative OCI reference, not an absolute path",
            f"{imageRef} -- use ContainerRoot for a local image path",
        )
        return False
    if ".." in refPath.parts:
        log.error("ImageReference must not contain '..'", imageRef)
        return False
    return True


def getMultiarchPaths(imageRef: str, basePath: str, arch: str, log=None) -> list[Path]:
    """Build the candidate CVMFS multiarch paths for the given OCI image reference.

    :param imageRef: Full OCI reference (e.g. ``registry.hub.docker.com/library/alpine:latest``)
    :param basePath: Multiarch base directory (e.g. ``/cvmfs/unpacked.cern.ch/.multiarch``)
    :param arch: architecture to resolve for (e.g. ``x86_64``)
    :param log: optional logger, defaults to a sub-logger of ``gLogger``
    :returns: one path ``<basePath>/<ociArch>/<imageRef>`` per candidate
        directory name of ``arch``, preferred first, or an empty list if the
        inputs cannot yield a valid path

    Example::

        >>> getMultiarchPaths("registry.hub.docker.com/library/alpine:latest",
        ...                "/cvmfs/unpacked.cern.ch/.multiarch", arch="aarch64")
        [PosixPath('/cvmfs/unpacked.cern.ch/.multiarch/arm64/registry.hub.docker.com/library/alpine:latest'),
         PosixPath('/cvmfs/unpacked.cern.ch/.multiarch/arm64:v8/registry.hub.docker.com/library/alpine:latest')]
    """
    log = log or gLogger.getSubLogger("ContainerImageResolver")
    if not arch:
        log.error(
            "Cannot determine the machine architecture",
            "platform.machine() returned an empty value: skipping the multiarch lookup",
        )
        return []
    if not imageRef or not _isSafeImageRef(imageRef, log):
        return []
    return [Path(basePath) / candidate / imageRef for candidate in getArchCandidates(arch)]


def _existsOnCvmfs(path: Path, log) -> bool:
    """Existence check protected against an unresponsive CVMFS mount."""
    found = safe_exists(str(path), timeout=EXISTS_TIMEOUT)
    if found is None:
        log.warn(f"Timed out after {EXISTS_TIMEOUT}s while checking container image path, assuming absent", str(path))
        return False
    return found


def _findMultiarchImage(imageRef: str | None, basePath: str, arch: str, log) -> tuple[Path | None, list[Path]]:
    """Return the first existing multiarch path, and every candidate tried.

    The candidate list is returned even when nothing is found, so that callers
    can report exactly what was looked for.
    """
    candidates = getMultiarchPaths(imageRef, basePath, arch, log=log) if imageRef else []
    for candidate in candidates:
        if _existsOnCvmfs(candidate, log):
            log.debug("Resolved multiarch image path", str(candidate))
            return candidate, candidates
    return None, candidates


def _getPublishedArchDirs(basePath: str, arch: str, log) -> list[str]:
    """List the directory names the repository publishes for this architecture.

    Called only once the multiarch lookup has missed, so the extra directory
    listing costs nothing on the path where the image is found.  It exists so
    that a variant directory this release does not know about -- say a future
    ``arm64:v9`` -- is named in the log, instead of the resolver silently
    degrading to the legacy single architecture image.

    The match is on the GOARCH part of the name, so an ARMv7 node is told about
    ``arm``, ``arm:v6`` and ``arm:v7`` alike.
    """
    bases = {candidate.split(":", 1)[0] for candidate in getArchCandidates(arch)}
    entries = safe_listdir(basePath, timeout=EXISTS_TIMEOUT)
    if entries is None:
        log.warn(f"Timed out after {EXISTS_TIMEOUT}s while listing the multiarch base path", basePath)
        return []
    return sorted(entry for entry in entries if entry.split(":", 1)[0] in bases)


def _getPublishedArchSummary(published: list[str], arch: str) -> str:
    """One sentence describing what the repository holds for this architecture."""
    goarch = normalizeArch(arch)
    if published:
        return f"The repository publishes {', '.join(published)} for architecture '{goarch}'. "
    return f"The repository publishes nothing for architecture '{goarch}'. "


def findMultiarchImage(
    imageRef: str,
    basePath: str | None = None,
    arch: str | None = None,
    log=None,
) -> Path | None:
    """Resolve an OCI reference in the multiarch layout, with no legacy fallback.

    :param imageRef: OCI image reference
        (e.g. ``registry.hub.docker.com/library/alpine:latest``)
    :param basePath: Base directory for the multiarch layout, defaults to the
        ``ImageBasePath`` CS option and then to :data:`BASE_PATH_DEFAULT`
    :param arch: Override architecture (default: autodetect via ``platform.machine()``)
    :param log: optional logger, defaults to a sub-logger of ``gLogger``
    :returns: the image path if it exists, else ``None``
    """
    log = log or gLogger.getSubLogger("ContainerImageResolver")
    basePath = basePath or gConfig.getValue("/Resources/Computing/Singularity/ImageBasePath") or BASE_PATH_DEFAULT
    arch = (arch or platform.machine() or "").strip()

    found, _candidates = _findMultiarchImage(imageRef, basePath, arch, log)
    return found


def resolveImagePath(
    imageRef: str | None = None,
    basePath: str | None = None,
    containerRoot: str | None = None,
    arch: str | None = None,
    defaultRoot: str | None = CONTAINER_DEFROOT,
    log=None,
) -> Path | None:
    """Resolve the container image path to use for Apptainer/Singularity.

    Resolution order:

    1. **Multiarch** ``<basePath>/<arch>/<imageRef>`` -- only attempted when
       an ``imageRef`` is explicitly provided (via parameter or CS config).
    2. **Legacy** ``containerRoot`` parameter, ``ContainerRoot`` CS option, or
       ``defaultRoot`` -- deprecated, and logged as such when used.
    3. ``None`` if nothing is found.

    :param imageRef: OCI image reference
        (e.g. ``registry.hub.docker.com/library/alpine:latest``)
    :param basePath: Base directory for the multiarch layout
        (e.g. ``/cvmfs/unpacked.cern.ch/.multiarch``)
    :param containerRoot: Legacy container root path for backward compatibility
    :param arch: Override architecture (default: autodetect via ``platform.machine()``)
    :param defaultRoot: Built-in legacy root, used when neither ``containerRoot``
        nor the ``ContainerRoot`` CS option is set.  Pass ``None`` to require an
        explicit configuration.
    :param log: optional logger, defaults to a sub-logger of ``gLogger``
    :returns: resolved :class:`~pathlib.Path` or ``None``
    """
    log = log or gLogger.getSubLogger("ContainerImageResolver")

    imageRef = imageRef or gConfig.getValue("/Resources/Computing/Singularity/ImageReference") or None
    basePath = basePath or gConfig.getValue("/Resources/Computing/Singularity/ImageBasePath") or BASE_PATH_DEFAULT
    arch = (arch or platform.machine() or "").strip()

    legacyRoot = containerRoot or gConfig.getValue("/Resources/Computing/Singularity/ContainerRoot") or defaultRoot

    # Keyed on the effective inputs, never on the raw arguments: gConfig can be
    # refreshed under a running process, and a changed option must not be masked
    # by an entry cached from the previous value.
    cacheKey = (imageRef, basePath, legacyRoot, arch)
    if cacheKey in _resolvedPaths:
        return _resolvedPaths[cacheKey]

    # 1) Try the CVMFS multiarch paths (only if an image reference is configured)
    found, candidates = _findMultiarchImage(imageRef, basePath, arch, log)
    if found:
        return _remember(cacheKey, found)

    # The multiarch lookup missed. Report what the repository does publish for
    # this architecture: a directory name we do not know about is the one thing
    # that would otherwise degrade to the legacy image without saying why.
    published = _getPublishedArchDirs(basePath, arch, log) if candidates else []

    # 2) Fall back to the legacy ContainerRoot, resolved above
    if legacyRoot and _existsOnCvmfs(Path(legacyRoot), log):
        # The legacy roots are single architecture images, in practice amd64 ones.
        # On a node known not to be able to run one, returning it would start a
        # payload that dies with an exec format error -- so refuse instead, and
        # let the caller fail with a diagnosable error.
        incompatible = _isKnownIncompatibleArch(arch)

        if imageRef:
            # Multiarch was attempted but did not yield a usable image
            if candidates:
                tried = ", ".join(str(candidate) for candidate in candidates)
                detail = (
                    f"Multiarch image not found for architecture '{normalizeArch(arch)}' (tried {tried}). "
                    + _getPublishedArchSummary(published, arch)
                )
            else:
                detail = (
                    f"Could not build a multiarch path for ImageReference '{imageRef}' "
                    f"on architecture '{normalizeArch(arch) if arch else 'unknown'}'. "
                )
            if not incompatible:
                detail += f"Falling back to legacy ContainerRoot '{legacyRoot}'. "
            _warnOnce(detail + "Please verify your ImageReference and ImageBasePath settings.", log)
        elif not incompatible:
            # No ImageReference configured, pure legacy usage
            _warnOnce(
                f"Using legacy ContainerRoot '{legacyRoot}'. "
                "ContainerRoot is deprecated and will be removed in a future release. "
                "Please configure ImageReference and ImageBasePath for the multiarch layout.",
                log,
            )

        if incompatible:
            # Logged on every payload, not once per process: this kills every one of them
            log.error(
                "Legacy ContainerRoot cannot run on this node",
                f"'{legacyRoot}' is a single architecture ({LEGACY_ARCH}) image but this node is "
                f"'{normalizeArch(arch)}': the payload would fail with an exec format error. "
                "Publish the image for this architecture under ImageBasePath, or point ContainerRoot "
                "at an image built for it.",
            )
            return None

        if arch and normalizeArch(arch) != LEGACY_ARCH:
            # Unrecognised architecture: it may still be a legacy-compatible node,
            # so this is advice rather than a refusal
            _warnOnce(
                f"Legacy ContainerRoot '{legacyRoot}' is a single architecture ({LEGACY_ARCH}) image and this "
                f"node reports an unrecognised architecture '{arch}': the payload may fail with an exec "
                "format error. Publish the image for this architecture under ImageBasePath.",
                log,
            )
        return _remember(cacheKey, Path(legacyRoot))

    log.error(
        "No container image could be resolved",
        f"arch={normalizeArch(arch) if arch else 'unknown'}, "
        f"multiarch={', '.join(str(c) for c in candidates) or ('unusable ImageReference' if imageRef else 'not attempted')}, "
        f"published={', '.join(published) or 'none'}, "
        f"legacy={legacyRoot or 'not configured'}",
    )
    return None
