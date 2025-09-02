"""Tests for ContainerImageResolver."""
from pathlib import Path

import pytest

import DIRAC.Core.Utilities.ContainerImageResolver as cir


class FakeGConfig:
    """Minimal gConfig stand-in, so that tests never read the process wide configuration."""

    def __init__(self, values=None):
        self.values = values or {}

    def getValue(self, key, default=None):
        # Same contract as the real gConfig: an absent key yields the caller's default
        return self.values.get(key, default)


class RecordingLog:
    """Logger stand-in recording what the resolver reports."""

    def __init__(self):
        self.messages = []

    def _record(self, level, message, variableText=""):
        self.messages.append((level, f"{message} {variableText}".strip()))

    def error(self, message, variableText=""):
        self._record("error", message, variableText)

    def warn(self, message, variableText=""):
        self._record("warn", message, variableText)

    def debug(self, message, variableText=""):
        self._record("debug", message, variableText)

    def hasMessage(self, level, fragment):
        return any(level == lvl and fragment in text for lvl, text in self.messages)


@pytest.fixture(autouse=True)
def isolatedResolver(monkeypatch):
    """Isolate every test from the process wide configuration and warning cache."""
    monkeypatch.setattr(cir, "gConfig", FakeGConfig())
    cir._emittedMessages.clear()
    cir._resolvedPaths.clear()
    yield
    cir._emittedMessages.clear()
    cir._resolvedPaths.clear()


@pytest.fixture
def log():
    return RecordingLog()


@pytest.mark.parametrize(
    "arch,expected",
    [
        ("x86_64", "amd64"),
        ("amd64", "amd64"),
        ("aarch64", "arm64"),
        ("arm64", "arm64"),
        # Variants are published with a colon separator: arm:v7, arm:v6, arm64:v8
        ("armv7l", "arm:v7"),
        ("armv6l", "arm:v6"),
        ("i686", "386"),
        ("i386", "386"),
        ("ppc64le", "ppc64le"),
        ("s390x", "s390x"),
        ("riscv64", "riscv64"),
        # Unknown values are passed through lowercased: the repository publishes
        # symlinks for most uname names, so they may well resolve anyway
        ("weirdarch", "weirdarch"),
        ("WeirdArch", "weirdarch"),
        ("X86_64", "amd64"),
    ],
)
def test_normalizeArch(arch, expected):
    assert cir.normalizeArch(arch) == expected


@pytest.mark.parametrize(
    "arch,expected",
    [
        # DUCC publishes arm64 either plain or variant-qualified depending on the
        # manifest, and only symlinks one to the other when the plain name is free
        ("aarch64", ["arm64", "arm64:v8"]),
        ("arm64", ["arm64", "arm64:v8"]),
        ("armv7l", ["arm:v7", "arm"]),
        ("armv6l", ["arm:v6", "arm"]),
        ("armel", ["arm:v6", "arm"]),
        ("armhf", ["arm:v7", "arm"]),
        # Architectures published under a single name yield a single candidate
        ("x86_64", ["amd64"]),
        ("x86-64", ["amd64"]),
        ("i686", ["386"]),
        ("ppc64le", ["ppc64le"]),
        ("weirdarch", ["weirdarch"]),
    ],
)
def test_getArchCandidates(arch, expected):
    assert cir.getArchCandidates(arch) == expected


def test_normalizeArch_is_the_preferred_candidate():
    for arch in ("aarch64", "armv7l", "x86_64", "weirdarch"):
        assert cir.normalizeArch(arch) == cir.getArchCandidates(arch)[0]


def test_getMultiarchPaths_covers_every_candidate():
    paths = cir.getMultiarchPaths("alpine:latest", basePath="/base", arch="aarch64")
    assert paths == [Path("/base/arm64/alpine:latest"), Path("/base/arm64:v8/alpine:latest")]


def test_explicit_arch():
    paths = cir.getMultiarchPaths(
        "registry.hub.docker.com/library/alpine:latest",
        basePath="/cvmfs/unpacked.cern.ch/.multiarch",
        arch="x86_64",
    )
    assert paths == [Path("/cvmfs/unpacked.cern.ch/.multiarch/amd64/registry.hub.docker.com/library/alpine:latest")]


def test_arm_variant_path():
    paths = cir.getMultiarchPaths("alpine:latest", basePath="/base", arch="armv7l")
    assert paths == [Path("/base/arm:v7/alpine:latest"), Path("/base/arm/alpine:latest")]


def test_trailing_slash_stripped():
    paths = cir.getMultiarchPaths(
        # Note the image reference ending with "/"
        "registry.hub.docker.com/library/alpine:latest/",
        basePath="/cvmfs/unpacked.cern.ch/.multiarch",
        arch="x86_64",
    )
    assert paths == [Path("/cvmfs/unpacked.cern.ch/.multiarch/amd64/registry.hub.docker.com/library/alpine:latest")]


def test_base_with_trailing_slash():
    paths = cir.getMultiarchPaths(
        "alpine:latest",
        # Note the base path ending with "/"
        basePath="/cvmfs/unpacked.cern.ch/.multiarch/",
        arch="x86_64",
    )
    assert paths == [Path("/cvmfs/unpacked.cern.ch/.multiarch/amd64/alpine:latest")]


def test_absolute_image_ref_rejected(log):
    """An absolute reference would make pathlib drop the base path and the architecture."""
    paths = cir.getMultiarchPaths("/cvmfs/cernvm-prod.cern.ch/cvm4", basePath="/base", arch="x86_64", log=log)
    assert paths == []
    assert log.hasMessage("error", "must be a relative OCI reference")


def test_parent_traversal_image_ref_rejected(log):
    paths = cir.getMultiarchPaths("../../etc", basePath="/base", arch="x86_64", log=log)
    assert paths == []
    assert log.hasMessage("error", "must not contain '..'")


def test_undetectable_arch_rejected(log):
    """platform.machine() returns an empty string when the architecture is unknown."""
    paths = cir.getMultiarchPaths("alpine:latest", basePath="/base", arch="", log=log)
    assert paths == []
    assert log.hasMessage("error", "Cannot determine the machine architecture")


def _make_multiarch_image(tmp_path, arch, image_ref):
    """Helper to create a fake multiarch image directory."""
    image_dir = tmp_path / ".multiarch" / arch / image_ref
    image_dir.mkdir(parents=True)
    return image_dir


def _make_legacy_root(tmp_path, name="cvm4"):
    """Helper to create a fake legacy container root."""
    legacy = tmp_path / name
    legacy.mkdir(parents=True)
    return legacy


def test_multiarch_found_as_directory(tmp_path):
    expected = _make_multiarch_image(tmp_path, "amd64", "registry.hub.docker.com/library/alma9:latest")

    result = cir.resolveImagePath(
        imageRef="registry.hub.docker.com/library/alma9:latest",
        basePath=str(tmp_path / ".multiarch"),
        arch="x86_64",
    )
    assert result == expected


def test_multiarch_found_aarch64(tmp_path):
    expected = _make_multiarch_image(tmp_path, "arm64", "registry.hub.docker.com/library/alma9:latest")

    result = cir.resolveImagePath(
        imageRef="registry.hub.docker.com/library/alma9:latest",
        basePath=str(tmp_path / ".multiarch"),
        arch="aarch64",
    )
    assert result == expected


def test_multiarch_found_under_arm64_variant(tmp_path):
    """An image published only as arm64:v8 must still be found on an aarch64 node.

    DUCC only symlinks .multiarch/arm64 to arm64:v8 when arm64 does not already
    exist as a real directory, so on a repository holding both the variant form
    is reachable under that name alone.
    """
    (tmp_path / ".multiarch" / "arm64").mkdir(parents=True)  # real directory, without this image
    expected = _make_multiarch_image(tmp_path, "arm64:v8", "registry.hub.docker.com/library/alma9:latest")

    result = cir.resolveImagePath(
        imageRef="registry.hub.docker.com/library/alma9:latest",
        basePath=str(tmp_path / ".multiarch"),
        arch="aarch64",
    )
    assert result == expected


def test_multiarch_plain_arm64_preferred_over_variant(tmp_path):
    """When the image exists under both names, the plain one wins, as DUCC prefers it."""
    expected = _make_multiarch_image(tmp_path, "arm64", "registry.hub.docker.com/library/alma9:latest")
    _make_multiarch_image(tmp_path, "arm64:v8", "registry.hub.docker.com/library/alma9:latest")

    result = cir.resolveImagePath(
        imageRef="registry.hub.docker.com/library/alma9:latest",
        basePath=str(tmp_path / ".multiarch"),
        arch="aarch64",
    )
    assert result == expected


def test_multiarch_arm_variant_falls_back_to_plain_arm(tmp_path):
    """An image published as plain "arm" must be found on an armv7l node."""
    expected = _make_multiarch_image(tmp_path, "arm", "registry.hub.docker.com/library/alma9:latest")

    result = cir.resolveImagePath(
        imageRef="registry.hub.docker.com/library/alma9:latest",
        basePath=str(tmp_path / ".multiarch"),
        arch="armv7l",
    )
    assert result == expected


def test_fallback_warning_names_every_candidate(tmp_path, log):
    """The operator must be able to see exactly which paths were looked for."""
    legacy = _make_legacy_root(tmp_path)

    cir.resolveImagePath(
        imageRef="nonexistent:latest",
        basePath=str(tmp_path / ".multiarch"),
        containerRoot=str(legacy),
        arch="aarch64",
        log=log,
    )
    assert log.hasMessage("warn", "arm64/nonexistent:latest")
    assert log.hasMessage("warn", "arm64:v8/nonexistent:latest")


def test_fallback_warning_reports_an_unknown_variant_directory(tmp_path, log):
    """A variant directory this release does not know about must be named, not silently missed."""
    _make_multiarch_image(tmp_path, "arm64:v9", "registry.hub.docker.com/library/alma9:latest")
    legacy = _make_legacy_root(tmp_path)

    result = cir.resolveImagePath(
        imageRef="registry.hub.docker.com/library/alma9:latest",
        basePath=str(tmp_path / ".multiarch"),
        containerRoot=str(legacy),
        arch="aarch64",
        log=log,
    )
    # An unknown variant is reported, never used: variant compatibility is
    # directional, so we cannot know it runs here
    assert result is None
    assert log.hasMessage("warn", "The repository publishes arm64:v9 for architecture 'arm64'")


def test_fallback_warning_reports_nothing_published(tmp_path, log):
    _make_multiarch_image(tmp_path, "amd64", "registry.hub.docker.com/library/alma9:latest")
    legacy = _make_legacy_root(tmp_path)

    cir.resolveImagePath(
        imageRef="registry.hub.docker.com/library/alma9:latest",
        basePath=str(tmp_path / ".multiarch"),
        containerRoot=str(legacy),
        arch="aarch64",
        log=log,
    )
    assert log.hasMessage("warn", "The repository publishes nothing for architecture 'arm64'")


def test_base_path_not_listed_when_the_image_is_found(tmp_path, monkeypatch):
    """The diagnostic listing must cost nothing on the path where the image resolves."""
    calls = []
    monkeypatch.setattr(cir, "safe_listdir", lambda *args, **kwargs: calls.append(args) or [])
    expected = _make_multiarch_image(tmp_path, "amd64", "registry.hub.docker.com/library/alma9:latest")

    result = cir.resolveImagePath(
        imageRef="registry.hub.docker.com/library/alma9:latest",
        basePath=str(tmp_path / ".multiarch"),
        arch="x86_64",
    )
    assert result == expected
    assert calls == []


def test_published_dirs_are_reported_per_goarch(tmp_path, log):
    """An ARMv7 node is told about every arm directory, and about no other architecture."""
    base = tmp_path / ".multiarch"
    for name in ("arm", "arm:v6", "arm64", "amd64"):
        (base / name).mkdir(parents=True)
    legacy = _make_legacy_root(tmp_path)

    cir.resolveImagePath(
        imageRef="nonexistent:latest",
        basePath=str(base),
        containerRoot=str(legacy),
        arch="armv7l",
        log=log,
    )
    # "arm" and "arm:v6" share the GOARCH part, "arm64" and "amd64" do not
    assert log.hasMessage("warn", "The repository publishes arm, arm:v6 for architecture 'arm:v7'")


def test_successful_resolution_is_cached(tmp_path, monkeypatch):
    """The resolver runs once per payload: a pilot must not stat CVMFS every time."""
    expected = _make_multiarch_image(tmp_path, "amd64", "registry.hub.docker.com/library/alma9:latest")
    kwargs = dict(
        imageRef="registry.hub.docker.com/library/alma9:latest",
        basePath=str(tmp_path / ".multiarch"),
        arch="x86_64",
    )
    assert cir.resolveImagePath(**kwargs) == expected

    def boom(*args, **kwargs):
        raise AssertionError("the filesystem must not be touched again")

    monkeypatch.setattr(cir, "safe_exists", boom)
    monkeypatch.setattr(cir, "safe_listdir", boom)
    assert cir.resolveImagePath(**kwargs) == expected


def test_failed_resolution_is_not_cached(tmp_path):
    """A miss may be a transient mount problem, so it must not stick for the pilot's life."""
    kwargs = dict(
        imageRef="registry.hub.docker.com/library/alma9:latest",
        basePath=str(tmp_path / ".multiarch"),
        arch="x86_64",
        defaultRoot=None,
    )
    assert cir.resolveImagePath(**kwargs) is None

    expected = _make_multiarch_image(tmp_path, "amd64", "registry.hub.docker.com/library/alma9:latest")
    assert cir.resolveImagePath(**kwargs) == expected


def test_cache_distinguishes_configurations(tmp_path):
    """Two CEs with different image references must not share a cached answer."""
    alma = _make_multiarch_image(tmp_path, "amd64", "registry.hub.docker.com/library/alma9:latest")
    ubuntu = _make_multiarch_image(tmp_path, "amd64", "registry.hub.docker.com/library/ubuntu:24.04")
    base = str(tmp_path / ".multiarch")

    assert (
        cir.resolveImagePath(imageRef="registry.hub.docker.com/library/alma9:latest", basePath=base, arch="x86_64")
        == alma
    )
    assert (
        cir.resolveImagePath(imageRef="registry.hub.docker.com/library/ubuntu:24.04", basePath=base, arch="x86_64")
        == ubuntu
    )


def test_multiarch_preferred_over_legacy(tmp_path):
    """When both multiarch and legacy exist, multiarch wins."""
    multiarch = _make_multiarch_image(tmp_path, "amd64", "registry.hub.docker.com/library/alma9:latest")
    legacy = _make_legacy_root(tmp_path)

    result = cir.resolveImagePath(
        imageRef="registry.hub.docker.com/library/alma9:latest",
        basePath=str(tmp_path / ".multiarch"),
        containerRoot=str(legacy),
        arch="x86_64",
    )
    assert result == multiarch


def test_multiarch_not_found_falls_back_to_container_root(tmp_path, log):
    legacy = _make_legacy_root(tmp_path)

    result = cir.resolveImagePath(
        imageRef="nonexistent:latest",
        basePath=str(tmp_path / ".multiarch"),
        containerRoot=str(legacy),
        arch="x86_64",
        log=log,
    )
    assert result == legacy
    assert log.hasMessage("warn", "Falling back to legacy ContainerRoot")


def test_invalid_image_ref_falls_back_to_container_root(tmp_path, log):
    """An unusable ImageReference must not be silently resolved to itself."""
    legacy = _make_legacy_root(tmp_path)

    result = cir.resolveImagePath(
        imageRef=str(legacy),  # absolute: not a valid OCI reference
        basePath=str(tmp_path / ".multiarch"),
        containerRoot=str(legacy),
        arch="x86_64",
        log=log,
    )
    assert result == legacy
    assert log.hasMessage("error", "must be a relative OCI reference")
    assert log.hasMessage("warn", "Falling back to legacy ContainerRoot")


def test_legacy_on_foreign_arch_is_refused(tmp_path, log):
    """The legacy roots are amd64 images: an arm64 node must fail, not start a doomed payload."""
    legacy = _make_legacy_root(tmp_path)

    result = cir.resolveImagePath(
        imageRef="nonexistent:latest",
        basePath=str(tmp_path / ".multiarch"),
        containerRoot=str(legacy),
        arch="aarch64",
        log=log,
    )
    assert result is None
    assert log.hasMessage("error", "Legacy ContainerRoot cannot run on this node")
    # ...and the log must not claim a fallback that did not happen
    assert not log.hasMessage("warn", "Falling back to legacy ContainerRoot")


def test_refusal_is_logged_for_every_payload(tmp_path, log):
    """A condition that kills every payload must not be deduplicated away after the first."""
    legacy = _make_legacy_root(tmp_path)

    for _ in range(3):
        cir.resolveImagePath(
            imageRef="nonexistent:latest",
            basePath=str(tmp_path / ".multiarch"),
            containerRoot=str(legacy),
            arch="aarch64",
            log=log,
        )
    refusals = [text for lvl, text in log.messages if lvl == "error" and "cannot run on this node" in text]
    assert len(refusals) == 3


def test_legacy_on_unrecognised_arch_is_warned_but_used(tmp_path, log):
    """An unknown uname may still be a legacy-compatible node: warn, do not refuse."""
    legacy = _make_legacy_root(tmp_path)

    result = cir.resolveImagePath(
        imageRef="nonexistent:latest",
        basePath=str(tmp_path / ".multiarch"),
        containerRoot=str(legacy),
        arch="weirdarch",
        log=log,
    )
    assert result == legacy
    assert log.hasMessage("warn", "unrecognised architecture 'weirdarch'")


def test_legacy_on_native_arch_is_not_reported(tmp_path, log):
    legacy = _make_legacy_root(tmp_path)

    cir.resolveImagePath(
        imageRef="nonexistent:latest",
        basePath=str(tmp_path / ".multiarch"),
        containerRoot=str(legacy),
        arch="x86_64",
        log=log,
    )
    assert not log.hasMessage("warn", "single architecture image but this node is")


def test_multiarch_not_found_falls_back_to_config_container_root(tmp_path, monkeypatch, log):
    legacy = _make_legacy_root(tmp_path)
    monkeypatch.setattr(cir, "gConfig", FakeGConfig({"/Resources/Computing/Singularity/ContainerRoot": str(legacy)}))

    result = cir.resolveImagePath(
        imageRef="nonexistent:latest",
        basePath=str(tmp_path / ".multiarch"),
        arch="x86_64",
        log=log,
    )
    assert result == legacy


def test_no_image_ref_skips_multiarch_uses_legacy_with_deprecation(tmp_path, log):
    """When no ImageReference is configured, legacy works but is reported as deprecated."""
    legacy = _make_legacy_root(tmp_path)

    result = cir.resolveImagePath(containerRoot=str(legacy), arch="x86_64", log=log)
    assert result == legacy
    assert log.hasMessage("warn", "ContainerRoot is deprecated")


def test_deprecation_is_emitted_once_per_process(tmp_path, log):
    """The resolver runs on every payload: it must not spam the pilot log."""
    legacy = _make_legacy_root(tmp_path)

    cir.resolveImagePath(containerRoot=str(legacy), arch="x86_64", log=log)
    firstCount = len(log.messages)
    assert firstCount

    cir.resolveImagePath(containerRoot=str(legacy), arch="x86_64", log=log)
    assert len(log.messages) == firstCount


def test_no_image_ref_no_legacy_returns_none(tmp_path, log):
    """When nothing is configured and defaults don't exist, return None."""
    result = cir.resolveImagePath(
        containerRoot=str(tmp_path / "no_such_root"),
        arch="x86_64",
        log=log,
    )
    assert result is None
    assert log.hasMessage("error", "No container image could be resolved")


def test_default_root_can_be_disabled(tmp_path):
    """dirac-apptainer-exec must not silently land on the built-in CernVM image."""
    result = cir.resolveImagePath(arch="x86_64", defaultRoot=None)
    assert result is None


def test_config_provides_image_ref_and_base(tmp_path, monkeypatch):
    expected = _make_multiarch_image(tmp_path, "amd64", "registry.hub.docker.com/library/centos:7")
    monkeypatch.setattr(
        cir,
        "gConfig",
        FakeGConfig(
            {
                "/Resources/Computing/Singularity/ImageBasePath": str(tmp_path / ".multiarch"),
                "/Resources/Computing/Singularity/ImageReference": "registry.hub.docker.com/library/centos:7",
            }
        ),
    )

    result = cir.resolveImagePath(arch="x86_64")
    assert result == expected


def test_explicit_args_override_config(tmp_path, monkeypatch):
    expected = _make_multiarch_image(tmp_path, "amd64", "registry.hub.docker.com/library/myimage:v1")
    monkeypatch.setattr(
        cir,
        "gConfig",
        FakeGConfig(
            {
                "/Resources/Computing/Singularity/ImageBasePath": "/should/not/be/used",
                "/Resources/Computing/Singularity/ImageReference": "should_not_be_used:latest",
            }
        ),
    )

    result = cir.resolveImagePath(
        imageRef="registry.hub.docker.com/library/myimage:v1",
        basePath=str(tmp_path / ".multiarch"),
        arch="x86_64",
    )
    assert result == expected


def test_nothing_found_returns_none(tmp_path):
    result = cir.resolveImagePath(
        imageRef="nonexistent:latest",
        basePath=str(tmp_path / ".multiarch"),
        containerRoot=str(tmp_path / "no_such_root"),
        arch="x86_64",
    )
    assert result is None


def test_unresponsive_filesystem_does_not_block(tmp_path, monkeypatch, log):
    """A hanging CVMFS mount must not hang the job slot: safe_exists times out."""
    legacy = _make_legacy_root(tmp_path)
    monkeypatch.setattr(cir, "safe_exists", lambda path, timeout=None: None)

    result = cir.resolveImagePath(
        imageRef="alpine:latest",
        basePath=str(tmp_path / ".multiarch"),
        containerRoot=str(legacy),
        arch="x86_64",
        log=log,
    )
    assert result is None
    assert log.hasMessage("warn", "Timed out")


def test_findMultiarchImage_found(tmp_path):
    expected = _make_multiarch_image(tmp_path, "amd64", "alpine:latest")

    result = cir.findMultiarchImage("alpine:latest", basePath=str(tmp_path / ".multiarch"), arch="x86_64")
    assert result == expected


def test_findMultiarchImage_not_found(tmp_path):
    result = cir.findMultiarchImage("nonexistent:latest", basePath=str(tmp_path / ".multiarch"), arch="x86_64")
    assert result is None


def test_findMultiarchImage_never_falls_back_to_legacy(tmp_path, monkeypatch):
    """dirac-apptainer-exec -i must never silently run a different image."""
    legacy = _make_legacy_root(tmp_path)
    monkeypatch.setattr(cir, "gConfig", FakeGConfig({"/Resources/Computing/Singularity/ContainerRoot": str(legacy)}))

    result = cir.findMultiarchImage("nonexistent:latest", basePath=str(tmp_path / ".multiarch"), arch="x86_64")
    assert result is None


def test_findMultiarchImage_uses_configured_base_path(tmp_path, monkeypatch):
    expected = _make_multiarch_image(tmp_path, "amd64", "alpine:latest")
    monkeypatch.setattr(
        cir, "gConfig", FakeGConfig({"/Resources/Computing/Singularity/ImageBasePath": str(tmp_path / ".multiarch")})
    )

    result = cir.findMultiarchImage("alpine:latest", arch="x86_64")
    assert result == expected
