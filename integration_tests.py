#!/usr/bin/env python
import fnmatch
import json
import os
import re
import shlex
import shutil
import subprocess
import sys
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager
from itertools import chain
from pathlib import Path
from typing import Optional

import git
import typer
import yaml
from packaging.version import Version
from typer import colors as c

# Editable configuration
DEFAULT_HOST_OS = "el9"
DEFAULT_MYSQL_VER = "mysql:8.4.4"
DEFAULT_ES_VER = "opensearchproject/opensearch:2.18.0"
# In MacOSX with Arm (MX), there's an issue with opensearch
# You *must* set the ES_PLATFORM flag to `linux/arm64` to make it work.
DEFAULT_ES_PLATFORM = "linux/amd64"
DEFAULT_IAM_VER = "indigoiam/iam-login-service:v1.10.2"
FEATURE_VARIABLES = {
    "DIRACOSVER": "master",
    "DIRACOS_TARBALL_PATH": None,
    "TEST_HTTPS": "No",
    "TEST_DIRACX": "No",
    "DIRAC_FEWER_CFG_LOCKS": None,
    "DIRAC_USE_JSON_ENCODE": None,
    "INSTALLATION_BRANCH": "",
    "DEBUG": "Yes",
}
DEFAULT_MODULES = {
    "DIRAC": Path(__file__).parent.absolute(),
}
# All services that have a FutureClient, but we *explicitly* deactivate
# (for example if we did not finish to develop it)
DIRACX_DISABLED_SERVICES = [
    "WorkloadManagement/JobMonitoring",
]

# Static configuration
DB_USER = "Dirac"
DB_PASSWORD = "Dirac"
DB_ROOTUSER = "root"
DB_ROOTPWD = "password"
DB_HOST = "mysql"
DB_PORT = "3306"

IAM_INIT_CLIENT_ID = "admin-client-rw"
IAM_INIT_CLIENT_SECRET = "secret"
IAM_SIMPLE_CLIENT_NAME = "simple-client"
IAM_SIMPLE_USER = "jane_doe"
IAM_SIMPLE_PASSWORD = "password"
IAM_ADMIN_CLIENT_NAME = "admin-client"
IAM_ADMIN_USER = "admin"
IAM_ADMIN_PASSWORD = "password"
IAM_HOST = "iam-login-service"
IAM_PORT = "8080"

# Implementation details
LOG_LEVEL_MAP = {
    "ALWAYS": (c.BLACK, c.WHITE),
    "NOTICE": (None, c.MAGENTA),
    "INFO": (None, c.GREEN),
    "VERBOSE": (None, c.CYAN),
    "DEBUG": (None, c.BLUE),
    "WARN": (None, c.YELLOW),
    "ERROR": (None, c.RED),
    "FATAL": (c.RED, c.BLACK),
}
LOG_PATTERN = re.compile(r"^[\d\-]{10} [\d:]{8} UTC [^\s]+ ([A-Z]+):")

# In niche cases where we use MacOSX with Orbstack, some commands may not work with docker compose
# If you're in that case, set in your environment `export DOCKER_COMPOSE_CMD="docker-compose"`
DOCKER_COMPOSE_CMD = shlex.split(os.environ.get("DOCKER_COMPOSE_CMD", "docker compose"))


class NaturalOrderGroup(typer.core.TyperGroup):
    """Group for showing subcommands in the correct order"""

    def list_commands(self, ctx):
        return self.commands.keys()


app = typer.Typer(
    cls=NaturalOrderGroup,
    help=f"""Run the DIRAC integration tests.

A local DIRAC setup can be created and tested by running:

\b
  ./integration_tests.py create

This is equivalent to running:

\b
  ./integration_tests.py prepare-environment
  ./integration_tests.py install-server
  ./integration_tests.py install-client
  ./integration_tests.py install-pilot
  ./integration_tests.py test-server
  ./integration_tests.py test-client
  ./integration_tests.py test-pilot

The test setup can be shutdown using:

\b
  ./integration_tests.py destroy

See below for additional subcommands which are useful during local development.

## Features

The currently known features and their default values are:

\b
  HOST_OS: {DEFAULT_HOST_OS!r}
  MYSQL_VER: {DEFAULT_MYSQL_VER!r}
  ES_VER: {DEFAULT_ES_VER!r}
  IAM_VER: {DEFAULT_IAM_VER!r}
  {(os.linesep + '  ').join(['%s: %r' % x for x in FEATURE_VARIABLES.items()])}

All features can be prefixed with "SERVER_" or "CLIENT_" to limit their scope.

## Extensions

Integration tests can be ran for extensions to DIRAC by specifying the module
name and path such as:

\b
  ./integration_tests.py create --extra-module MyDIRAC=/path/to/MyDIRAC

This will modify the setup process based on the contents of
`MyDIRAC/tests/.dirac-ci-config.yaml`. See the Vanilla DIRAC file for the
available options.

## Command completion

Command completion of typer based scripts can be enabled by running:

  typer --install-completion

After restarting your terminal you command completion is available using:

  typer ./integration_tests.py run ...

## DiracX

If you want to activate DiracX, you have to set the flag TEST_DIRACX to "Yes".
It will search for legacy adapted services (services with a future client activated)
and do the necessary to make DIRAC work alongside DiracX.

To deactivate a legacy adapted service (to pass CI for example), you have to add it in
the `DIRACX_DISABLED_SERVICES` list. If you don't, the program will set this service to be used
with DiracX, and if it is badly adapted, errors will be raised.

> Note that you can provide a DiracX project (repository, branch) by building it and providing
the dist folder to the prepare-environment command.
""",
)


@app.command()
def create(
    flags: Optional[list[str]] = typer.Argument(None),
    editable: Optional[bool] = None,
    extra_module: Optional[list[str]] = None,
    diracx_dist_dir: Optional[str] = None,
    release_var: Optional[str] = None,
    run_server_tests: bool = True,
    run_client_tests: bool = True,
    run_pilot_tests: bool = True,
):
    """Start a local instance of the integration tests"""
    prepare_environment(flags, editable, extra_module, diracx_dist_dir, release_var)
    install_server()
    install_client()
    install_pilot()
    exit_code = 0
    if run_server_tests:
        try:
            test_server()
        except TestExit as e:
            exit_code += e.exit_code
        else:
            raise NotImplementedError()
    if run_client_tests:
        try:
            test_client()
        except TestExit as e:
            exit_code += e.exit_code
        else:
            raise NotImplementedError()
    if run_pilot_tests:
        try:
            test_pilot()
        except TestExit as e:
            exit_code += e.exit_code
        else:
            raise NotImplementedError()
    if exit_code != 0:
        typer.secho("One or more tests failed", err=True, fg=c.RED)
    raise typer.Exit(exit_code)


@app.command()
def destroy():
    """Destroy a local instance of the integration tests"""
    typer.secho("Shutting down and removing containers", err=True, fg=c.GREEN)
    with _gen_docker_compose(DEFAULT_MODULES) as docker_compose_fn:
        os.execvpe(
            DOCKER_COMPOSE_CMD[0],
            [*DOCKER_COMPOSE_CMD, "-f", docker_compose_fn, "down", "--remove-orphans", "-t", "0", "--volumes"],
            _make_env({}),
        )


@app.command()
def prepare_environment(
    flags: Optional[list[str]] = typer.Argument(None),
    editable: Optional[bool] = None,
    extra_module: Optional[list[str]] = None,
    diracx_dist_dir: Optional[str] = None,
    release_var: Optional[str] = None,
):
    """Prepare the local environment for installing DIRAC."""
    if extra_module is None:
        extra_module = []
    _check_containers_running(is_up=False)
    if editable is None:
        editable = sys.stdout.isatty()
        typer.secho(
            f"No value passed for --[no-]editable, automatically detected: {editable}",
            fg=c.YELLOW,
        )
    typer.echo("Preparing environment")

    modules = DEFAULT_MODULES | dict(f.split("=", 1) for f in extra_module)
    modules = {k: Path(v).absolute() for k, v in modules.items()}

    if not flags:
        flags = {}
    else:
        flags = dict(f.split("=", 1) for f in flags)
    docker_compose_env = _make_env(flags)
    server_flags = {}
    client_flags = {}
    pilot_flags = {}
    for key, value in flags.items():
        if key.startswith("SERVER_"):
            server_flags[key[len("SERVER_") :]] = value
        elif key.startswith("CLIENT_"):
            client_flags[key[len("CLIENT_") :]] = value
        elif key.startswith("PILOT_"):
            pilot_flags[key[len("PILOT_") :]] = value
        else:
            server_flags[key] = value
            client_flags[key] = value
            pilot_flags[key] = value
    server_config = _make_config(modules, server_flags, release_var, editable)
    client_config = _make_config(modules, client_flags, release_var, editable)
    pilot_config = _make_config(modules, pilot_flags, release_var, editable)

    # The dependencies of dirac-server and dirac-client will be automatically
    # started but we need to add manually all the extra services
    module_configs = _load_module_configs(modules)
    extra_services = list(chain(*[config["extra-services"] for config in module_configs.values()]))

    typer.secho("Running docker compose to create containers", fg=c.GREEN)
    with _gen_docker_compose(modules, diracx_dist_dir=diracx_dist_dir) as docker_compose_fn:
        subprocess.run(
            [*DOCKER_COMPOSE_CMD, "-f", docker_compose_fn, "up", "-d", "dirac-server", "dirac-client", "dirac-pilot"]
            + extra_services,
            check=True,
            env=docker_compose_env,
        )

    typer.secho("Creating users in server client and pilot containers", fg=c.GREEN)
    for container_name in ["server", "client", "pilot"]:
        if os.getuid() == 0:
            continue
        cmd = _build_docker_cmd(container_name, use_root=True, cwd="/")
        gid = str(os.getgid())
        uid = str(os.getuid())
        ret = subprocess.run(cmd + ["groupadd", "--gid", gid, "dirac"], check=False)
        if ret.returncode != 0:
            typer.secho(f"Failed to add group dirac with id={gid}", fg=c.YELLOW)
        subprocess.run(
            cmd
            + [
                "useradd",
                "--uid",
                uid,
                "--gid",
                gid,
                "-s",
                "/bin/bash",
                "-d",
                "/home/dirac",
                "dirac",
            ],
            check=True,
        )
        subprocess.run(cmd + ["chown", "dirac", "/home/dirac"], check=True)

    typer.secho("Creating MySQL user", fg=c.GREEN)
    mysql_command = "mariadb" if "mariadb" in docker_compose_env["MYSQL_VER"].lower() else "mysql"
    cmd = ["docker", "exec", "mysql", f"{mysql_command}", f"--password={DB_ROOTPWD}", "-e"]
    # It sometimes takes a while for MySQL to be ready so wait for a while if needed
    for _ in range(10):
        ret = subprocess.run(
            cmd + [f"CREATE USER '{DB_USER}'@'%' IDENTIFIED BY '{DB_PASSWORD}';"],
            check=False,
        )
        if ret.returncode == 0:
            break
        typer.secho("Failed to connect to MySQL, will retry in 10 seconds", fg=c.YELLOW)
        time.sleep(10)
    else:
        raise Exception(ret)
    subprocess.run(
        cmd + [f"CREATE USER '{DB_USER}'@'localhost' IDENTIFIED BY '{DB_PASSWORD}';"],
        check=True,
    )
    subprocess.run(
        cmd + [f"CREATE USER '{DB_USER}'@'mysql' IDENTIFIED BY '{DB_PASSWORD}';"],
        check=True,
    )

    _prepare_iam_instance()

    typer.secho("Copying files to containers", fg=c.GREEN)
    for name, config in [("server", server_config), ("client", client_config), ("pilot", pilot_config)]:
        if path := config.get("DIRACOS_TARBALL_PATH"):
            path = Path(path)
            config["DIRACOS_TARBALL_PATH"] = f"/{path.name}"
            subprocess.run(
                ["docker", "cp", str(path), f"{name}:/{config['DIRACOS_TARBALL_PATH']}"],
                check=True,
            )

        config_as_shell = _dict_to_shell(config)
        typer.secho(f"## {name.title()} config is:", fg=c.BRIGHT_WHITE, bg=c.BLACK)
        typer.secho(config_as_shell)

        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "CONFIG"
            path.write_text(config_as_shell)
            subprocess.run(
                ["docker", "cp", str(path), f"{name}:/home/dirac"],
                check=True,
            )

    for module_name, module_configs in _load_module_configs(modules).items():
        for command in module_configs.get("commands", {}).get("post-prepare", []):
            typer.secho(
                f"Running post-prepare command for {module_name}: {command}",
                err=True,
                fg=c.GREEN,
            )
            subprocess.run(command, check=True, shell=True)

    docker_compose_fn_final = Path(tempfile.mkdtemp()) / "ci"
    typer.secho("Running docker compose to create DiracX containers", fg=c.GREEN)
    typer.secho(f"Will leave a folder behind: {docker_compose_fn_final}", fg=c.YELLOW)

    with _gen_docker_compose(modules, diracx_dist_dir=diracx_dist_dir) as docker_compose_fn:
        # We cannot use the temporary directory created in the context manager because
        # we don't stay in the contect manager (Popen)
        # So we need something that outlives it.
        shutil.copytree(docker_compose_fn.parent, docker_compose_fn_final, dirs_exist_ok=True)
        # We use Popen because we don't want to wait for this command to finish.
        # It is going to start all the diracx containers, including one which waits
        # for the DIRAC installation to be over.
        subStdout = open(docker_compose_fn_final / "stdout", "w")
        subStderr = open(docker_compose_fn_final / "stderr", "w")

        subprocess.Popen(
            [*DOCKER_COMPOSE_CMD, "-f", docker_compose_fn_final / "docker-compose.yml", "up", "-d", "diracx"],
            env=docker_compose_env,
            stdin=None,
            stdout=subStdout,
            stderr=subStderr,
            close_fds=True,
        )


@app.command()
def install_server():
    """Install DIRAC in the server container."""
    _check_containers_running()

    # This runs a continuous loop that exports the config in yaml
    # for the diracx container to use
    # It needs to be started and running before the DIRAC server installation
    # because after installing the databases, the install server script
    # calls dirac-proxy-init.
    # At this point we need the new CS to have been updated
    # already else the token exchange fails.

    typer.secho("Starting configuration export loop for diracx", fg=c.GREEN)
    base_cmd = _build_docker_cmd("server", tty=False, daemon=True, use_root=True)
    subprocess.run(
        base_cmd + ["bash", "/home/dirac/LocalRepo/ALTERNATIVE_MODULES/DIRAC/tests/CI/exportCSLoop.sh"],
        check=True,
    )

    typer.secho("Running server installation", fg=c.GREEN)
    base_cmd = _build_docker_cmd("server", tty=False)
    subprocess.run(
        base_cmd + ["bash", "/home/dirac/LocalRepo/TestCode/DIRAC/tests/CI/install_server.sh"],
        check=True,
    )


@app.command()
def install_client():
    """Install DIRAC in the client container."""
    _check_containers_running()
    typer.secho("Running client installation", fg=c.GREEN)
    base_cmd = _build_docker_cmd("client")
    subprocess.run(
        base_cmd + ["bash", "/home/dirac/LocalRepo/TestCode/DIRAC/tests/CI/install_client.sh"],
        check=True,
    )


@app.command()
def install_pilot():
    """Run a pilot in a container."""
    _check_containers_running()
    typer.secho("Running pilot installation", fg=c.GREEN)
    base_cmd = _build_docker_cmd("pilot")
    subprocess.run(
        base_cmd + ["bash", "/home/dirac/LocalRepo/TestCode/DIRAC/tests/CI/run_pilot.sh"],
        check=True,
    )


@app.command()
def test_server():
    """Run the server integration tests."""
    _check_containers_running()
    typer.secho("Running server tests", err=True, fg=c.GREEN)
    base_cmd = _build_docker_cmd("server")
    ret = subprocess.run(base_cmd + ["bash", "TestCode/DIRAC/tests/CI/run_tests.sh"], check=False)
    color = c.GREEN if ret.returncode == 0 else c.RED
    typer.secho(f"Server tests finished with {ret.returncode}", err=True, fg=color)
    raise TestExit(ret.returncode)


@app.command()
def test_client():
    """Run the client integration tests."""
    _check_containers_running()
    typer.secho("Running client tests", err=True, fg=c.GREEN)
    base_cmd = _build_docker_cmd("client")
    ret = subprocess.run(base_cmd + ["bash", "TestCode/DIRAC/tests/CI/run_tests.sh"], check=False)
    color = c.GREEN if ret.returncode == 0 else c.RED
    typer.secho(f"Client tests finished with {ret.returncode}", err=True, fg=color)
    raise TestExit(ret.returncode)


@app.command()
def test_pilot():
    """Run the pilot integration tests."""
    _check_containers_running()
    typer.secho("Running pilot tests", err=True, fg=c.GREEN)
    base_cmd = _build_docker_cmd("pilot")
    ret = subprocess.run(base_cmd + ["bash", "LocalRepo/TestCode/DIRAC/tests/CI/run_tests.sh"], check=False)
    color = c.GREEN if ret.returncode == 0 else c.RED
    typer.secho(f"pilot tests finished with {ret.returncode}", err=True, fg=color)
    raise TestExit(ret.returncode)


@app.command()
def exec_server():
    """Start an interactive session in the server container."""
    _check_containers_running()
    cmd = _build_docker_cmd("server")
    cmd += [
        "bash",
        "-c",
        ". $HOME/CONFIG && . $HOME/ServerInstallDIR/bashrc && exec bash",
    ]
    typer.secho("Opening prompt inside server container", err=True, fg=c.GREEN)
    os.execvp(cmd[0], cmd)


@app.command()
def exec_client():
    """Start an interactive session in the client container."""
    _check_containers_running()
    cmd = _build_docker_cmd("client")
    cmd += [
        "bash",
        "-c",
        ". $HOME/CONFIG && . $HOME/ClientInstallDIR/bashrc && exec bash",
    ]
    typer.secho("Opening prompt inside client container", err=True, fg=c.GREEN)
    os.execvp(cmd[0], cmd)


@app.command()
def exec_pilot():
    """Start an interactive session in the pilot container."""
    _check_containers_running()
    cmd = _build_docker_cmd("pilot")
    cmd += ["bash", "-c", ". $HOME/CONFIG && exec bash"]
    typer.secho("Opening prompt inside pilot container", err=True, fg=c.GREEN)
    os.execvp(cmd[0], cmd)


@app.command()
def exec_mysql():
    """Start an interactive session in the server container."""
    _check_containers_running()
    cmd = _build_docker_cmd("mysql", use_root=True, cwd="/")
    cmd += [
        "bash",
        "-c",
        f"exec mysql --user={DB_USER} --password={DB_PASSWORD}",
    ]
    typer.secho("Opening prompt inside server container", err=True, fg=c.GREEN)
    os.execvp(cmd[0], cmd)


@app.command()
def list_services():
    """List the services which have been running.

    Only the services for which /log/current exists are shown.
    """
    _check_containers_running()
    typer.secho("Known services:", err=True)
    for service in _list_services()[1]:
        typer.secho(f"* {service}", err=True)


@app.command()
def runsvctrl(command: str, pattern: str):
    """Execute runsvctrl inside the server container."""
    _check_containers_running()
    runit_dir, services = _list_services()
    cmd = _build_docker_cmd("server", cwd=runit_dir)
    services = fnmatch.filter(services, pattern)
    if not services:
        typer.secho(f"No services match {pattern!r}", fg=c.RED)
        raise typer.Exit(code=1)
    cmd += ["runsvctrl", command] + services
    os.execvp(cmd[0], cmd)


@app.command()
def logs(pattern: str = "*", lines: int = 10, follow: bool = True):
    """Show DIRAC's logs from the service container.

    For services matching [--pattern] show the most recent [--lines] from the
    logs. If [--follow] is True, continiously stream the logs.
    """
    _check_containers_running()
    runit_dir, services = _list_services()
    base_cmd = _build_docker_cmd("server", tty=False) + ["tail"]
    base_cmd += [f"--lines={lines}"]
    if follow:
        base_cmd += ["-f"]
    with ThreadPoolExecutor(len(services)) as pool:
        futures = []
        for service in fnmatch.filter(services, pattern):
            cmd = base_cmd + [f"{runit_dir}/{service}/log/current"]
            p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=None, text=True)
            futures.append(pool.submit(_log_popen_stdout, p))
        for res in as_completed(futures):
            err = res.exception()
            if err:
                raise err


class TestExit(typer.Exit):
    pass


@contextmanager
def _gen_docker_compose(modules, *, diracx_dist_dir=None):
    # Load the docker compose configuration and mount the necessary volumes
    input_fn = Path(__file__).parent / "tests/CI/docker-compose.yml"
    docker_compose = yaml.safe_load(input_fn.read_text())
    # diracx-wait-for-db needs the volume to be able to run the waiting script
    for ctn in ("dirac-server", "dirac-client", "dirac-pilot", "diracx-wait-for-db"):
        if "volumes" not in docker_compose["services"][ctn]:
            docker_compose["services"][ctn]["volumes"] = []
    volumes = [f"{path}:/home/dirac/LocalRepo/ALTERNATIVE_MODULES/{name}" for name, path in modules.items()]
    volumes += [f"{path}:/home/dirac/LocalRepo/TestCode/{name}" for name, path in modules.items()]
    docker_compose["services"]["dirac-server"]["volumes"].extend(volumes[:])
    docker_compose["services"]["dirac-client"]["volumes"].extend(volumes[:])
    docker_compose["services"]["dirac-pilot"]["volumes"].extend(volumes[:])
    docker_compose["services"]["diracx-wait-for-db"]["volumes"].extend(volumes[:])

    module_configs = _load_module_configs(modules)
    if diracx_dist_dir is not None:
        for container_name in [
            "dirac-client",
            "dirac-pilot",
            "dirac-server",
            "diracx-init-cs",
            "diracx-wait-for-db",
            "diracx-init-db",
            "diracx",
        ]:
            docker_compose["services"][container_name].setdefault("volumes", []).append(
                f"{diracx_dist_dir}:/diracx_sources"
            )
            docker_compose["services"][container_name].setdefault("environment", []).append(
                "DIRACX_CUSTOM_SOURCE_PREFIXES=/diracx_sources"
            )

    # Add any extension services
    for module_name, module_configs in module_configs.items():
        for service_name, service_config in module_configs["extra-services"].items():
            typer.secho(f"Adding service {service_name} for {module_name}", err=True, fg=c.GREEN)
            docker_compose["services"][service_name] = service_config.copy()
            docker_compose["services"][service_name]["volumes"] = volumes[:]

    # Write to a tempory file with the appropriate profile name
    prefix = "ci"
    with tempfile.TemporaryDirectory() as tmpdir:
        input_docker_compose_dir = Path(__file__).parent / "tests/CI/"
        output_fn = Path(tmpdir) / prefix / "docker-compose.yml"
        output_fn.parent.mkdir()
        output_fn.write_text(yaml.safe_dump(docker_compose, sort_keys=False))
        shutil.copytree(input_docker_compose_dir / "envs", str(Path(tmpdir) / prefix), dirs_exist_ok=True)
        yield output_fn


def _check_containers_running(*, is_up=True):
    with _gen_docker_compose(DEFAULT_MODULES) as docker_compose_fn:
        running_containers = subprocess.run(
            [*DOCKER_COMPOSE_CMD, "-f", docker_compose_fn, "ps", "-q", "-a"],
            stdout=subprocess.PIPE,
            env=_make_env({}),
            # docker compose ps has a non-zero exit code when no containers are running
            check=False,
            text=True,
        ).stdout.split("\n")
    if is_up:
        if not any(running_containers):
            typer.secho(
                "No running containers found, environment must be prepared first!",
                err=True,
                fg=c.RED,
            )
            raise typer.Exit(code=1)
    else:
        if any(running_containers):
            typer.secho(
                "Running instance already found, it must be destroyed first!",
                err=True,
                fg=c.RED,
            )
            raise typer.Exit(code=1)


def _find_dirac_release():
    # Start by looking for the GitHub/GitLab environment variables
    if "GITHUB_BASE_REF" in os.environ:  # this will be "rel-v8r0"
        return os.environ["GITHUB_BASE_REF"]
    if "CI_COMMIT_REF_NAME" in os.environ:
        return os.environ["CI_COMMIT_REF_NAME"]
    if "CI_MERGE_REQUEST_TARGET_BRANCH_NAME" in os.environ:
        return os.environ["CI_MERGE_REQUEST_TARGET_BRANCH_NAME"]

    repo = git.Repo(os.getcwd())
    # Try to make sure the upstream remote is up to date
    try:
        upstream = repo.remote("upstream")
    except ValueError:
        typer.secho("No upstream remote found, adding", err=True, fg=c.YELLOW)
        upstream = repo.create_remote("upstream", "https://github.com/DIRACGrid/DIRAC.git")
    try:
        upstream.fetch()
    except Exception:
        typer.secho("Failed to fetch from remote 'upstream'", err=True, fg=c.YELLOW)
    # Find the most recent tag on the current branch
    version = Version(
        repo.git.describe(
            dirty=True,
            tags=True,
            long=True,
            match="*[0-9]*",
            exclude=["v[0-9]r*", "v[0-9][0-9]r*"],
        ).split("-")[0]
    )
    # See if there is a remote branch named "rel-vXrY"
    version_branch = f"rel-v{version.major}r{version.minor}"
    try:
        upstream.refs[version_branch]
    except IndexError:
        typer.secho(
            f"Failed to find branch for {version_branch}, defaulting to integration",
            err=True,
            fg=c.YELLOW,
        )
        return "integration"
    else:
        return version_branch


def _make_env(flags):
    env = os.environ.copy()
    env["DIRAC_UID"] = str(os.getuid())
    env["DIRAC_GID"] = str(os.getgid())
    env["HOST_OS"] = flags.pop("HOST_OS", DEFAULT_HOST_OS)
    env["CI_REGISTRY_IMAGE"] = flags.pop("CI_REGISTRY_IMAGE", "diracgrid")
    env["MYSQL_VER"] = flags.pop("MYSQL_VER", DEFAULT_MYSQL_VER)
    if "mariadb" in env["MYSQL_VER"].lower():
        env["MYSQL_ADMIN_COMMAND"] = "mariadb-admin"
    else:
        env["MYSQL_ADMIN_COMMAND"] = "mysqladmin"
    env["ES_VER"] = flags.pop("ES_VER", DEFAULT_ES_VER)
    env["ES_PLATFORM"] = flags.pop("ES_PLATFORM", DEFAULT_ES_PLATFORM)
    env["IAM_VER"] = flags.pop("IAM_VER", DEFAULT_IAM_VER)
    if "CVMFS_DIR" not in env or not Path(env["CVMFS_DIR"]).is_dir():
        typer.secho(f"CVMFS_DIR environment value: {env.get('CVMFS_DIR', 'NOT SET')}", fg=c.YELLOW)
        env["CVMFS_DIR"] = "/tmp"
    return env


def _dict_to_shell(variables):
    lines = []
    for name, value in variables.items():
        if value is None:
            continue
        elif isinstance(value, list):
            lines += [f"declare -a {name}"]
            lines += [f"{name}+=({shlex.quote(v)})" for v in value]
        elif isinstance(value, bool):
            lines += [f"export {name}={'Yes' if value else 'No'}"]
        elif isinstance(value, str):
            lines += [f"export {name}={shlex.quote(value)}"]
        else:
            raise NotImplementedError(name, value, type(value))
    return "\n".join(lines)


def _prepare_iam_instance():
    """Prepare the IAM instance such as we have:

    * 2 clients:
      * exchange-token-test: able to exchange token
      * simple-token-test: for users
    * 2 users:
      * admin and jane doe (a user)
    * 3 groups:
      * dirac/admin
      * dirac/prod
      * dirac/user
    """
    issuer = f"http://iam-login-service:{IAM_PORT}"

    typer.secho("Getting an IAM admin token", fg=c.GREEN)

    # It sometimes takes a while for IAM to be ready so wait for a while if needed
    for _ in range(10):
        try:
            tokens = _get_iam_token(issuer, IAM_INIT_CLIENT_ID, IAM_INIT_CLIENT_SECRET)
            break
        except typer.Exit:
            typer.secho("Failed to connect to IAM, will retry in 10 seconds", fg=c.YELLOW)
            time.sleep(10)
    else:
        raise RuntimeError("All attempts to _get_iam_token failed")

    initial_admin_access_token = tokens.get("access_token")

    # Update the configuration of the initial IAM client adding the necessary scopes
    _update_init_iam_client(
        issuer,
        initial_admin_access_token,
        IAM_INIT_CLIENT_ID,
        "Admin client (read-write)",
        " ".join(["scim:read", "scim:write", "iam:admin.read", "iam:admin.write"]),
        ["client_credentials"],
    )
    # Fetch a new token with the updated client
    tokens = _get_iam_token(issuer, IAM_INIT_CLIENT_ID, IAM_INIT_CLIENT_SECRET)
    admin_access_token = tokens.get("access_token")

    typer.secho("Creating IAM clients", fg=c.GREEN)
    _create_iam_client(
        issuer,
        admin_access_token,
        IAM_SIMPLE_CLIENT_NAME,
        grant_types=["password", "client_credentials"],
    )
    _create_iam_client(
        issuer,
        admin_access_token,
        IAM_ADMIN_CLIENT_NAME,
        grant_types=["password", "urn:ietf:params:oauth:grant-type:token-exchange"],
    )

    typer.secho("Creating IAM users", fg=c.GREEN)
    simple_user_config = _create_iam_user(issuer, admin_access_token, IAM_SIMPLE_USER, IAM_SIMPLE_PASSWORD)

    typer.secho("Creating IAM groups", fg=c.GREEN)
    dirac_group_config = _create_iam_group(issuer, admin_access_token, "dirac")
    dirac_group_id = dirac_group_config["id"]
    _create_iam_subgroup(issuer, admin_access_token, "dirac", dirac_group_id, "admin")
    dirac_prod_group_config = _create_iam_subgroup(issuer, admin_access_token, "dirac", dirac_group_id, "prod")
    dirac_user_group_config = _create_iam_subgroup(issuer, admin_access_token, "dirac", dirac_group_id, "user")

    typer.secho("Adding IAM users to groups", fg=c.GREEN)
    _create_iam_group_membership(
        issuer,
        admin_access_token,
        simple_user_config["userName"],
        simple_user_config["id"],
        [dirac_group_id, dirac_prod_group_config["id"], dirac_user_group_config["id"]],
    )


def _iam_curl(
    url: str, *, data: list[str] = [], verb: Optional[str] = None, user: Optional[str] = None, headers: list[str] = []
) -> subprocess.CompletedProcess:
    cmd = ["docker", "exec", "server", "curl", "-L", "-s"]
    if verb:
        cmd += ["-X", verb]
    if user:
        cmd += ["-u", user]
    for arg in data:
        cmd += ["-d", arg]
    for header in headers:
        cmd += ["-H", header]
    cmd += [url]

    return subprocess.run(cmd, capture_output=True, check=False)


def _get_iam_token(issuer: str, client_id: str, client_secret: str) -> dict:
    """Get a token using the password flow

    :param str issuer: url of the issuer
    :param str user: username
    :param str password: password
    :param str client_id: client id
    :param str client_secret: client secret
    """
    # We use subprocess instead of requests to interact with IAM
    # Otherwise, if executed from a docker container in a different network namespace, it would not work
    url = os.path.join(issuer, "token")
    ret = _iam_curl(
        url,
        user=f"{client_id}:{client_secret}",
        data=["grant_type=client_credentials"],
    )

    if not ret.returncode == 0:
        typer.secho(f"Failed to get an admin token: {ret.returncode} {ret.stdout} {ret.stderr}", err=True, fg=c.RED)
        raise typer.Exit(code=1)

    return json.loads(ret.stdout)


def _update_init_iam_client(
    issuer: str, admin_access_token: str, client_id: str, client_name: str, scope: str, grant_types: list[str]
) -> dict:
    """Update the configuration of the initial IAM client adding the necessary scopes

    :param str issuer: url of the issuer
    :param str admin_access_token: access token to register a client
    :param str client_id: id of the client
    """
    # Get the configuration of the client
    url = os.path.join(issuer, "iam/api/clients", client_id)
    ret = _iam_curl(
        url,
        headers=[f"Authorization: Bearer {admin_access_token}", "Content-Type: application/json"],
    )

    if not ret.returncode == 0:
        typer.secho(
            f"Failed to get config for client {client_id}: {ret.returncode} {ret.stdout} {ret.stderr}",
            err=True,
            fg=c.RED,
        )
        raise typer.Exit(code=1)

    # Update the configuration with the provided values
    client_config = json.loads(ret.stdout)
    client_config["client_name"] = client_name
    client_config["scope"] = scope
    client_config["grant_types"] = grant_types
    client_config["redirect_uris"] = []
    client_config["code_challenge_method"] = "S256"

    # Update the client
    url = os.path.join(issuer, "iam/api/clients", client_id)
    ret = _iam_curl(
        url,
        verb="PUT",
        data=[json.dumps(client_config)],
        headers=[f"Authorization: Bearer {admin_access_token}", "Content-Type: application/json"],
    )

    if not ret.returncode == 0:
        typer.secho(
            f"Failed to update config for client {client_id}: {ret.returncode} {ret.stdout} {ret.stderr}",
            err=True,
            fg=c.RED,
        )
        raise typer.Exit(code=1)

    return json.loads(ret.stdout)


def _create_iam_client(
    issuer: str, admin_access_token: str, client_name: str, scope: str = "", grant_types: list[str] = []
) -> dict:
    """Generate an IAM client

    :param str issuer: url of the issuer
    :param str admin_access_token: access token to register a client
    :param str client_name: name of the client
    :param str scope: scope of the client
    :param list grant_types: list of grant types
    """
    scope = "openid profile offline_access " + scope

    default_grant_types = ["refresh_token"]

    # Some grant types are privileged and cannot be added at the creation of the client, but can be added later
    privileged_grant_types = ["password", "urn:ietf:params:oauth:grant-type:token-exchange", "client_credentials"]
    requested_privileged_grant_types = []
    for grant_type in privileged_grant_types:
        if grant_type in grant_types:
            grant_types.remove(grant_type)
            requested_privileged_grant_types.append(grant_type)

    grant_types = list(set(default_grant_types + grant_types))

    client_config = {
        "client_name": client_name,
        "token_endpoint_auth_method": "client_secret_basic",
        "scope": scope,
        "grant_types": grant_types,
        "response_types": ["code"],
    }

    url = os.path.join(issuer, "iam/api/client-registration")
    ret = _iam_curl(
        url,
        verb="POST",
        headers=[f"Authorization: Bearer {admin_access_token}", "Content-Type: application/json"],
        data=[json.dumps(client_config)],
    )

    if not ret.returncode == 0:
        typer.secho(f"Failed to create client {client_name}: {ret.returncode} {ret.stderr}", err=True, fg=c.RED)
        raise typer.Exit(code=1)

    client_config = json.loads(ret.stdout)
    client_id = client_config["client_id"]
    ret = _update_init_iam_client(
        issuer,
        admin_access_token,
        client_id,
        client_name,
        scope,
        list(set(requested_privileged_grant_types + grant_types)),
    )
    print(ret)
    return ret


def _create_iam_user(issuer: str, admin_access_token: str, username: str, password: str) -> dict:
    """Generate an IAM user

    :param str issuer: url of the issuer
    :param str admin_access_token: access token to register a client
    :param str given_name: name of user
    :param str family_name: family name of the user
    """
    given_name, family_name = username.split("_")
    given_name = given_name.capitalize()
    family_name = family_name.capitalize()
    user_config = {
        "active": True,
        "userName": username,
        "password": password,
        "name": {
            "givenName": given_name,
            "familyName": family_name,
            "formatted": f"{given_name} {family_name}",
        },
        "emails": [
            {
                "type": "work",
                "value": f"{given_name}.{family_name}@donotexist.email",
                "primary": True,
            }
        ],
    }

    url = os.path.join(issuer, "scim/Users")
    ret = _iam_curl(
        url,
        verb="POST",
        headers=[f"Authorization: Bearer {admin_access_token}", "Content-Type: application/scim+json"],
        data=[json.dumps(user_config)],
    )

    if not ret.returncode == 0:
        typer.secho(
            f"Failed to create user {given_name} {family_name}: {ret.returncode} {ret.stderr}",
            err=True,
            fg=c.RED,
        )
        raise typer.Exit(code=1)
    return json.loads(ret.stdout)


def _create_iam_group(issuer: str, admin_access_token: str, group_name: str) -> dict:
    """Generate an IAM group

    :param str issuer: url of the issuer
    :param str admin_access_token: access token to register a client
    :param str group_name: name of the group
    """
    group_config = {"schemas": ["urn:ietf:params:scim:schemas:core:2.0:Group"], "displayName": group_name}

    url = os.path.join(issuer, "scim/Groups")
    ret = _iam_curl(
        url,
        verb="POST",
        headers=[f"Authorization: Bearer {admin_access_token}", "Content-Type: application/scim+json"],
        data=[json.dumps(group_config)],
    )

    if not ret.returncode == 0:
        typer.secho(f"Failed to create group {group_name}: {ret.returncode} {ret.stderr}", err=True, fg=c.RED)
        raise typer.Exit(code=1)
    return json.loads(ret.stdout)


def _create_iam_subgroup(
    issuer: str, admin_access_token: str, group_name: str, group_id: str, subgroup_name: str
) -> dict:
    """Generate an IAM subgroup

    :param str issuer: url of the issuer
    :param str admin_access_token: access token to register a client
    :param str group_name: name of the group
    :param str group_id: id of the group
    :param str subgroup_name: name of the subgroup
    """
    subgroup_config = {
        "schemas": ["urn:ietf:params:scim:schemas:core:2.0:Group", "urn:indigo-dc:scim:schemas:IndigoGroup"],
        "urn:indigo-dc:scim:schemas:IndigoGroup": {
            "parentGroup": {
                "display": group_name,
                "value": group_id,
                r"\$ref": os.path.join(issuer, "scim/Groups", group_id),
            },
        },
        "displayName": subgroup_name,
    }

    url = os.path.join(issuer, "scim/Groups")
    ret = _iam_curl(
        url,
        verb="POST",
        headers=[f"Authorization: Bearer {admin_access_token}", "Content-Type: application/scim+json"],
        data=[json.dumps(subgroup_config)],
    )

    if not ret.returncode == 0:
        typer.secho(
            f"Failed to create subgroup {group_name}/{subgroup_name}: {ret.returncode} {ret.stderr}",
            err=True,
            fg=c.RED,
        )
        raise typer.Exit(code=1)
    return json.loads(ret.stdout)


def _create_iam_group_membership(
    issuer: str, admin_access_token: str, username: str, user_id: str, group_ids: list[str]
):
    """Bind a given user to some groups/subgroups

    :param str issuer: url of the issuer
    :param str admin_access_token: access token to register a client
    :param str username: username
    :param str user_id:: id of the user
    :param list group_ids: list of group/subgroup ids
    """
    membership_config = {
        "schemas": ["urn:ietf:params:scim:api:messages:2.0:PatchOp"],
        "operations": [
            {
                "op": "add",
                "path": "members",
                "value": [
                    {"display": username, "value": user_id, r"\$ref": os.path.join(issuer, "scim/Users", user_id)}
                ],
            }
        ],
    }

    for group_id in group_ids:
        url = os.path.join(issuer, "scim/Groups", group_id)
        ret = _iam_curl(
            url,
            verb="PATCH",
            headers=[f"Authorization: Bearer {admin_access_token}", "Content-Type: application/scim+json"],
            data=[json.dumps(membership_config)],
        )

        if not ret.returncode == 0:
            typer.secho(f"Failed to add {username} to {group_id}: {ret.returncode} {ret.stderr}", err=True, fg=c.RED)
            raise typer.Exit(code=1)


def _make_config(modules, flags, release_var, editable):
    config = {
        # MYSQL Settings
        "DB_USER": DB_USER,
        "DB_PASSWORD": DB_PASSWORD,
        "DB_ROOTUSER": DB_ROOTUSER,
        "DB_ROOTPWD": DB_ROOTPWD,
        "DB_HOST": DB_HOST,
        "DB_PORT": DB_PORT,
        # OpenSearch settings
        "NoSQLDB_USER": "elastic",
        "NoSQLDB_PASSWORD": "changeme",
        "NoSQLDB_HOST": "opensearch",
        "NoSQLDB_PORT": "9200",
        # IAM initial settings
        "IAM_INIT_CLIENT_ID": IAM_INIT_CLIENT_ID,
        "IAM_INIT_CLIENT_SECRET": IAM_INIT_CLIENT_SECRET,
        "IAM_SIMPLE_CLIENT_NAME": IAM_SIMPLE_CLIENT_NAME,
        "IAM_SIMPLE_USER": IAM_SIMPLE_USER,
        "IAM_SIMPLE_PASSWORD": IAM_SIMPLE_PASSWORD,
        "IAM_ADMIN_CLIENT_NAME": IAM_ADMIN_CLIENT_NAME,
        "IAM_ADMIN_USER": IAM_ADMIN_USER,
        "IAM_ADMIN_PASSWORD": IAM_ADMIN_PASSWORD,
        "IAM_HOST": IAM_HOST,
        "IAM_PORT": IAM_PORT,
        # Hostnames
        "SERVER_HOST": "server",
        "CLIENT_HOST": "client",
        "PILOT_HOST": "pilot",
        # Test specific variables
        "WORKSPACE": "/home/dirac",
        # DiracX variable
        "DIRACX_URL": "http://diracx:8000",
    }

    if editable:
        config["PIP_INSTALL_EXTRA_ARGS"] = "-e"

    required_feature_flags = []
    for _, module_ci_config in _load_module_configs(modules).items():
        config |= module_ci_config["config"]
        required_feature_flags += module_ci_config.get("required-feature-flags", [])
    config["DIRAC_CI_SETUP_SCRIPT"] = "/home/dirac/LocalRepo/TestCode/" + config["DIRAC_CI_SETUP_SCRIPT"]

    # This can likely be removed after the Python 3 migration
    if release_var:
        config |= dict([release_var.split("=", 1)])
    else:
        config["DIRAC_RELEASE"] = _find_dirac_release()

    for key, default_value in FEATURE_VARIABLES.items():
        config[key] = flags.pop(key, default_value)
    for key in required_feature_flags:
        try:
            config[key] = flags.pop(key)
        except KeyError:
            typer.secho(f"Required feature variable {key!r} is missing", err=True, fg=c.RED)
            raise typer.Exit(code=1)

    # If we test DiracX, add specific config
    if config["TEST_DIRACX"].lower() in ("yes", "true"):
        if DIRACX_DISABLED_SERVICES:
            # We link all disabled services
            # config["DIRACX_DISABLED_SERVICES"] = "Service1 Service2 Service3 ..."
            diracx_disabled_services = " ".join(DIRACX_DISABLED_SERVICES)

            typer.secho(f"The following services won't be legacy adapted: {diracx_disabled_services}", fg="yellow")

            config["DIRACX_DISABLED_SERVICES"] = diracx_disabled_services

    config["TESTREPO"] = [f"/home/dirac/LocalRepo/TestCode/{name}" for name in modules]
    config["ALTERNATIVE_MODULES"] = [f"/home/dirac/LocalRepo/ALTERNATIVE_MODULES/{name}" for name in modules]

    # Exit with an error if there are unused feature flags remaining
    if flags:
        typer.secho(f"Unrecognised feature flags {flags!r}", err=True, fg=c.RED)
        raise typer.Exit(code=1)

    return config


def _load_module_configs(modules):
    module_ci_configs = {}
    for module_name, module_path in modules.items():
        module_ci_config_path = module_path / "tests/.dirac-ci-config.yaml"
        if not module_ci_config_path.exists():
            continue
        module_ci_configs[module_name] = yaml.safe_load(module_ci_config_path.read_text())
    return module_ci_configs


def _build_docker_cmd(container_name, *, use_root=False, cwd="/home/dirac", tty=True, daemon=False):
    if use_root or os.getuid() == 0:
        user = "root"
    else:
        user = "dirac"
    cmd = ["docker", "exec"]
    if tty:
        if sys.stdout.isatty():
            cmd += ["-it"]
        else:
            typer.secho(
                'Not passing "-it" to docker as stdout is not a tty',
                err=True,
                fg=c.YELLOW,
            )
    if daemon:
        cmd += ["-d"]
    cmd += [
        "-e=TERM=xterm-color",
        "-e=INSTALLROOT=/home/dirac",
        f"-e=INSTALLTYPE={container_name}",
        f"-u={user}",
        f"-w={cwd}",
        container_name,
    ]
    return cmd


def _list_services():
    # The Python 3 runit dir ends up in /diracos
    for runit_dir in ["ServerInstallDIR/runit", "ServerInstallDIR/diracos/runit"]:
        cmd = _build_docker_cmd("server")
        cmd += [
            "bash",
            "-c",
            f'cd {runit_dir}/ && for fn in */*/log/current; do echo "$(dirname "$(dirname "$fn")")"; done',
        ]
        ret = subprocess.run(cmd, check=False, stdout=subprocess.PIPE, text=True)
        if not ret.returncode:
            return runit_dir, ret.stdout.split()
    else:
        typer.secho("Failed to find list of available services", err=True, fg=c.RED)
        typer.secho(f"stdout was: {ret.stdout!r}", err=True)
        typer.secho(f"stderr was: {ret.stderr!r}", err=True)
        raise typer.Exit(1)


def _log_popen_stdout(p):
    while p.poll() is None:
        line = p.stdout.readline().rstrip()
        if not line:
            continue
        bg, fg = None, None
        if match := LOG_PATTERN.match(line):
            bg, fg = LOG_LEVEL_MAP.get(match.groups()[0], (bg, fg))
        typer.secho(line, err=True, bg=bg, fg=fg)


if __name__ == "__main__":
    app()
