#!/usr/bin/env python
import fnmatch
import json
import os
from pathlib import Path
import re
import shlex
import shutil
import subprocess
import sys
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager
from typing import Optional

import git
import typer
import yaml
from packaging.version import Version
from typer import colors as c

# Editable configuration
DEFAULT_HOST_OS = "cc7"
DEFAULT_MYSQL_VER = "mysql:8.0"
DEFAULT_ES_VER = "elasticsearch:7.9.1"
DEFAULT_IAM_VER = "indigoiam/iam-login-service:v1.8.0"
FEATURE_VARIABLES = {
    "DIRACOSVER": "master",
    "DIRACOS_TARBALL_PATH": None,
    "TEST_HTTPS": "No",
    "DIRAC_FEWER_CFG_LOCKS": None,
    "DIRAC_USE_JSON_ENCODE": None,
    "DIRAC_USE_JSON_DECODE": None,
}
DEFAULT_MODULES = {
    "DIRAC": Path(__file__).parent.absolute(),
}

# Static configuration
DB_USER = "Dirac"
DB_PASSWORD = "Dirac"
DB_ROOTUSER = "root"
DB_ROOTPWD = "password"
DB_HOST = "mysql"
DB_PORT = "3306"

IAM_INIT_CLIENT_ID = "password-grant"
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
  ./integration_tests.py test-server
  ./integration_tests.py test-client

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
""",
)


@app.command()
def create(
    flags: Optional[list[str]] = typer.Argument(None),
    editable: Optional[bool] = None,
    extra_module: Optional[list[str]] = None,
    release_var: Optional[str] = None,
    run_server_tests: bool = True,
    run_client_tests: bool = True,
):
    """Start a local instance of the integration tests"""
    prepare_environment(flags, editable, extra_module, release_var)
    install_server()
    install_client()
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
    if exit_code != 0:
        typer.secho("One or more tests failed", err=True, fg=c.RED)
    raise typer.Exit(exit_code)


@app.command()
def destroy():
    """Destroy a local instance of the integration tests"""
    typer.secho("Shutting down and removing containers", err=True, fg=c.GREEN)
    with _gen_docker_compose(DEFAULT_MODULES) as docker_compose_fn:
        os.execvpe(
            "docker-compose",
            ["docker-compose", "-f", docker_compose_fn, "down", "--remove-orphans", "-t", "0"],
            _make_env({}),
        )


@app.command()
def prepare_environment(
    flags: Optional[list[str]] = typer.Argument(None),
    editable: Optional[bool] = None,
    extra_module: Optional[list[str]] = None,
    release_var: Optional[str] = None,
):
    """Prepare the local environment for installing DIRAC."""

    _check_containers_running(is_up=False)
    if editable is None:
        editable = sys.stdout.isatty()
        typer.secho(
            f"No value passed for --[no-]editable, automatically detected: {editable}",
            fg=c.YELLOW,
        )
    typer.echo(f"Preparing environment")

    modules = DEFAULT_MODULES | dict(f.split("=", 1) for f in extra_module)
    modules = {k: Path(v).absolute() for k, v in modules.items()}

    flags = dict(f.split("=", 1) for f in flags)
    docker_compose_env = _make_env(flags)
    server_flags = {}
    client_flags = {}
    for key, value in flags.items():
        if key.startswith("SERVER_"):
            server_flags[key[len("SERVER_") :]] = value
        elif key.startswith("CLIENT_"):
            client_flags[key[len("CLIENT_") :]] = value
        else:
            server_flags[key] = value
            client_flags[key] = value
    server_config = _make_config(modules, server_flags, release_var, editable)
    client_config = _make_config(modules, client_flags, release_var, editable)

    typer.secho("Running docker-compose to create containers", fg=c.GREEN)
    with _gen_docker_compose(modules) as docker_compose_fn:
        subprocess.run(
            ["docker-compose", "-f", docker_compose_fn, "up", "-d"],
            check=True,
            env=docker_compose_env,
        )

    typer.secho("Creating users in server and client containers", fg=c.GREEN)
    for container_name in ["server", "client"]:
        if os.getuid() == 0:
            continue
        cmd = _build_docker_cmd(container_name, use_root=True, cwd="/")
        gid = str(os.getgid())
        uid = str(os.getuid())
        ret = subprocess.run(cmd + ["groupadd", "--gid", gid, "dirac"], check=False)
        if ret.returncode != 0:
            typer.secho(f"Failed to add add group dirac with id={gid}", fg=c.YELLOW)
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
    cmd = ["docker", "exec", "mysql", "mysql", f"--password={DB_ROOTPWD}", "-e"]
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
    for name, config in [("server", server_config), ("client", client_config)]:
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


@app.command()
def install_server():
    """Install DIRAC in the server container."""
    _check_containers_running()

    typer.secho("Running server installation", fg=c.GREEN)
    base_cmd = _build_docker_cmd("server", tty=False)
    subprocess.run(
        base_cmd + ["bash", "/home/dirac/LocalRepo/TestCode/DIRAC/tests/CI/install_server.sh"],
        check=True,
    )

    typer.secho("Copying credentials and certificates", fg=c.GREEN)
    base_cmd = _build_docker_cmd("client", tty=False)
    subprocess.run(
        base_cmd
        + [
            "mkdir",
            "-p",
            "/home/dirac/ServerInstallDIR/user",
            "/home/dirac/ClientInstallDIR/etc",
            "/home/dirac/.globus",
        ],
        check=True,
    )
    for path in [
        "etc/grid-security",
        "user/client.pem",
        "user/client.key",
        f"/tmp/x509up_u{os.getuid()}",
    ]:
        source = os.path.join("/home/dirac/ServerInstallDIR", path)
        ret = subprocess.run(
            ["docker", "cp", f"server:{source}", "-"],
            check=True,
            text=False,
            stdout=subprocess.PIPE,
        )
        if path.startswith("user/"):
            dest = f"client:/home/dirac/ServerInstallDIR/{os.path.dirname(path)}"
        elif path.startswith("/"):
            dest = f"client:{os.path.dirname(path)}"
        else:
            dest = f"client:/home/dirac/ClientInstallDIR/{os.path.dirname(path)}"
        subprocess.run(["docker", "cp", "-", dest], check=True, text=False, input=ret.stdout)
    subprocess.run(
        base_cmd
        + [
            "bash",
            "-c",
            "cp /home/dirac/ServerInstallDIR/user/client.* /home/dirac/.globus/",
        ],
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
def _gen_docker_compose(modules):
    # Load the docker-compose configuration and mount the necessary volumes
    input_fn = Path(__file__).parent / "tests/CI/docker-compose.yml"
    docker_compose = yaml.safe_load(input_fn.read_text())
    volumes = [f"{path}:/home/dirac/LocalRepo/ALTERNATIVE_MODULES/{name}" for name, path in modules.items()]
    volumes += [f"{path}:/home/dirac/LocalRepo/TestCode/{name}" for name, path in modules.items()]
    docker_compose["services"]["dirac-server"]["volumes"] = volumes[:]
    docker_compose["services"]["dirac-client"]["volumes"] = volumes[:]

    # Add any extension services
    for module_name, module_configs in _load_module_configs(modules).items():
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
            ["docker-compose", "-f", docker_compose_fn, "ps", "-q", "-a"],
            stdout=subprocess.PIPE,
            env=_make_env({}),
            check=True,
            text=True,
        ).stdout.split("\n")
    if is_up:
        if not any(running_containers):
            typer.secho(
                f"No running containers found, environment must be prepared first!",
                err=True,
                fg=c.RED,
            )
            raise typer.Exit(code=1)
    else:
        if any(running_containers):
            typer.secho(
                f"Running instance already found, it must be destroyed first!",
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
    env["ES_VER"] = flags.pop("ES_VER", DEFAULT_ES_VER)
    env["IAM_VER"] = flags.pop("IAM_VER", DEFAULT_IAM_VER)
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
            tokens = _get_iam_token(
                issuer, IAM_ADMIN_USER, IAM_ADMIN_PASSWORD, IAM_INIT_CLIENT_ID, IAM_INIT_CLIENT_SECRET
            )
            break
        except typer.Exit:
            typer.secho("Failed to connect to IAM, will retry in 10 seconds", fg=c.YELLOW)
            time.sleep(10)
    else:
        raise RuntimeError("All attempts to _get_iam_token failed")

    admin_access_token = tokens.get("access_token")

    typer.secho("Creating IAM clients", fg=c.GREEN)
    user_client_config = _create_iam_client(
        issuer,
        admin_access_token,
        IAM_SIMPLE_CLIENT_NAME,
    )
    admin_client_config = _create_iam_client(
        issuer,
        admin_access_token,
        IAM_ADMIN_CLIENT_NAME,
        grant_types=["urn:ietf:params:oauth:grant-type:token-exchange"],
    )

    typer.secho("Creating IAM users", fg=c.GREEN)
    simple_user_config = _create_iam_user(issuer, admin_access_token, IAM_SIMPLE_USER, IAM_SIMPLE_PASSWORD)

    typer.secho("Creating IAM groups", fg=c.GREEN)
    dirac_group_config = _create_iam_group(issuer, admin_access_token, "dirac")
    dirac_group_id = dirac_group_config["id"]
    dirac_admin_group_config = _create_iam_subgroup(issuer, admin_access_token, "dirac", dirac_group_id, "admin")
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


def _get_iam_token(issuer: str, user: str, password: str, client_id: str, client_secret: str) -> dict:
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
        data=[f"grant_type=password", f"username={user}", f"password={password}"],
    )

    if not ret.returncode == 0:
        typer.secho(f"Failed to get an admin token: {ret.returncode} {ret.stderr}", err=True, fg=c.RED)
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

    default_grant_types = ["refresh_token", "password"]
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
        headers=[f"Authorization: Bearer {admin_access_token}", f"Content-Type: application/json"],
        data=[json.dumps(client_config)],
    )

    if not ret.returncode == 0:
        typer.secho(f"Failed to create client {client_name}: {ret.returncode} {ret.stderr}", err=True, fg=c.RED)
        raise typer.Exit(code=1)

    # FIX TO REMOVE WITH IAM:v1.8.2
    # -----------------------------
    # Because of an issue in IAM, a client dynamically registered using the password flow
    # will provide invalid refresh token: https://github.com/indigo-iam/iam/issues/575
    # To cope with this problem, we have to update the client with the following params
    client_config = json.loads(ret.stdout)
    client_config["grant_types"].append("client_credentials")
    client_config["refresh_token_validity_seconds"] = 3600
    client_config["access_token_validity_seconds"] = 3600

    url = os.path.join(issuer, "iam/api/clients", client_config["client_id"])
    ret = _iam_curl(
        url,
        verb="PUT",
        headers=[f"Authorization: Bearer {admin_access_token}", f"Content-Type: application/json"],
        data=[json.dumps(client_config)],
    )

    if not ret.returncode == 0:
        typer.secho(f"Failed to update client {client_name}: {ret.returncode} {ret.stderr}", err=True, fg=c.RED)
        raise typer.Exit(code=1)
    # -----------------------------

    return json.loads(ret.stdout)


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
        headers=[f"Authorization: Bearer {admin_access_token}", f"Content-Type: application/scim+json"],
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
        headers=[f"Authorization: Bearer {admin_access_token}", f"Content-Type: application/scim+json"],
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
        headers=[f"Authorization: Bearer {admin_access_token}", f"Content-Type: application/scim+json"],
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
            headers=[f"Authorization: Bearer {admin_access_token}", f"Content-Type: application/scim+json"],
            data=[json.dumps(membership_config)],
        )

        if not ret.returncode == 0:
            typer.secho(f"Failed to add {username} to {group_id}: {ret.returncode} {ret.stderr}", err=True, fg=c.RED)
            raise typer.Exit(code=1)


def _make_config(modules, flags, release_var, editable):
    config = {
        "DEBUG": "True",
        # MYSQL Settings
        "DB_USER": DB_USER,
        "DB_PASSWORD": DB_PASSWORD,
        "DB_ROOTUSER": DB_ROOTUSER,
        "DB_ROOTPWD": DB_ROOTPWD,
        "DB_HOST": DB_HOST,
        "DB_PORT": DB_PORT,
        # ElasticSearch settings
        "NoSQLDB_USER": "elastic",
        "NoSQLDB_PASSWORD": "changeme",
        "NoSQLDB_HOST": "elasticsearch",
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
        # Test specific variables
        "WORKSPACE": "/home/dirac",
    }

    if editable:
        config["PIP_INSTALL_EXTRA_ARGS"] = "-e"

    required_feature_flags = []
    for module_name, module_ci_config in _load_module_configs(modules).items():
        config |= module_ci_config["config"]
        required_feature_flags += module_ci_config.get("required-feature-flags", [])
    config["DIRAC_CI_SETUP_SCRIPT"] = "/home/dirac/LocalRepo/TestCode/" + config["DIRAC_CI_SETUP_SCRIPT"]

    # This can likely be removed after the Python 3 migration
    if release_var:
        config |= dict([release_var.split("=", 1)])
    else:
        config["DIRAC_RELEASE"] = _find_dirac_release()

    print(config)

    for key, default_value in FEATURE_VARIABLES.items():
        config[key] = flags.pop(key, default_value)
    for key in required_feature_flags:
        try:
            config[key] = flags.pop(key)
        except KeyError:
            typer.secho(f"Required feature variable {key!r} is missing", err=True, fg=c.RED)
            raise typer.Exit(code=1)
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


def _build_docker_cmd(container_name, *, use_root=False, cwd="/home/dirac", tty=True):
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
