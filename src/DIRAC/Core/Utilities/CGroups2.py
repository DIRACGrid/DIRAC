#!/usr/bin/env python3
"""cgroup2 support for DIRAC pilot."""

import os
import functools
import subprocess
from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.Core.Utilities.DIRACSingleton import DIRACSingleton
from DIRAC.Core.Utilities import Subprocess


class CG2Manager(metaclass=DIRACSingleton):
    """A class to manage cgroup2 hierachy for a typical pilot job use-case.

    This creates a group for all of the pilot processes (anything in the
    group at the start. This is a requirement for controlling the
    sub-groups (no processes in non-leaf groups).

    A group is then created on request for each "slot" under the pilot,
    with the requested limits.
    """

    # Paths used to lookup cgroup info
    FILE_MOUNTS = "/proc/mounts"
    FILE_CUR_CGROUP = f"/proc/{os.getpid()}/cgroup"
    # Control file names within the cgroup2 hierachy
    CTRL_CONTROLLERS = "cgroup.controllers"
    CTRL_PROCS = "cgroup.procs"
    CTRL_SUBTREE = "cgroup.subtree_control"
    CTRL_MEM_OOM_GROUP = "memory.oom.group"
    CTRL_MEM_EVENTS = "memory.events"
    CTRL_MEM_MAX = "memory.max"
    CTRL_MEM_SWAP_MAX = "memory.swap.max"
    CTRL_MEM_PEAK = "memory.peak"
    CTRL_CPU_MAX = "cpu.max"
    # CPU controller constants
    # Weight is the max value for 1 CPU core
    CPU_WEIGHT = 100000
    # Period is the averaging time in us to apply the limit
    # The default is 100k and I see no particularly reason this should change
    CPU_PERIOD = 100000
    # Name of the group for the existing pilot processes
    PILOT_GROUP = f"dirac_pilot_{os.getpid()}"

    def __init__(self):
        """Set-up CGroup2 manager."""
        # This boolean will be set to True if the cgroups are configured
        # in the expected way
        self._ready = False
        # A counter of number of subgroups created
        # Used to create unique group names
        self._subproc_num = 0
        # Physical path to the starting cgroup for this process
        # (i.e. the base of our hierachy)
        self._cgroup_path = None
        # Logger
        self.log = gLogger.getSubLogger("CG2Manager")

    @staticmethod
    def _filter_file(path, filterfcn):
        """Opens a file and runs filterfcn for each line.
        If filterfcn returns any value, that value will be returned
        by this function.
        Returns None if no line matches.
        """
        with open(path, encoding="ascii") as file_in:
            for line in file_in.readlines():
                line = line.strip()
                if res := filterfcn(line):
                    return res
        return None

    def _detect_root(self):
        """Find the cgroup2 filesystem mountpoint on this system.
        Returns the mountpoint path or None if it isn't found.
        """

        def filt(line):
            """Filter function to find the first cgroup2 mount point
            from a standard /proc/mounts layout file.
            """
            parts = line.split(" ")
            if len(parts) < 3:
                return None
            if parts[2] == "cgroup2":
                return parts[1]
            return None

        return self._filter_file(self.FILE_MOUNTS, filt)

    def _detect_path(self):
        """Finds the full physical path to the current cgroup control dir.
        Sets self._cgroup_path on success.
        Raises a RuntimeError if the path cannot be determined.
        """

        def filt(line):
            """Filter to find the current cgroup2 name for the current
            process, without the leading /.
            """
            if line.startswith("0::/"):
                return line[4:]
            return False

        if not (root_path := self._detect_root()):
            raise RuntimeError("Failed to find cgroup mount point")
        if not (cur_group := self._filter_file(self.FILE_CUR_CGROUP, filt)):
            raise RuntimeError("Failed to find current cgroup")
        self._cgroup_path = os.path.join(root_path, cur_group)

    def _create_group(self, group_name, isolate_oom=True):
        """Creates a new group.
        If "isolate_oom" is True, the new group will be decoupled
        from the parent's OOM group.
        Raises a RuntimeError if the group cannot be created.
        """
        try:
            os.mkdir(os.path.join(self._cgroup_path, group_name))
        except PermissionError as err:
            raise RuntimeError(f"Permission denied creating sub-cgroup '{group_name}'") from err
        if isolate_oom:
            self._write_control(group_name, self.CTRL_MEM_OOM_GROUP, "0")

    def _remove_group(self, group_name):
        """Removes a group."""
        os.rmdir(os.path.join(self._cgroup_path, group_name))

    def _move_init_procs(self):
        """Creates the pilot sub-group and moves all of the initial processes
        from the top group into the new sub-group.
        Will raise a RuntimeError if any cgroup configuration problem
        prevents this from completing succesfully.
        """
        self._create_group(self.PILOT_GROUP, isolate_oom=False)
        cur_pids = self._read_control("", self.CTRL_PROCS)
        self._write_control(self.PILOT_GROUP, self.CTRL_PROCS, cur_pids)

    def _read_control(self, group_name, ctrl_name):
        """Reads a control value for the given group_name (relative to our base path).
        The returned value varies depending on the value content:
          - For a single token value, a string containing that token will be returned.
          - For a single line value with space-seperated tokens, a list of tokens will be returned.
          - For a multi-line value (where each line is a token), a list of tokens will be returned.
        All tokens in the return values are strings.
        A RuntimeError will be raised if the control cannot be read.
        """
        try:
            with open(
                os.path.join(self._cgroup_path, group_name, ctrl_name),
                encoding="ascii",
            ) as file_in:
                values = [line.strip() for line in file_in.readlines()]
                if " " in values and len(values) == 1:
                    values = values[0].split(" ")
                if len(values) == 1:
                    values = values[0]
                return values
        except PermissionError as err:
            raise RuntimeError(f"Access denied reading read control '{group_name}/{ctrl_name}'") from err

    def _write_control(self, group_name, ctrl_name, value):
        """Writes a control value for a given group_name (relative to our base path).
        The value can be a string or an iterable of strings. The values should not
        contain any whitespace characters.
        A RuntimeError will be raised if the control cannot be set.
        """
        try:
            ctrl_path = os.path.join(self._cgroup_path, group_name, ctrl_name)
            with open(ctrl_path, "w", encoding="ascii") as file_out:
                if isinstance(value, str):
                    value = [value]
                for arg in value:
                    file_out.write(f"{arg}\n")
                    # Flush is critical here as setting multiple values at the same time may fail
                    file_out.flush()
        except PermissionError as err:
            raise RuntimeError(f"Access denied writing control '{group_name}/{ctrl_name}'") from err
        except OSError as err:
            # This generally happens if we're trying to set a value that is
            # considered invalid, for example delegating a controller that isn't enabled
            # in the first place.
            raise RuntimeError(f"Error writing control '{group_name}/{ctrl_name}' = {value}") from err

    def _get_oom_count(self, slot_name):
        """Extracts the OOM counter as an int for the given slot.
        Returns an int on success, can return a None if the memory.events
        doesn't contain an oom counter or throws RuntimeError on failure.
        """

        def filt(line):
            """Filter to find the oom counter from a memory.events file."""
            if line.startswith("oom "):
                return int(line[4:])
            return False

        mem_events = os.path.join(self._cgroup_path, slot_name, self.CTRL_MEM_EVENTS)
        return self._filter_file(mem_events, filt)

    def _set_limits(self, group_name, cores=None, memory=None, noswap=False):
        """Sets the limits for an existing group.
        See create_slot for a description of the other parameters.
        This will raise a RuntimeError if appyling any of the limits fail to apply.
        """
        if cores:
            proc_max = int(cores * self.CPU_WEIGHT)
            self._write_control(group_name, self.CTRL_CPU_MAX, f"{proc_max} {self.CPU_PERIOD}")
        if memory:
            self._write_control(group_name, self.CTRL_MEM_MAX, f"{memory}")
        if noswap:
            self._write_control(group_name, self.CTRL_MEM_SWAP_MAX, "0")

    def _prepare(self):
        """Sets up the cgroup tree for the current process.
        Should be called once, before using any of the other functions in this class.

        Note that this function (specifcally the _move_init_procs call) assumes that
        the list of processes is static. If the process list changes while this is running,
        it is likely that this will fail to set things up properly.
        """
        self._detect_path()
        controllers = self._read_control("", self.CTRL_CONTROLLERS)
        if not controllers:
            raise RuntimeError("No controllers enabled")
        for ctrl in ["cpu", "memory"]:
            if not ctrl in controllers:
                raise RuntimeError(f"{ctrl} controller not enabled")
        self._move_init_procs()
        self._write_control("", self.CTRL_SUBTREE, ["+cpu", "+memory"])
        self._ready = True

    def _create_slot(self, slot_name, cores=None, memory=None, noswap=False):
        """Creates a slot for a job with the given slot_name.
        Cores is a float, number of CPU cores this group may use.
        Memory is a string or int, either a number of bytes to limit the group RSS,
        or a string limit with a unit suffix, e.g. "1G" as supported by the cgroup memory
        controller.
        If noswap is set to true, the swap memory limit will be set to 0; this is mostly
        useful for testing (where the system may swap memory instead of triggering an
        OOM, which may allow a process to use more than the memory limit).
        This will raise a RuntimeError if setting up the slot fails.
        """
        if not self._ready:
            return
        self._create_group(slot_name)
        self._set_limits(slot_name, cores, memory, noswap)

    def _remove_slot(self, slot_name):
        """Removes a slot with the given name.
        Can raise usual filesystem OSError if the slot doesn't exist.
        """
        if not self._ready:
            return
        self._remove_group(slot_name)

    def _setup_subproc(self, slot_name):
        """A subprocess preexec function for setting up cgroups.
        This will move te current process into the given cgroup slot.
        On failure, no error will be reported.
        """
        # Threading danger!
        # There are potential threading issues with preexec functions
        # They must not hold any locks that the parent process might already
        # be holding, including ones in standard library functions.
        # This function should be kept as minimal as possible.
        try:
            self._write_control(slot_name, self.CTRL_PROCS, f"{os.getpid()}")
        except Exception as err:
            # We can't even really log here as we're in the set-up
            # context of the new proces
            pass

    def setUp(self):
        """Creates the base cgroup tree if possible. Should be called once
        per process before using systemCall.
        Returns S_OK/S_ERROR.
        """
        try:
            self._prepare()
        except Exception as err:
            # The majority of CGroup failures will be RuntimeError
            # However we don't want any unexpected failure to crash the upstream module,
            # We just want to continue without cgroup support instead
            return S_ERROR(str(err))
        return S_OK()

    def systemCall(self, *args, **kwargs):
        """A proxy function for Subprocess.systemCall but will create a cgroup2 slot
        if the functionality is available. An optional ceParameters dictionary
        may be included, which will be searched for specific cgroup memory options.
        Returns the usual S_OK/S_ERROR from Subprocess.systemCall.
        """
        preexec_fn = None
        slot_name = f"subproc_{os.getpid()}_{self._subproc_num}"
        self._subproc_num += 1
        if self._ready:
            self.log.info(f"Creating slot cgroup {slot_name}")
            cores = None
            memory = None
            noswap = False
            if "ceParameters" in kwargs:
                if cpuLimit := kwargs["ceParameters"].get("CPULimit", None):
                    cores = float(cpuLimit)
                if memoryMB := int(kwargs["ceParameters"].get("MemoryLimitMB", 0)):
                    memory = memoryMB * 1024 * 1024
                if kwargs["ceParameters"].get("MemoryNoSwap", "no").lower() in ("yes", "true"):
                    noswap = True
            try:
                self.log.info(f"CGroup Limits, CPU: {cores}, Mem: {memory}, NoSwap: {noswap}")
                self._create_slot(slot_name, cores=cores, memory=memory, noswap=noswap)
                preexec_fn = functools.partial(CG2Manager._setup_subproc, self, slot_name)
            except Exception as err:
                self.log.warn("Failed to create slot cgroup:", str(err))
        kwargs["preexec_fn"] = preexec_fn
        kwargs.pop("ceParameters", None)
        res = Subprocess.systemCall(*args, **kwargs)
        if self._ready:
            self.log.info(f"Removing slot cgroup {slot_name}")
            try:
                oom_count = self._get_oom_count(slot_name)
                if oom_count:
                    # Child process triggered an OOM
                    # We can't readily report this upstream (child process will probably
                    # fail with an error code), so just log it and continue
                    self.log.info(f"OOM detected from child process (slot {slot_name})")
                self._remove_slot(slot_name)
            except Exception as err:
                self.log.warn(f"Failed to delete slot {slot_name} cgroup:", str(err))
        return res
