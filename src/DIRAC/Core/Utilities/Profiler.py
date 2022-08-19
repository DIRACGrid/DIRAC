"""
Profiling class for updated information on process status
"""
import datetime
import errno
import psutil

from DIRAC import gLogger, S_OK, S_ERROR
from DIRAC.Core.Utilities.DErrno import EEZOMBIE, EENOPID, EEEXCEPTION


def checkInvocation(func):
    """Decorator for invoking psutil methods"""

    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except psutil.ZombieProcess as e:
            gLogger.error("Zombie process: %s" % e)
            return S_ERROR(EEZOMBIE, "Zombie process: %s" % e)
        except psutil.NoSuchProcess as e:
            gLogger.error("No such process: %s" % e)
            return S_ERROR(errno.ESRCH, "No such process: %s" % e)
        except psutil.AccessDenied as e:
            gLogger.error("Access denied: %s" % e)
            return S_ERROR(errno.EPERM, "Access denied: %s" % e)
        except Exception as e:  # pylint: disable=broad-except
            gLogger.error(e)
            return S_ERROR(EEEXCEPTION, e)

    return wrapper


class Profiler:
    """
    Class for profiling both general stats about a machine and individual processes.
    Every instance of this class is associated to a single process by using its PID.
    Calls to the different methods of the class will return the current state of the process.
    """

    def __init__(self, pid=None):
        """
        :param str pid: PID of the process to be profiled
        """
        self.process = None
        if pid:
            try:
                self.process = psutil.Process(int(pid))
            except psutil.NoSuchProcess as e:
                gLogger.error("No such process: %s" % e)

    def pid(self):
        """
        Returns the process PID
        """
        if self.process:
            return S_OK(self.process.pid)
        else:
            gLogger.error("No PID of process to profile")
            return S_ERROR(EENOPID, "No PID of process to profile")

    @checkInvocation
    def status(self):
        """Returns the process status"""
        result = self.process.status()
        return S_OK(result)

    @checkInvocation
    def runningTime(self):
        """
        Returns the uptime of the process
        """
        start = datetime.datetime.fromtimestamp(self.process.create_time())
        result = (datetime.datetime.now() - start).total_seconds()
        return S_OK(result)

    @checkInvocation
    def memoryUsage(self, withChildren=False):
        """
        Returns the memory usage of the process in MB
        """
        # Information is returned in bytes
        rss = self.process.memory_info().rss
        if withChildren:
            for child in self.process.children(recursive=True):
                rss += child.memory_info().rss
        # converted to MB
        return S_OK(rss / float(2**20))

    @checkInvocation
    def vSizeUsage(self, withChildren=False):
        """
        Returns the memory usage of the process in MB
        """
        # Information is returned in bytes
        vms = self.process.memory_info().vms
        if withChildren:
            for child in self.process.children(recursive=True):
                vms += child.memory_info().vms
        # converted to MB
        return S_OK(vms / float(2**20))

    @checkInvocation
    def numThreads(self, withChildren=False):
        """
        Returns the number of threads the process is using
        """
        nThreads = self.process.num_threads()
        if withChildren:
            for child in self.process.children(recursive=True):
                nThreads += child.num_threads()
        return S_OK(nThreads)

    @checkInvocation
    def cpuPercentage(self, withChildren=False):
        """
        Returns the percentage of cpu used by the process
        """
        cpuPercentage = self.process.cpu_percent()
        if withChildren:
            for child in self.process.children(recursive=True):
                cpuPercentage += child.cpu_percent()
        return S_OK(cpuPercentage)

    @checkInvocation
    def cpuUsageUser(self, withChildren=False, withTerminatedChildren=False):
        """
        Returns the percentage of cpu used by the process
        """
        cpuUsageUser = self.process.cpu_times().user
        childrenUser = 0
        oldChildrenUser = 0
        if withChildren:  # active children
            for child in self.process.children(recursive=True):
                childrenUser += child.cpu_times().user
            gLogger.debug("CPU user (process, children)", f"({cpuUsageUser:.1f}s, {childrenUser:.1f}s)")
        if withTerminatedChildren:  # all terminated children of the root process
            for child in self.process.children(recursive=True):
                oldChildrenUser += child.cpu_times().children_user
            gLogger.debug("CPU user (process, old children)", f"({cpuUsageUser:.1f}s, {oldChildrenUser:.1f}s)")
        else:
            gLogger.debug("CPU user", "%.1fs" % cpuUsageUser)
        return S_OK(cpuUsageUser + childrenUser + oldChildrenUser)

    @checkInvocation
    def cpuUsageSystem(self, withChildren=False, withTerminatedChildren=False):
        """
        Returns the percentage of cpu used by the process
        """
        cpuUsageSystem = self.process.cpu_times().system
        childrenSystem = 0
        oldChildrenSystem = 0
        if withChildren:  # active children
            for child in self.process.children(recursive=True):
                childrenSystem += child.cpu_times().system
            gLogger.debug("CPU user (process, children)", f"({cpuUsageSystem:.1f}s, {childrenSystem:.1f}s)")
        if withTerminatedChildren:  # all terminated children of the root process
            for child in self.process.children(recursive=True):
                oldChildrenSystem += child.cpu_times().children_system
            gLogger.debug("CPU user (process, old children)", f"({cpuUsageSystem:.1f}s, {oldChildrenSystem:.1f}s)")
        else:
            gLogger.debug("CPU user", "%.1fs" % cpuUsageSystem)
        return S_OK(cpuUsageSystem + childrenSystem + oldChildrenSystem)

    def getAllProcessData(self, withChildren=False, withTerminatedChildren=False):
        """
        Returns data available about a process
        """
        data = {}

        data["datetime"] = datetime.datetime.utcnow()
        data["stats"] = {}

        result = self.pid()
        if result["OK"]:
            data["stats"]["pid"] = result["Value"]

        result = self.status()
        if result["OK"]:
            data["stats"]["status"] = result["Value"]

        result = self.runningTime()
        if result["OK"]:
            data["stats"]["runningTime"] = result["Value"]

        result = self.memoryUsage(withChildren)
        if result["OK"]:
            data["stats"]["memoryUsage"] = result["Value"]

        result = self.vSizeUsage(withChildren)
        if result["OK"]:
            data["stats"]["vSizeUsage"] = result["Value"]

        result = self.numThreads(withChildren)
        if result["OK"]:
            data["stats"]["threads"] = result["Value"]

        result = self.cpuPercentage(withChildren)
        if result["OK"]:
            data["stats"]["cpuPercentage"] = result["Value"]

        result = self.cpuUsageUser(withChildren, withTerminatedChildren)
        if result["OK"]:
            data["stats"]["cpuUsageUser"] = result["Value"]

        result = self.cpuUsageSystem(withChildren, withTerminatedChildren)
        if result["OK"]:
            data["stats"]["cpuUsageSystem"] = result["Value"]

        return S_OK(data)
