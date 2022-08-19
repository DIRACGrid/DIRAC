""" Palette is a tool to generate colors for various Graphs plots and legends

    The DIRAC Graphs package is derived from the GraphTool plotting package of the
    CMS/Phedex Project by ... <to be added>
"""
import hashlib

from DIRAC.WorkloadManagementSystem.Client import JobStatus
from DIRAC.WorkloadManagementSystem.Client import JobMinorStatus

job_status_palette = {
    JobStatus.RECEIVED: "#D9E7F8",
    JobStatus.CHECKING: "#FAFAFA",
    JobStatus.STAGING: "#6190CD",
    JobStatus.WAITING: "#004EFF",
    JobStatus.MATCHED: "#FEF7AA",
    JobStatus.RUNNING: "#FDEE65",
    JobStatus.COMPLETING: "#FFAF55",
    JobStatus.STALLED: "#BC5757",
    JobStatus.COMPLETED: "#00FF21",
    JobStatus.DONE: "#238802",
    JobStatus.FAILED: "#FF0000",
    JobStatus.KILLED: "#111111",
}

job_minor_status_palette = {
    JobMinorStatus.APP_ERRORS: "#BC2133",
    JobMinorStatus.EXCEPTION_DURING_EXEC: "#AA240C",
    JobMinorStatus.EXEC_COMPLETE: "#338B39",
    JobMinorStatus.ILLEGAL_JOB_JDL: "#D96C00",
    JobMinorStatus.INPUT_NOT_AVAILABLE: "#2822A6",
    JobMinorStatus.INPUT_DATA_RESOLUTION: "#FFBE94",
    JobMinorStatus.DOWNLOADING_INPUT_SANDBOX: "#586CFF",
    JobMinorStatus.INPUT_CONTAINS_SLASHES: "#AB7800",
    JobMinorStatus.INPUT_INCORRECT: "#6812D6",
    JobMinorStatus.JOB_WRAPPER_INITIALIZATION: "#FFFFCC",
    JobMinorStatus.JOB_EXCEEDED_WALL_CLOCK: "#FF33CC",
    JobMinorStatus.JOB_INSUFFICIENT_DISK: "#33FFCC",
    JobMinorStatus.JOB_EXCEEDED_CPU: "#AABBCC",
    "No Ancestors Found For Input Data": "#BDA544",
    JobMinorStatus.NO_CANDIDATE_SITE_FOUND: "#E2FFBC",
    JobMinorStatus.PENDING_REQUESTS: "#52FF4F",
    JobMinorStatus.RECEIVED_KILL_SIGNAL: "#FF312F",
    JobMinorStatus.WATCHDOG_STALLED: "#FFCC99",
}

miscelaneous_pallette = {"Others": "#666666", "NoLabels": "#0025AD", "Total": "#00FFDC", "Default": "#FDEE65"}

country_palette = {
    "France": "#73C6BC",
    "UK": "#DCAF8A",
    "Spain": "#C2B0E1",
    "Netherlands": "#A9BF8E",
    "Germany": "#800000",
    "Russia": "#00514A",
    "Italy": "#004F00",
    "Switzerland": "#433B00",
    "Poland": "#528220",
    "Hungary": "#825CE2",
    "Portugal": "#009182",
    "Turkey": "#B85D00",
}


class Palette:
    def __init__(self, palette={}, colors=[]):

        self.palette = country_palette
        self.palette.update(job_status_palette)
        self.palette.update(miscelaneous_pallette)
        self.palette.update(job_minor_status_palette)

    def setPalette(self, palette):
        self.palette = palette

    def setColor(self, label, color):
        self.palette[label] = color

    def addPalette(self, palette):
        self.palette.update(palette)

    def getColor(self, label):

        if label in self.palette:
            return self.palette[label]
        else:
            return self.generateColor(label)

    def generateColor(self, label):

        myMD5 = hashlib.md5()
        myMD5.update(label.encode())
        hexstring = myMD5.hexdigest()
        color = "#" + hexstring[:6]
        return color
