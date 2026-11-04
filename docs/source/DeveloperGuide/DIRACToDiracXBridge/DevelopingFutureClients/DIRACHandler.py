# DIRAC/WorkloadManagementSystem/Service/JobAnnotatorHandler.py
from DIRAC import S_ERROR, S_OK
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC.Core.Utilities.ObjectLoader import ObjectLoader


class JobAnnotatorHandler(RequestHandler):
    @classmethod
    def initializeHandler(cls, infoDict):
        result = ObjectLoader().loadObject("WorkloadManagementSystem.DB.JobAnnotatorDB", "JobAnnotatorDB")
        if not result["OK"]:
            return result
        cls.annotatorDB = result["Value"]()
        return S_OK()

    types_addAnnotation = [[str, int], str]

    def export_addAnnotation(self, jobID, annotation):
        """Attach an annotation to a job."""
        return self.annotatorDB.addAnnotation(int(jobID), annotation)

    types_getAnnotations = [[str, int]]

    def export_getAnnotations(self, jobID):
        """Return the list of annotations of a job."""
        return self.annotatorDB.getAnnotations(int(jobID))
