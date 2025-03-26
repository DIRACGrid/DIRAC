import uuid

from DIRAC import S_ERROR, S_OK
from DIRAC.Resources.Computing.ComputingElement import ComputingElement

# Strategies are not used yet, just an idea
SENDING_STRATEGIES = {
    "NO_MORE_JOBS_FIT",
    "MAX_TIME_SINCE_FIRST",
    "MAX_TIME_BETWEEN_SUBMISSIONS",
}

STORING_STRATEGIES = {
    "NO_STRATEGY",
    "SAME_JOB_TYPE",
}

# SHELL code that bundles all wrappers
BUNDLE_STRING = """\
#!/bin/bash
set -e

BASEDIR=${{PWD}}
INPUT={inputs}

get_id() {{
    basename ${{1}} .json
}}

run_task() {{
    local input=$1
    local task_id=$(get_id ${{input}})

    >&2 echo "Executing task ${{task_id}}"
    >&2 {command} ${{BASEDIR}}/${{input}} >task_${{task_id}}.log 2>&1  &
    local task_pid=$!

    >&2 echo "Task ${{task_id}} waiting for pid ${{task_pid}}..."
    wait ${{task_pid}} ; local task_status=$?

    # report status
    echo "${{task_id}} ${{task_pid}} ${{task_status}}" | tee task_${{task_id}}.status
}}

# execute tasks
for input in ${{INPUT}}; do 
    [ -f "$input" ] || break
    taskdir="task_$(get_id ${{input}})"
    mkdir ${{taskdir}} && cd "$_" &&
        run_task ${{input}} >> ${{BASEDIR}}/tasks_status.log &
    cd ${{BASEDIR}}
done

# wait for all tasks
wait
"""

class BundleComputingElement(ComputingElement):
    def __init__(self, ceUniqueID):
        """Standard constructor."""
        super().__init__(ceUniqueID)

        self.jobToBundle = {}
        self.bundles = {}
        self.bundleReady = {}

        # These are just ideas, could be interesting to take into account
        self.timeout = -1
        self.max_time_between_submissions = -1

        self.storeStrategies = []
        self.sendStrategies = []

        # Currently this has to be hard-coded. 
        # It must either be generated dynamically through the ceDict or use
        #  another Inner one like PoolCE does.
        self.ce = None

        self.log.setLevel("DEBUG")

    def _storeOnBundle(self, bundleId, job, n_processors):
        self.bundles[bundleId]["Jobs"].append(job)
        self.bundles[bundleId]["ProcessorSum"] += n_processors
    
    def _storeJob(self, jobID, executable, ceDict, n_processors, proxy=None, inputs=None):
        bundle_id = f"{ceDict['Site']}:{ceDict['GridCE']}:{ceDict['Queue']}"
        
        if bundle_id not in self.bundles:
            self._initBundle(bundle_id, ceDict)

        bundle = self.bundles[bundle_id]
        job = {
            "ID": jobID,
            "Executable": executable,
            "Inputs": inputs,
            "Proxy": proxy
        }

        # Is the bundle ready for execution in this CE?
        if bundle["ProcessorSum"] + n_processors >= bundle["MaxProcessors"]:
            # Clear bundle related to the CE
            self._initBundle(bundle_id, ceDict)

            # Add the job to the bundle ready if it fits. 
            # Otherwise, add it to the storage
            if bundle["ProcessorSum"] + n_processors == bundle["MaxProcessors"]:
                bundle["Jobs"].append(job)
            else:
                self._storeOnBundle(bundle_id, job, n_processors)

            # Make the bundle ready for execution
            self.bundleReady = bundle

        else:
            # Just store it
            self._storeOnBundle(bundle_id, job, n_processors)

        self.log.debug("Current bundle status: ", self.bundles)

        return bundle_id

    def _initBundle(self, bundleId, ceDict, startingProcessors=0):
        self.bundles[bundleId] = {}
        self.bundles[bundleId]["Jobs"] = []
        self.bundles[bundleId]["ProcessorSum"] = startingProcessors
        self.bundles[bundleId]["MaxProcessors"] = ceDict["NumberOfProcessors"]
        self.bundles[bundleId]["LastAddedJobTimestamp"] = 0
        self.bundles[bundleId]["CEDict"] = ceDict
    
    def submitJob(self, executableFiles, proxy=None, numberOfProcessors=1, jobDesc=None, inputs=None):
        jobID = jobDesc["jobID"]
        resourceParams = jobDesc["resourceParams"]
        
        bundleID = self._storeJob(jobID, executableFiles, resourceParams, numberOfProcessors, proxy=proxy, inputs=inputs)

        if not self.bundleReady:
            self.log.info(f"Job {jobID} stored successfully in bundle: ", bundleID)
            return S_OK()

        executablePath, proxy, grouped_inputs = self._wrap_bundle(command="bash")

        self.log.info("Submitting job to CE: ", self.ce.ceName)

        # result = self.ce.submitJob(executablePath, proxy, inputs=grouped_inputs)
        result = {}
        
        self.bundleReady = None
        
        return S_OK(result)
    
    def _wrap_bundle(self, command):
        wrap_string = BUNDLE_STRING
        bundle_inputs_string = "("
        inputs = []

        filepath = f"/tmp/BundledJobs_{uuid.uuid4()}"
        for job in self.bundleReady["Jobs"]:
            self.jobToBundle[job["ID"]] = filepath
            bundle_inputs_string += job["Executable"].replace(" ", "\ ") + " "
            
            # Add the original executable as an input, as well as the original inputs
            inputs.append(job["Executable"])
            inputs += job["Inputs"]
        
        bundle_inputs_string = bundle_inputs_string[:-1] + ")"
        
        wrap_string = wrap_string.format(inputs=bundle_inputs_string, command=command)

        with open(filepath, "x") as fd:
            fd.write(wrap_string)

        self.log.debug("Bundle created:\n", wrap_string)
        self.log.debug("Inputs used:", inputs)

        return filepath, self.bundleReady["Jobs"][0]["Proxy"], inputs

    #
    # BIG ISSUE HERE 
    # ----------------
    # If we accept job bundling from multiple CEs, there is no way of obtaining the status of
    # the CE, because it's different depending of the job bundle you are asking about
    #
    # A way of circumvent this is enforcing the usage of just a singular Inner CE
    #
    def getDescription(self):
        pass

    def getCEStatus(self):
        pass

if __name__ == "__main__":
    from DIRAC.Resources.Computing.InProcessComputingElement import InProcessComputingElement
    
    bundleCE = BundleComputingElement("BundleCE")
    innerCE = InProcessComputingElement("InnerCE")
    bundleCE.ce = innerCE

    max_processors = 3

    CE_DICT = {
        'NumberOfProcessors': max_processors, 
        'CPUTime': 3456, 
        'FileCatalog': 'FileCatalog',
        'CPUTimeLeft': 10000, 
        'WaitingToRunningRatio': 0.5, 
        'MaxWaitingJobs': 1, 
        'MaxTotalJobs': 366, 
        'CEType': 'AREX', 
        'architecture': 'x86_64', 
        'VO': 'lhcb', 
        'VirtualOrganization': 'lhcb', 
        'MaxRAM': 16000,
        'SubmissionMode': 'Direct', 
        'Preamble': 'source /cvmfs/lhcb.cern.ch/lhcbdirac/diracosrc', 
        'XRSLExtraString': '(runtimeEnvironment="ENV/SINGULARITY" "/gpfs/projects/sall73/cvmfs/lhcb.cern.ch/containers/os-base/alma9-devel/prod/amd64/" "" "/apps/GPP/SINGULARITY/3.11.5/bin/singularity")', 
        'Port': 8443, 
        'Platform': 'skylake-any', 
        'Timeout': 300, 
        'ARCLogLevel': 'DEBUG', 
        'MaxCPUTime': 3456, 
        'CPUNormalizationFactor': 30, 
        'Tag': ['MultiProcessor'], 
        'Queue': 'nordugrid-slurm-gp_resa', 
        'GridCE': 'lhcbvs02.ific.uv.es', 
        'Site': 'DIRAC.MareNostrum.es', 
        'GridEnv': '', 
        'Setup': 'MyDIRAC-Production', 
        'RequiredTag': [], 
        'DIRACVersion': 'v8.0.55', 
        'ReleaseVersion': 'v8.0.55', 
        'RemoteExecution': True
    }
    
    dummy_proxy = """\
-----BEGIN CERTIFICATE-----
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
-----END CERTIFICATE-----
-----BEGIN PRIVATE KEY-----
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
-----END PRIVATE KEY-----
-----BEGIN CERTIFICATE-----
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
-----END CERTIFICATE-----
"""

    for i in range(max_processors*2):
        executable_file = f"./test/job_{i}.py"
        inputs = [f"./test/wrapper_{i}.py", f"./test/wrapper_{i}.json"]
        
        jobDesc = {"jobID": i, "resourceParams": CE_DICT}
        bundleCE.submitJob(f"./test/job_{i}.py", dummy_proxy, numberOfProcessors=1, jobDesc=jobDesc, inputs=inputs)