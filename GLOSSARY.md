# DIRAC

DIRAC is a framework for distributed computing: a layer between user communities (Virtual Organizations) and heterogeneous grid/cloud resources, providing job execution, data management, and resource status under a common interface.

Grid acronyms (CE, SE, LFN, PFN, WMS, VO, ...) are expanded in `docs/source/UserGuide/glossary.rst`.

## Language

### Workload management

**Job**:
The unit of user work submitted to DIRAC: a description stored in the job database and executed on a grid resource, carrying a major/minor status state machine.
_Avoid_: task (that is a Transformation concept)

**Pilot**:
A short placeholder job submitted to a Computing Element that starts before any user work exists and pulls a real Job via the Matcher.

**JobAgent**:
The agent running inside a pilot on a worker node that matches itself against the central Matcher and locally executes the Payload.
_Avoid_: agent (ambiguous with the server-side Agent)

**Payload**:
The user application actually executed inside a pilot's JobWrapper.
_Avoid_: application

**JobWrapper**:
The per-job harness that resolves input data, runs the Payload under a Watchdog, uploads the output sandbox, and reports job parameters and status.

**Watchdog**:
The monitor thread tracking CPU/memory/time of the running Payload and enforcing limits and signals.

**Matcher**:
The WMS service assigning waiting Jobs to Pilots, selecting from Task Queues.

**Task Queue (TQ)**:
A named queue of Jobs awaiting matching, with configurable shares between communities; unrelated to a Transformation Task.

**Sandbox**:
The small input/output files of a Job held by the central SandboxStore, as opposed to bulk input data managed by the DMS.

**Optimizer**:
A plugin in the chain processing freshly submitted Jobs before scheduling (job path, sanity, scheduling, input data).

### Data management

**StorageElement (SE)**:
A logical storage endpoint abstracting one or more protocol plug-ins; can be administratively banned for read/write operations.

**FileCatalog**:
The logical namespace mapping LFNs to Replicas; writes go through the Master catalog plug-in to keep multiple catalogs consistent.

**LFN**:
Logical File Name: the globally unique path of a file in the FileCatalog.
_Avoid_: path

**PFN**:
Physical File Name: the concrete URL of a Replica on a StorageElement.

**Replica**:
A copy of an LFN stored on one specific StorageElement.

**Request**:
A persistent, failover-safe container of Operations executed asynchronously by agents; the fault-tolerant path for all data operations.

**Operation**:
One unit of data work within a Request (e.g. ReplicateAndRegister, PhysicalRemoval), itself holding Files.

### Transformation system

**Transformation**:
A reusable recipe that automates production of Tasks over input data, turning high-level data processing into Jobs or Requests.

**Task**:
One instantiation produced by a Transformation, mapped onto a WMS Job or an RMS Request.
_Avoid_: job (a Task maps to a Job, it is not one)

### Resources and status

**Site**:
A grid site: one of the two element families of the resource topology, disjoint from Resource.

**Resource**:
A concrete service instance (CE, SE, catalog, ...): the second element family of the topology.

**Element**:
The umbrella term for a Site or a Resource in the resource topology.

**Policy**:
A rule evaluating a status (Active/Degraded/Banned/Probing) for Sites and Resources; propagation pushes status changes down the topology.

### Infrastructure

**ComputingElement (CE)**:
The base class abstracting a compute resource: generates the CE description used for job matching and evaluates available CPU slots.

**Proxy**:
A short-lived X.509 credential, optionally VOMS-extended for a Virtual Organization.

**Shifter**:
A named privileged identity (e.g. JobSubmission, Production) whose Proxy is assumed server-side on behalf of the community.

**Agent**:
A server-side daemon running a periodic `execute()` cycle as part of a System.
_Avoid_: conflating with the worker-node JobAgent

**Service**:
A long-running RPC server component of a System (Tornado or DISET based).

**System**:
A subsystem bundling Agents, Services and databases (WMS, DMS, RMS, RSS, Framework, ...).

**Configuration (CS)**:
The central distributed configuration tree read via `gConfig`; sections like `/LocalSite` and `/Resources/Computing` are load-bearing for matching and credentials.
