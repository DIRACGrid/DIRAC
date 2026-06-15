.. raw:: html

    <style> .subtitle { font-size:150%; line-height:2.0em; }
            p.first { background-color:lightgray; }
    </style>

.. role:: subtitle

.. _jdlDescription:

=========================================
Job Description Language Reference
=========================================

In this section all the attributes that can be used in the DIRAC JDL job descriptions are presented.

+---------------------+---------------------------------------------+-------------------------------------------------------------------------------------+
|                                                                                                                                                         |
| :subtitle:`The basic JDL parameters`                                                                                                                    |
|                                                                                                                                                         |
| These are the parameters giving the basic job description                                                                                               |
+---------------------+---------------------------------------------+-------------------------------------------------------------------------------------+
| **Attribute Name**  | **Description**                             | **Example**                                                                         |
+---------------------+---------------------------------------------+-------------------------------------------------------------------------------------+
| *Executable*        | Name of the executable file                 | Executable = ``"/bin/ls";``                                                         |
|                     |                                             |                                                                                     |
+---------------------+---------------------------------------------+-------------------------------------------------------------------------------------+
| *Arguments*         | String of arguments for the job             | Arguments = ``"-ltr";``                                                             |
|                     | executable                                  |                                                                                     |
+---------------------+---------------------------------------------+-------------------------------------------------------------------------------------+
| *StdError*          | Name of the file to get the standard error  | StdError = ``"std.err";``                                                           |
|                     | stream of the user application              |                                                                                     |
+---------------------+---------------------------------------------+-------------------------------------------------------------------------------------+
| *StdOutput*         | Name of the file to get the standard output | StdOutput = ``"std.out";``                                                          |
|                     | stream of the user application              |                                                                                     |
+---------------------+---------------------------------------------+-------------------------------------------------------------------------------------+
| *InputSandbox*      | A list of input sandbox files               | InputSandbox = ``{"jobScript.sh"};``                                                |
+---------------------+---------------------------------------------+-------------------------------------------------------------------------------------+
| *OutputSandbox*     | A list of output sandbox files              | OutputSandbox = ``{"std.err","std.out"};``                                          |
+---------------------+---------------------------------------------+-------------------------------------------------------------------------------------+
|                                                                                                                                                         |
|  :subtitle:`Job Requirements`                                                                                                                           |
|                                                                                                                                                         |
|  These parameters are interpreted as job requirements                                                                                                   |
+---------------------+---------------------------------------------+-------------------------------------------------------------------------------------+
| **Attribute Name**  | **Description**                             | **Example**                                                                         |
+---------------------+---------------------------------------------+-------------------------------------------------------------------------------------+
| *CPUTime*           | Max CPU time required by the job in         |   CPUTime = 18000;                                                                  |
|                     | HEPSPEC06 seconds                           |                                                                                     |
+---------------------+---------------------------------------------+-------------------------------------------------------------------------------------+
| *Site*              | Job destination site                        | Site = ``{"EGI.CPPM.fr"};``                                                         |
+---------------------+---------------------------------------------+-------------------------------------------------------------------------------------+
| *BannedSites*       | Sites where the job must not go             | BannedSites = ``{"EGI.LAPP.fr"};``                                                  |
+---------------------+---------------------------------------------+-------------------------------------------------------------------------------------+
| *GridCE*            | Job destination CE                          | GridCE = ``{"some.ce.lapp.fr"};``                                                   |
+---------------------+---------------------------------------------+-------------------------------------------------------------------------------------+
| *Platform*          | Target Operating System                     | Platform = ``"Linux_x86_64_glibc-2.17";``                                           |
+---------------------+---------------------------------------------+-------------------------------------------------------------------------------------+
|                                                                                                                                                         |
| :subtitle:`Data`                                                                                                                                        |
|                                                                                                                                                         |
| Describing job data                                                                                                                                     |
+---------------------+---------------------------------------------+-------------------------------------------------------------------------------------+
| **Attribute Name**  | **Description**                             | **Example**                                                                         |
+---------------------+---------------------------------------------+-------------------------------------------------------------------------------------+
| *InputData*         | Job input data files                        | InputData = ``{"/dirac/user/a/atsareg/data1",                                       |
|                     |                                             | "/dirac/user/a/atsareg/data1"};``                                                   |
+---------------------+---------------------------------------------+-------------------------------------------------------------------------------------+
| *InputDataModule*   | Job input data module                       | InputDataModule = ``"DIRAC.WorkloadManagementSystem.Client.InputDataResolution"``   |
+---------------------+---------------------------------------------+-------------------------------------------------------------------------------------+
| *InputDataPolicy*   | Job input data policy                       | InputDataPolicy = ``"DIRAC.WorkloadManagementSystem.Client.DownloadInputData";``    |
+---------------------+---------------------------------------------+-------------------------------------------------------------------------------------+
| *OutputData* [1]    | Job output data files                       | OutputData = ``{"output1","output2"};``                                             |
+---------------------+---------------------------------------------+-------------------------------------------------------------------------------------+
| *OutputPath* [2]    | The output data path in the File Catalog    | OutputPath = ``{"/myjobs/output"};``                                                |
+---------------------+---------------------------------------------+-------------------------------------------------------------------------------------+
| *OutputSE* [3]      | The output data Storage Element             | OutputSE = ``{"DIRAC-USER"};``                                                      |
+---------------------+---------------------------------------------+-------------------------------------------------------------------------------------+
|                                                                                                                                                         |
|  :subtitle:`Parametric Jobs`                                                                                                                            |
|                                                                                                                                                         |
|  Bulk submission parameters                                                                                                                             |
+---------------------+---------------------------------------------+-------------------------------------------------------------------------------------+
| **Attribute Name**  | **Description**                             | **Example**                                                                         |
+---------------------+---------------------------------------------+-------------------------------------------------------------------------------------+
| *Parameters*        | Number of parameters or a list of values    | Parameters = 10;                                                                    |
+---------------------+---------------------------------------------+-------------------------------------------------------------------------------------+
| *ParameterStart*    | Value of the first parameter                | ParameterStart = 0.;                                                                |
+---------------------+---------------------------------------------+-------------------------------------------------------------------------------------+
| *ParameterStep*     | Parameter increment                         | ParameterStep = 0.1; (default 0.)                                                   |
+---------------------+---------------------------------------------+-------------------------------------------------------------------------------------+
| *ParameterFactor*   | Parameter multiplier                        | ParameterFactor = 1.1; (default 1.)                                                 |
+---------------------+---------------------------------------------+-------------------------------------------------------------------------------------+

1. Elements of OutputData can be specified in several forms:

  - filenames; in this case files with the specified names will be looked for in the job directory and uploaded
    to a location specified by the OutputPath (see below);
  - filenames with wild cards, e.g. "*.log"; same after the filenames expansion;
  - output data specified in a form "LFN:/vo/full/destination/path/filename"; in this case the file "filename" in
    the job directory will be uploaded to the specified LFN path without taking into account the OutputPath.
    Note that "filename" here can be also specified with wild cards, e.g. "LFN:/vo/full/destination/path/*.log".

2. The OutputPath can be specified in several ways

  - if not given, it will be taken as the user's home directory + the job directory
    for example "/lhcb/user/a/atsareg/1234/1234567", where 1234567 is the job ID;
  - if given as a path starting with "/", it will be appended to the user's home
    directory, e.g. outputPath = "/my/analysis" will make output files to go to the
    "/lhcb/user/a/atsareg/my/analysis" directory
  - if given as "LFN:/output/path", it will be taken as an absolute path for
    output files in the logical namespace. It is the responsibility of the user to make
    sure that this path is accessible for writing for the user's data.

3. If multiple output SEs are specified, they will be tried one-by-one for each
   output file until a successful file upload.
