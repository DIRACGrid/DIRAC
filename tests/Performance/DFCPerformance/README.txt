All this set of script was used to test the managers using Foreign Keys and Stored procedure,
and specifically the performance aspect. It was used prior the migration of LHCb from the LFC to DFC.

It is quite a heavy testing procedure, so it is not to be run in a jenkins or so. The idea is the following:

* you generate DB content using one of the scripts in generateDB. These scripts generate an SQL file
  that you then have to load. They were written to generate data following the schema of the file
  catalog with PS and SP, but could be easily adapted to generate for other schema. The paths that 
  are generated follow some logic, so that in the client, we can randomly regenerate these paths.
* You can run multiple DFC service on the same servers, but you need to specify the hostname and the list of ports
  in the perf scripts and the extraction script. 
* test the performance using readPerf/writePerf/mixedPerf. There are some options to tune in these scripts,
  and they have to match the options you used to generate the DB. Also you have to say on which server is the DFC.
  These scripts produce two files, time.txt and clock.txt, which contains the time measurement to be analyzed.
* If you want to massively hammer the DFC, you can submit many jobs that will actually run the different perf scripts.
  There is a set of script to help you with that. 'submitJobs' will submit all the jobs. 'retrieveResults' will loop
  through the jobs and fetch their results. 'extractResult.sh' will merge all the results of all the jobs, and output
  6 files (1 for each of read/write/delete and each of successful/timeout call). 
* You can then plot the results. For this, you can use 'make_all_plots', which will generate plots for each type of calls
  read/write/delete with and without max. Or you can use 'make_plot', which can take many more options.
  
 In any case, read the doc of each script individually.