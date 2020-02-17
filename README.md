# do-not-call
Generates a binary map file from raw do-not-call file downloaded from the Do Not Call registry.

#Here are the steps to use it:

1. Compile this project via the menu item `Build/Build Artifacts`
2. Make sure to use for the run a host with more than 30GB free RAM
3. Place in the same folder:
  1. The resulting jar file: `do-not-call.jar`
  2. The `run_do_not_call.sh` script from this project
  3. The raw input file as downloaded from the do-not-call registry `https://telemarketing.donotcall.gov/`
4. Modify the `inputFileName` and the `doNotCallDate` arguments in run_do_not_call.sh script to match the input file name and the date
5. Run the run_do_not_call.sh script from command-line. It will take about an hour to finish and generate the `do-not-call._.<doNotCallDate>` binary file.
