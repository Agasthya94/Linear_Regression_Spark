1. Download the input files and place them on local system of cloudera
2. Run the job using the following command
spark-submit <filename.py> <input file path>

Instructions to run on DSBA cluster
------------------------------------
1. Open a terminal and connect to the DSBA cluster
2. Create a new Directory in the cluster
3. Open another terminal and copy the files to the cluster using the following command
scp <filename> <local path> <username>@dsba-hadoop.uncc.edu:<path of the cluster>
4. Open the first terminal and submit the job using the following command
spark-submit <filename.py> <input file path>
