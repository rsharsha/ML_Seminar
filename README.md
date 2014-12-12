The application contains two main classes HadoopMain and Main. The Main class is for connecting to database and write the data into data.txt file. The data.txt file needs to be uploaded to the HDFS file system into /input directory. The HadoopMain class contains the MapReduce code which clusters the data from data.txt file. The output from the MapReduce is stored in /output folder. 

In HadoopMain class, we need to set the value for ITERATIONS, DATA_SIZE and CLUSTER_SIZE. ITERATIONS is for number of iterations the MapReduce algorithm should run. DATA_SIZE is for number of rows in /input/data.txt file (This value is used for identifying centroids). CLUSTER_SIZE is for number of clusters needed. 

A sample data.txt file is also uploaded in the project folder. The values are set for the above constants for this file.
