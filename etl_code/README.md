The scripts are a pipeline that are meant to be executed in the following order: 

  1.  Unzip the files into the target folder 
  2.  Concatenate the files together 

There are two sets of download, unzip, and concatenate scripts.  This is because there are two ways of representing the data set.  The representation with larger amounts of memory has extensive string representations of the data.  The representation that takes less memory is an integer representation of the text options. 

How to execute the following scripts:

```chmod +x *.bash```

```./<script_name> netID```

Example:

````./unzip_data.bash jjl359````
````./unzip_files_hmda_codes.bash jjl359````

This should download the zip and subsequently unzip the LAR files into your scratch workspace.

In order to put together all 11 files together, use the ````concatenate```` bash scripts in the folder where the files live: 

````./concatenate.bash````
````./concatenate_hmda_codes.bash````

This will ensure all the files are combined into one set, with only one set of headers. 

 
Once the files are ready in scratch, then they can be put into hdfs by: 

````hdfs dfs -put <filename> /user/<username>/<target-folder>````

