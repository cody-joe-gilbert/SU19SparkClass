The bash scripts will Get/download the zip files from the CFPB's website.

There are two sets of download, unzip, and concatenate scripts.  This is because there are two ways of representing the data set.  The representation with larger amounts of memory has extensive string representations of the data.  The representation that takes less memory is an integer representation of the text options. 

How to execute the following scripts:

```chmod +x *.bash```

```./<script_name> netID```

Example:

````./get_hmda_code_zip_files.bash jjl359````

This should download the zip and subsequently unzip the LAR files into your scratch workspace.
