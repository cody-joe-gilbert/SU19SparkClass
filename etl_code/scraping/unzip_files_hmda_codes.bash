#!/bin/bash

mkdir /scratch/"$@"/hmda_code_files
mkdir /scratch/"$@"/hmda_code_files/data-files

for i in {2007..2017};
do
  prefix="hmda_"
  ext=".zip"
  csv=".csv"
  end_name="_nationwide_all-records_codes.csv"
  
  echo $prefix$i$ext

  unzip -d /scratch/"$@"/hmda_code_files/data-files/ /scratch/"$@"/hmda_code_files/zip-files/$prefix$i$ext
  mv /scratch/"$@"/hmda_code_files/data-files/hmda_$i$end_name /scratch/"$@"/hmda_code_files/data-files/$prefix$i$csv

done
