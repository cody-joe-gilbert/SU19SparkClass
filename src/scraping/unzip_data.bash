#!/bin/bash

mkdir /scratch/"$@"/hmda_script2/
mkdir /scratch/"$@"/hmda_script2/data-files

for i in {2007..2017};
do
  prefix="hmda_"
  ext=".zip"
  csv=".csv"

  echo $prefix$i$ext

  unzip -d /scratch/"$@"/hmda_script2/data-files/ /scratch/"$@"/hmda_script2/zip-files/$prefix$i$ext
  mv /scratch/"$@"/hmda_script2/data-files/hmda_lar.csv /scratch/"$@"/hmda_script2/data-files/$prefix$i$csv

done
