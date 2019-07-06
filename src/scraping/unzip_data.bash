#!/bin/bash

mkdir /scratch/"$@"/hmda_script/
mkdir /scratch/"$@"/hmda_script/data-files

for i in {2007..2017};
do
  prefix="hmda_"
  ext=".zip"
  csv=".csv"

  echo $prefix$i$ext

  unzip -d /scratch/"$@"/hmda_script/data-files/ $prefix$i$ext
  mv /scratch/"$@"/hmda_script/data-files/hmda_lar.csv /scratch/"$@"/hmda_script/data-files/$prefix$i$csv

done
