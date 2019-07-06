#!/bin/bash


for i in {2007..2017};
do
  prefix="hmda_"
  ext=".zip"
  csv=".csv"

  echo $prefix$i$ext

  unzip -d /scratch/jjl359/hmda_script/data-files/ $prefix$i$ext
  mv /scratch/jjl359/hmda_script/data-files/hmda_lar.csv /scratch/jjl359/hmda_script/data-files/$prefix$i$csv

done