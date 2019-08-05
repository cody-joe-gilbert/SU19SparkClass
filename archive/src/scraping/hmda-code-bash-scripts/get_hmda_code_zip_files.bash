#!/bin/bash

mkdir /scratch/"$@"/hmda_code_files/
mkdir /scratch/"$@"/hmda_code_files/zip-files

curl -o /scratch/"$@"/hmda_code_files/zip-files/hmda_2007.zip "https://files.consumerfinance.gov/hmda-historic-loan-data/hmda_2007_nationwide_all-records_codes.zip"
curl -o /scratch/"$@"/hmda_code_files/zip-files/hmda_2008.zip "https://files.consumerfinance.gov/hmda-historic-loan-data/hmda_2008_nationwide_all-records_codes.zip"
curl -o /scratch/"$@"/hmda_code_files/zip-files/hmda_2009.zip "https://files.consumerfinance.gov/hmda-historic-loan-data/hmda_2009_nationwide_all-records_codes.zip"
curl -o /scratch/"$@"/hmda_code_files/zip-files/hmda_2010.zip "https://files.consumerfinance.gov/hmda-historic-loan-data/hmda_2010_nationwide_all-records_codes.zip"
curl -o /scratch/"$@"/hmda_code_files/zip-files/hmda_2011.zip "https://files.consumerfinance.gov/hmda-historic-loan-data/hmda_2011_nationwide_all-records_codes.zip"
curl -o /scratch/"$@"/hmda_code_files/zip-files/hmda_2012.zip "https://files.consumerfinance.gov/hmda-historic-loan-data/hmda_2012_nationwide_all-records_codes.zip"
curl -o /scratch/"$@"/hmda_code_files/zip-files/hmda_2013.zip "https://files.consumerfinance.gov/hmda-historic-loan-data/hmda_2013_nationwide_all-records_codes.zip"
curl -o /scratch/"$@"/hmda_code_files/zip-files/hmda_2014.zip "https://files.consumerfinance.gov/hmda-historic-loan-data/hmda_2014_nationwide_all-records_codes.zip"
curl -o /scratch/"$@"/hmda_code_files/zip-files/hmda_2015.zip "https://files.consumerfinance.gov/hmda-historic-loan-data/hmda_2015_nationwide_all-records_codes.zip"
curl -o /scratch/"$@"/hmda_code_files/zip-files/hmda_2016.zip "https://files.consumerfinance.gov/hmda-historic-loan-data/hmda_2016_nationwide_all-records_codes.zip"
curl -o /scratch/"$@"/hmda_code_files/zip-files/hmda_2017.zip "https://files.consumerfinance.gov/hmda-historic-loan-data/hmda_2017_nationwide_all-records_codes.zip"
