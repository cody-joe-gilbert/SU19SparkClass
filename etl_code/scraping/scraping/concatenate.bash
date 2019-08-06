#!/bin/bash

{ cat hmda_2007.csv; sed '1d' hmda_2008.csv; sed '1d' hmda_2009.csv; sed '1d' hmda_2010.csv; sed '1d' hmda_2011.csv; sed '1d' hmda_2012.csv; sed '1d' hmda_2013.csv; sed '1d' hmda_2014.csv; sed '1d' hmda_2015.csv; sed '1d' hmda_2016.csv;  sed '1d' hmda_2017.csv;} > HMDA_2007_to_2017.csv
