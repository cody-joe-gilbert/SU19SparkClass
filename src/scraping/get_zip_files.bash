#!/bin/bash

mkdir /scratch/"$@"/hmda_script/
mkdir /scratch/"$@"/hmda_script/zip-files

curl -o /scratch/"$@"/hmda_script/zip-files/hmda_2007.zip "https://s3.amazonaws.com/cfpb-public-hmda-data/d67aa2ac2da1a6e3ce8da9d58c5707a3.zip"
curl -o /scratch/"$@"/hmda_script/zip-files/hmda_2008.zip "https://s3.amazonaws.com/cfpb-public-hmda-data/42009dfe11c820520533f9b7bd80c1f3.zip"
curl -o /scratch/"$@"/hmda_script/zip-files/hmda_2009.zip "https://s3.amazonaws.com/cfpb-public-hmda-data/f205482ae13d2a4803e0572a816ea976.zip"
curl -o /scratch/"$@"/hmda_script/zip-files/hmda_2010.zip "https://s3.amazonaws.com/cfpb-public-hmda-data/fc84404b661c40bbaec9bb6bf324db59.zip"
curl -o /scratch/"$@"/hmda_script/zip-files/hmda_2011.zip "https://s3.amazonaws.com/cfpb-public-hmda-data/1da11a41641405e0734f192d9c326d37.zip"
curl -o /scratch/"$@"/hmda_script/zip-files/hmda_2012.zip "https://s3.amazonaws.com/cfpb-public-hmda-data/d0d8524d6fc1b7de392f71cf18d2d4e2.zip"
curl -o /scratch/"$@"/hmda_script/zip-files/hmda_2013.zip "https://s3.amazonaws.com/cfpb-public-hmda-data/10c85fa4ea4202e1a0c3d278c8556001.zip"
curl -o /scratch/"$@"/hmda_script/zip-files/hmda_2014.zip "https://s3.amazonaws.com/cfpb-public-hmda-data/a5bdd779981cd7323bc07b6f95512aad.zip"
curl -o /scratch/"$@"/hmda_script/zip-files/hmda_2015.zip "https://s3.amazonaws.com/cfpb-public-hmda-data/803a494a7c1cf1289bbaf3c034669f56.zip"
curl -o /scratch/"$@"/hmda_script/zip-files/hmda_2016.zip "https://s3.amazonaws.com/cfpb-public-hmda-data/e37bb84f7b92e63ef9e2c95fc1347f91.zip"
curl -o /scratch/"$@"/hmda_script/zip-files/hmda_2017.zip "https://s3.amazonaws.com/cfpb-public-hmda-data/88b1e23eff98aa8517f0aa10fc400010.zip"

