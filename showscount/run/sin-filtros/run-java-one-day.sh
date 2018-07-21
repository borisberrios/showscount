#!/usr/bin/env bash

#directorio in/out
jar=showscount-0.0.3.jar
input_dir=/user/bberrios/twitter/raw_one_day/*
output_dir=/user/bberrios/output/twitter/raw/java-one-day
output_java_shows=one_day_java

#Eliminar el directorio de output
hadoop fs -rm -r $output_dir

hadoop jar $jar showscount.ShowCount $input_dir $output_dir
hadoop fs -cat $output_dir/* > $output_java_shows
