#!/usr/bin/env bash

#directorio in/out
jar=showscount-0.0.3.jar
input_dir=/user/bberrios/twitter/raw_week1/*
output_dir=/user/bberrios/output/twitter/raw/java-one-week-filtered
output_java_shows=one_week_java_filtered
filter=concert\|band\|music

#Eliminar el directorio de output
hadoop fs -rm -r $output_dir

hadoop jar $jar showscount.ShowCount $input_dir $output_dir "$filter"
hadoop fs -cat $output_dir/* > $output_java_shows
