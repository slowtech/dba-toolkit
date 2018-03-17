#!/bin/bash

if [ $# -ne 2 ];then
  echo "sh $0 table_directory table_name"
  echo "Usage:sh $0 /var/lib/mysql/db1 t1"
  exit
fi

table_directory=$1
target_table=$2

function get_file_size() {
    local file=$1
    file_size=`stat -c '%s' $file 2>/dev/null`
    echo $file_size 
}

target_table_file="$table_directory"/"$target_table".ibd

if [[ ! -f "$target_table_file" ]]
then
    echo "The $target_table.ibd does not exist in $table_directory !!!" 
    exit 
fi

target_table_file_size=`get_file_size "$target_table_file"`
db_name=`basename "$table_directory"`

intermediate_table_file=`ls "$table_directory"/"#sql"*".ibd" 2>/dev/null`

if [[ -z "$intermediate_table_file" ]]
then
    echo "Can not find the intermediate table for $target_table.ibd,Maybe the DDL has not started yet"
        exit
fi
last_intermediate_table_file_size=`get_file_size "$intermediate_table_file"`

echo "Altering $db_name.$target_table ..."

while true
do
        sleep 10
    intermediate_table_file_size=`get_file_size "$intermediate_table_file"`
    if [[ -z "$intermediate_table_file_size" ]]
    then
        echo "Successfully altered $db_name.$target_table"
            exit
    fi
        percent=`echo "$intermediate_table_file_size*100/$target_table_file_size" | bc`
        if [[ "$percent" -gt 100 ]]
        then
        percent=100
        fi
        alter_speed=`echo "scale=2;($intermediate_table_file_size-$last_intermediate_table_file_size)/10" | bc`
        remain_second=`echo "($target_table_file_size-$intermediate_table_file_size)/$alter_speed" |bc `
        if [[ "$remain_second" -lt 0 ]]
        then
        remain_second=0
    fi
        remain_time=`date -u -d @$remain_second +"%T"`
        echo "Altering $db_name.$target_table:  $percent% $remain_time remain"
        last_intermediate_table_file_size=$intermediate_table_file_size
done