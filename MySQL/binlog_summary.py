#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time  : 2021-04-18
# @Author: Victor 
# @Blog  : https://www.cnblogs.com/ivictor

from __future__ import print_function
import os
import re
import sqlite3
import argparse
import datetime

SQLITE_DB_FILE = r'/tmp/Victor&sqlite3.db'

class SQLite():
    def __init__(self,db_file):
        self.db_file=db_file
    def __enter__(self):
        self.conn = sqlite3.connect(self.db_file)
        self.cursor=self.conn.cursor()
        return self
    def execute(self,sql):
        self.cursor.execute(sql)
        result=self.cursor.fetchall()
        return result
    def executemany(self,sql,paras):
        self.cursor.executemany(sql, paras)
    def commit(self):
        self.conn.commit()
    def __exit__(self,exc_type, exc_value, traceback):
        self.cursor.close()
        self.conn.close()

def init_sqlite_table():
    dml_info_create_sql="create table dml_info (id integer auto_increment primary key,transaction_name varchar(10), \
                           schema_table_name varchar(50), dml_type varchar(10), dml_time datetime)"
    transaction_info_create_sql="create table transaction_info (id integer auto_increment primary key, \
              transaction_name varchar(10),transaction_begin_time datetime,transaction_commit_time datetime, \
              transaction_begin_log_pos integer,transaction_commit_log_pos integer)"
    with SQLite(SQLITE_DB_FILE) as db:
       db.execute(dml_info_create_sql)
       db.execute(transaction_info_create_sql)
       db.commit()


def parse_binlog_text_file(binlog_text_file):
    transaction_number=1
    transaction_name='t1'
    with open(binlog_text_file) as f:
        dml_info_records = []
        transaction_info_records=[]
        use_database=""
        for line in f:
            dml_flag = 0
            match_sub_strings=["use","# at","server id","BEGIN","insert","delete","update","DELETE","INSERT","UPDATE","COMMIT"]
            if not any(each_str in line for each_str in match_sub_strings):
                continue
            if "server id" in line:
                if "Query" not in line and "Xid" not in line:
                    continue 
            if re.match(r'# at \d+',line):
                 start_log_pos=line.split()[2]
            elif "server id" in line:
                    m=re.match(r'#(.*) server id.*end_log_pos (\d+)',line)
                    # dml_time is binlog event begin time
                    dml_time=m.group(1) 
                    dml_time=datetime.datetime.strptime(dml_time,'%y%m%d %H:%M:%S').strftime('%Y-%m-%d %H:%M:%S')
                    end_log_pos=m.group(2)
            elif re.match(r'^BEGIN\n$',line):
                    transaction_begin_time=dml_time
                    transaction_begin_log_pos=start_log_pos
                    transaction_name="t%d"%(transaction_number)
                    transaction_number=transaction_number+1      
            elif re.match('use',line) and line.strip().endswith('/*!*/;'):
                use_database=re.split('`|`',line)[1]
            elif re.match('### (DELETE|INSERT|UPDATE)',line):
                     line_split=line.split()
                     schema_table_name=line_split[-1].replace('`','').strip('\n')
                     dml_type=line_split[1]
                     dml_flag=1
            elif re.match('insert|delete|update',line,re.I):
                    if re.match('insert',line,re.I):
                        m= re.search(r'(into)(.*?)(values|\(|\n|partition|select)',line,re.I)
                        table=m.group(2).strip()
                        dml_type='INSERT'
                    elif re.match('delete',line,re.I):
                        m=re.search(r'(from)(.*?)(partition|where|limit|\n)',line,re.I)
                        table=m.group(2).strip()
                        dml_type='DELETE'
                    else:
                        m=re.search(r'(update|LOW_PRIORITY|IGNORE)(.*?)(set|\n)',line,re.I)
                        table=m.group(2).strip()
                        dml_type='UPDATE'
                    schema_table_name=table
                    if '.' not in schema_table_name:
                        if use_database != "":
                            schema_table_name = use_database + '.' + table
                        else:
                           schema_table_name=table
                    dml_flag=1
            elif 'COMMIT/*!*/;' in line:
                    transaction_commit_time=dml_time
                    transaction_commit_log_pos=end_log_pos
                    transaction_info_records.append([transaction_name,transaction_begin_time,transaction_commit_time,transaction_begin_log_pos,transaction_commit_log_pos])
            if dml_flag ==1:
                    dml_info_records.append([transaction_name,schema_table_name,dml_type,dml_time])
            if len(dml_info_records) % 10000 ==0:
                with SQLite(SQLITE_DB_FILE) as db:
                    db.executemany("insert into dml_info(transaction_name,schema_table_name,dml_type,dml_time) values (?,?,?,?)",dml_info_records)
                    db.commit()
                    dml_info_records=[]
            if len(transaction_info_records) % 10000 ==0:
                with SQLite(SQLITE_DB_FILE) as db:
                    db.executemany("insert into transaction_info(transaction_name,transaction_begin_time,transaction_commit_time, \
                            transaction_begin_log_pos,transaction_commit_log_pos) values (?,?,?,?,?)", transaction_info_records)
                    db.commit()
                    transaction_info_records=[]
        with SQLite(SQLITE_DB_FILE) as db:
            db.executemany("insert into dml_info(transaction_name,schema_table_name,dml_type,dml_time) values (?,?,?,?)",dml_info_records)
            db.executemany("insert into transaction_info(transaction_name,transaction_begin_time,transaction_commit_time, \
                                transaction_begin_log_pos,transaction_commit_log_pos) values (?,?,?,?,?)", transaction_info_records)
            db.commit()

def query_and_print(col_name,sql,print_flag=True):
    with SQLite(SQLITE_DB_FILE) as db:
        query_result=db.execute(sql)
    if not print_flag:
       return query_result 
    else:
        for each_col in col_name:
            print(each_col.ljust(18),end=' ')
        print()
        for each_row in query_result:
            for each_col in each_row:
                print(str(each_col).ljust(18),end=' ')
            print()

def operation_per_second(start_datetime,stop_datetim,limit):
    if start_datetime:
        get_opr_sql = "select schema_table_name,upper(dml_type),count(*) times from dml_info \
                         where dml_time BETWEEN '%s' and '%s' group by schema_table_name,dml_type order by 3 desc"%(start_datetime, stop_datetime)
    else:
        get_opr_sql = "select schema_table_name,upper(dml_type),count(*) times from dml_info group by schema_table_name,dml_type order by 3 desc"
    if limit:
        get_opr_sql = '%s limit %d'%(get_opr_sql,limit)
    query_and_print(("TABLE_NAME","DML_TYPE","NUMS"),get_opr_sql)

def transaction_per_second(start_datetime,stop_datetime,limit):
    if start_datetime:
        get_tps_sql="select transaction_commit_time, count(*) from transaction_info \
                 where transaction_commit_time BETWEEN '%s' and '%s' group by transaction_commit_time order by 1"%(start_datetime, stop_datetime)
    else:
        get_tps_sql = "select transaction_commit_time, count(*) from transaction_info group by transaction_commit_time order by 1"
    if limit:
        get_tps_sql = '%s limit %d'%(get_tps_sql,limit)
    query_and_print(("COMMIT_TIME","TPS"),get_tps_sql)
    
def get_transaction_info(start_datetime,stop_datetime,sort_condition,extend,limit):
    if start_datetime:
        get_transaction_sql = "select transaction_name,transaction_begin_time,transaction_commit_time,transaction_begin_log_pos, \
                   transaction_commit_log_pos,strftime('%s',transaction_commit_time)-strftime('%s',transaction_begin_time),\
                  transaction_commit_log_pos-transaction_begin_log_pos from transaction_info where transaction_commit_time \
                  BETWEEN '%s' and '%s'"%(start_datetime,stop_datetime)
    else:
        get_transaction_sql = "select transaction_name,transaction_begin_time,transaction_commit_time,transaction_begin_log_pos, \
                   transaction_commit_log_pos,strftime('%s',transaction_commit_time)-strftime('%s',transaction_begin_time),\
                  transaction_commit_log_pos-transaction_begin_log_pos from transaction_info"
    if sort_condition == "time":
        get_transaction_sql = '%s order by 6 desc'%(get_transaction_sql)
    elif sort_condition == "size":
        get_transaction_sql = '%s order by 7 desc'%(get_transaction_sql)
    if limit:
        get_transaction_sql = '%s limit %d'%(get_transaction_sql,limit)
    col_names=("TRANS_NAME","BEGIN_TIME","COMMIT_TIME","BEGIN_LOG_POS","COMMIT_LOG_POS","DURATION_TIME","SIZE")
    if not extend:
        query_and_print(col_names,get_transaction_sql)
    else:
        transaction_info=query_and_print(col_names,get_transaction_sql,False)
        get_opr_sql="select transaction_name,schema_table_name,upper(dml_type),count(*) times from dml_info \
                     group by 1,2,3"
        opr_info=query_and_print([],get_opr_sql,False)
        opr_info_dict={}
        for each_opr in opr_info:
            if opr_info_dict.has_key(each_opr[0]):
                opr_info_dict[each_opr[0]].append([each_opr[1],each_opr[2],each_opr[3]])
            else:
                opr_info_dict[each_opr[0]]=[[each_opr[1],each_opr[2],each_opr[3]]]
        print("TRANS_NAME".ljust(15),"BEGIN_TIME".ljust(20),"COMMIT_TIME".ljust(20),"BEGIN_LOG_POS".ljust(15), \
              "COMMIT_LOG_POS".ljust(15),"DURATION_TIME".ljust(15),"SIZE")
        for each_transaction_info in transaction_info:
            transaction_name=each_transaction_info[0]
            print(each_transaction_info[0].ljust(15),each_transaction_info[1].ljust(20),each_transaction_info[2].ljust(20), \
                  str(each_transaction_info[3]).ljust(15),str(each_transaction_info[4]).ljust(15), \
                  str(each_transaction_info[5]).ljust(15),each_transaction_info[6])
            for each_opr in opr_info_dict[transaction_name]:
                print("├──            ",each_opr[0].ljust(41),each_opr[1].ljust(15),each_opr[2])
    
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-f", "--file",dest="binlog_text_file", help="Binlog text file, not the Raw binary file")
    parser.add_argument("--new", action='store_true', help="Make a fresh start")
    parser.add_argument("-c","--command",dest='command_type',choices=['tps', 'opr', 'transaction'], help="Command type: [tps, opr, transaction],tps: transaction per second, opr: dml per table, transaction: show transaction info")
    parser.add_argument("--start", dest='start_datetime', help="Start datetime, for example: 2004-12-25 11:25:56")
    parser.add_argument("--stop", dest='stop_datetime', help="Stop datetime, for example: 2004-12-25 11:25:56")
    parser.add_argument("--sort",dest='sort_condition', help="Sort condition: time or size, you can use it when command type is transaction")
    parser.add_argument("-e","--extend",action='store_true', help="Show transaction info in detail,you can use it when command type is transaction")
    parser.add_argument("--limit",type=int, dest='limit', help="Limit the number of rows to display")
    args = parser.parse_args()
    if (args.start_datetime and not args.stop_datetime) or (not args.stop_datetime and args.start_datetime):
        print("you have to specify the start_datetime and stop_datetime both")
        exit() 
    if args.new and os.path.exists(SQLITE_DB_FILE):
        os.remove(SQLITE_DB_FILE)
    if not os.path.exists(SQLITE_DB_FILE):
        init_sqlite_table() 
        parse_binlog_text_file(args.binlog_text_file)
    if args.command_type == "opr":
        operation_per_second(args.start_datetime, args.stop_datetime, args.limit)
    if args.command_type == "tps":
        transaction_per_second(args.start_datetime, args.stop_datetime, args.limit)
    if args.command_type == "transaction":
        get_transaction_info(args.start_datetime, args.stop_datetime, args.sort_condition, args.extend, args.limit)

if __name__ == '__main__':
    main()