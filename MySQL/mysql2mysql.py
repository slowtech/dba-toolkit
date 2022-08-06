#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pymysql,argparse,time,json,sys
from pymysqlreplication import BinLogStreamReader

reload(sys)
sys.setdefaultencoding('utf8')

from pymysqlreplication.row_event import (
    DeleteRowsEvent,
    UpdateRowsEvent,
    WriteRowsEvent,
)
from pymysqlreplication.event import (RotateEvent,QueryEvent,XidEvent)

class DbUtils:
    def __init__(self, host, user,passwd,port):
        self.conn=pymysql.connect(host,user,passwd,port=port,charset='utf8',autocommit=False)
        self.cursor=self.conn.cursor()
    def query(self,sql):
        self.cursor.execute(sql)
        result=self.cursor.fetchall()
        return result
    def execute(self,sql):
        binlog_file_pos=sql.pop()
        master_log_name, master_log_start_pos, master_log_end_pos = binlog_file_pos
        try:
            update_relay_info_sql="insert into mysql2mysql.relay_info (id, master_log_name, master_log_start_pos,master_log_end_pos) values (NULLIF(%s, 0), '%s'," \
                              "%s,%s) on duplicate key update last_update=NOW(),master_log_name='%s',master_log_start_pos=%s,master_log_end_pos=%s "
            update_file_pos_state=update_relay_info_sql%(1,master_log_name, master_log_start_pos, master_log_end_pos,master_log_name, master_log_start_pos, master_log_end_pos)
            self.cursor.execute(update_file_pos_state)
            insert_file_pos_state=update_relay_info_sql%(0,master_log_name, master_log_start_pos, master_log_end_pos,master_log_name, master_log_start_pos, master_log_end_pos)
            self.cursor.execute(insert_file_pos_state)
            for each_sql in sql:
				self.cursor.execute(each_sql)
            self.conn.commit()
        except Exception,e:
			try:
				self.conn.rollback()
			except Exception,e1:
				print e1,sql
			raise Exception(e,sql)

def get_binlog_file_pos(connection_values,role):
    db=DbUtils(**connection_values)
    if role == 'master':
        file_log_status = db.query('show master status')
        log_file,log_pos,_,_,_ = file_log_status[0]
    elif role == 'slave':
        file_log_status = db.query("select master_log_name,master_log_end_pos from mysql2mysql.relay_info where id=1")
        if not file_log_status:
            raise Exception("No record in mysql2mysql.ralay_info")
        log_file, log_pos= file_log_status[0]
    return log_file,log_pos

def compare_items(items):
    (k, v) = items
    if v is None:
        return '`%s` IS %%s' % k
    else:
        return '`%s`=%%s' % k

def execute_sql_in_dest_db(dest_connection_info,transaction_sql):
    db=DbUtils(**dest_connection_info)
    db.execute(transaction_sql)

def handle_binlog_event(source_connection_info,dest_connection_info,log_file,log_pos):
    stream = BinLogStreamReader(connection_settings=source_connection_info,
                                server_id=100, blocking=True,
                                only_events=[DeleteRowsEvent, WriteRowsEvent, UpdateRowsEvent, RotateEvent, QueryEvent,
                                             XidEvent],
                                resume_stream=True,
                                log_file=log_file, log_pos=log_pos)
    conn = pymysql.connect(**source_connection_info)
    cursor = conn.cursor()
    transaction_sql = []
    for binlog_event in stream:
        if isinstance(binlog_event, RotateEvent):
            log_file = binlog_event.next_binlog
        elif isinstance(binlog_event, QueryEvent) and binlog_event.query == 'BEGIN':
            transaction_start_pos = binlog_event.packet.log_pos
        elif isinstance(binlog_event, (DeleteRowsEvent, UpdateRowsEvent, WriteRowsEvent)):
            schema, table = binlog_event.schema, binlog_event.table
            for row in binlog_event.rows:
                if isinstance(binlog_event, DeleteRowsEvent):
                    delete_sql_template = 'DELETE FROM `{0}`.`{1}` WHERE {2} LIMIT 1;'.format(
                        schema, table, ' AND '.join(map(compare_items, row['values'].items())))
                    delete_sql = cursor.mogrify(delete_sql_template, row['values'].values())
                    transaction_sql.append(delete_sql)

                elif isinstance(binlog_event, UpdateRowsEvent):
                    update_sql_template = 'UPDATE `{0}`.`{1}` SET {2} WHERE {3} LIMIT 1;'.format(
                        schema, table,
                        ', '.join(['`%s`=%%s' % k for k in row['after_values'].keys()]),
                        ' AND '.join(map(compare_items, row['before_values'].items()))
                    )
                    values = list(row['after_values'].values()) + list(row['before_values'].values())
                    update_sql = cursor.mogrify(update_sql_template, values)
                    transaction_sql.append(update_sql)

                elif isinstance(binlog_event, WriteRowsEvent):
                    insert_sql_template = 'INSERT INTO `{0}`.`{1}`({2}) VALUES ({3})'.format(
                        schema, table,
                        ', '.join(map(lambda key: '`%s`' % key, row['values'].keys())),
                        ', '.join(['%s'] * len(row['values']))
                    )
                    insert_sql = cursor.mogrify(insert_sql_template, row['values'].values())
                    transaction_sql.append(insert_sql)
        elif isinstance(binlog_event, XidEvent):
            transaction_end_pos = binlog_event.packet.log_pos
            #print '\n',log_file, transaction_start_pos, transaction_end_pos
            transaction_sql.append([log_file,transaction_start_pos, transaction_end_pos])
            execute_sql_in_dest_db(dest_connection_info,transaction_sql)
            transaction_sql = []
        # time.sleep(5)
    stream.close()
    cursor.close()
    conn.close()

def parse_args():
    USAGE = "%(prog)s --source user:pass@host:port --dest user:pass@host:port " \
            "--start-file mysql-bin.000001 --start-pos 154"
    parser = argparse.ArgumentParser(usage=USAGE,version='0.1')
    parser.add_argument("--source", action="store", dest="source",type=str,
                      help="connection information for source server in "
                           "the form: <user>[:<password>]@<host>[:<port>]")
    parser.add_argument("--dest", action="store", dest="destination",type=str,
                      help="connection information for destination server in "
                           "the form: <user>[:<password>]@<host>[:<port>]")
    parser.add_argument("--start-file", dest='start_file', type=str,
                      help='start binlog file to be parsed,if not given,get binlog file & pos from "show master status"')
    parser.add_argument('--start-pos', dest='start_pos', type=int,default=4,
                      help='start position of the --start-file,if not given,default 4')
    parser.add_argument('-c','--continue',dest='continue_flag',action='store_true',default=False,
                      help='get binlog file & postion from dest db mysql2mysql.ralay_info,default False')
    #args = parser.parse_args(r'--source root:123456@192.168.244.10:3306 --dest root:123456@192.168.244.20:3306'.split())
    args = parser.parse_args()
    if not args.source or not args.destination:
        parser.error("You must specify both --source and --dest.")
    if args.start_file and args.continue_flag:
        parser.error("You cannot use --start-file and -c together.")
    return args

def parse_connection(connection_values):
    conn_format = connection_values.rsplit('@', 1)
    user,passwd=conn_format[0].split(":")
    host,port=conn_format[1].split(":")
    connection = {
        "user": user,
        "host": host,
        "port": int(port),
        "passwd": passwd
    }
    return connection

def main():
    args=parse_args()
    source_connection_info=parse_connection(args.source)
    dest_connection_info=parse_connection(args.destination)
    if not args.start_file and not args.continue_flag:
        log_file,log_pos=get_binlog_file_pos(source_connection_info,'master')
    elif args.start_file:
        log_file,log_pos=args.start_file,args.start_pos
    elif args.continue_flag:
        log_file,log_pos=get_binlog_file_pos(dest_connection_info,'slave')
    handle_binlog_event(source_connection_info,dest_connection_info,log_file,log_pos)

if __name__ == '__main__':
    main()
