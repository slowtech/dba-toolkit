#!/usr/bin/python
# -*- coding:UTF-8 -*-
import MySQLdb,re,prettytable,optparse,os,sys,subprocess,tempfile

def get_variables(config_file):
    variables = {}
    with open(config_file) as f:
        mysqld_flag=0
        for line in f:

            #用于后续过滤空行
            line=line.strip()

            if line.startswith('[mysqld]'):
                mysqld_flag = 1
            elif line.startswith('['):
                mysqld_flag=0
            if mysqld_flag==1 and line and not line.startswith('#') and not line.startswith('[mysqld]'):

                #用于剔除参数后面的注释
                if "#" in line:
                    line= line.split('#')[0]

                #之所以增加这个判断，是为了避免对于optimizer-trace-features greedy_search=on参数的误判
                if "=" in line:
                    if len(re.split('=',line)[0].split()) == 1:
                        line=line.replace('=',' ',1)
                if "(No default value)" in line:
                    line_with_variables=line.split("(No default value)")
                    variables[line_with_variables[0]] = ''
                else:
                    line_with_variables=line.split()
                    if len(line_with_variables) == 1:
                        variables[line_with_variables[0]]=''
                    else:
                        variables[line_with_variables[0]] = line_with_variables[1]
    return variables

def get_variables_from_instance(host,port,user,passwd):
    try:
        conn=MySQLdb.connect(host=host,port=port,user=user,passwd=passwd)
        cursor = conn.cursor()
        query='show global variables'
        cursor.execute(query)
        results=cursor.fetchall()
        variables=dict(results)
        return variables
    except Exception as e:
        print e

def convert_variable_value(variable_value):
    #路径区分大小写，所以路径直接返回
    if not '/' in variable_value:
        if variable_value.lower() in ['false','off','0']:
            variable_value='0'
        elif variable_value.lower() in ['true','on','1']:
            variable_value='1'
        elif re.search(r'^(\d+)G$',variable_value,re.IGNORECASE):
            variable_value=str(int(re.split('G|g',variable_value)[0])*1024*1024*1024)
        elif re.search(r'^(\d+)M$',variable_value,re.IGNORECASE):
            variable_value=str(int(re.split('M|m',variable_value)[0])*1024*1024)
        elif re.search(r'^(\d+)K$', variable_value, re.IGNORECASE):
            variable_value = str(int(re.split('K|k',variable_value)[0]) * 1024)
        variable_value=variable_value.lower()
    return variable_value

def convert_variable_name(variables):
    convert_variables={}
    for variable_name,variable_value in variables.iteritems():
        new_variable_name=variable_name.replace('-','_')
        new_variable_name=new_variable_name.strip()
        convert_variables[new_variable_name]=variable_value
    return  convert_variables

def convert_connect_info(instance_info):
    connect_info={}
    instance_info_dict=dict(info.split('=') for info in instance_info.split(','))
    connect_info['host']=instance_info_dict.get('h')
    connect_info['port'] = int(instance_info_dict.get('P'))
    connect_info['user'] = instance_info_dict.get('u')
    connect_info['passwd'] = instance_info_dict.get('p')
    return connect_info

def get_variables_from_mysqld_help(default):
    if default == 'mysqld':
        command='mysqld --no-defaults --verbose --help'
    else:
        command=os.path.join(default,'mysqld --no-defaults --verbose --help')
    p=subprocess.Popen(command,shell=True,stdout=subprocess.PIPE)
    temp=tempfile.mkstemp()
    temp_file=temp[1]
    flag=0
    with open(temp_file,'w+') as f:
        for line in p.stdout:
            if line.startswith('---------'):
                f.write('[mysqld]\n')
                flag=1
                continue
            if flag == 1 and len(line) == 1:
                break
            if flag ==1:
                f.write(line)  
    return temp 


def main():
    usage = '''Four types Comparison are supported
       1. Config file vs Config file 
          ./find_config_diff.py --f1 my_5.6.cnf --f2 my_5.7.cnf
       2. Conifig file vs Instance variables 
          ./find_config_diff.py --f1 my.cnf --instance h=192.168.244.10,P=3306,u=root,p=123456
       3. Instance variables vs Default variables 
          ./find_config_diff.py --instance h=192.168.244.10,P=3306,u=root,p=123456 --default=mysqld
       4. Conifig file vs Default variables 
          ./find_config_diff.py --f1 my.cnf --default=/usr/local/mysql/bin
           '''
    parser = optparse.OptionParser(usage)
    parser.add_option("--f1",action="store", help="The first config file")
    parser.add_option("--f2",action="store", help="The second config file")
    parser.add_option("--instance",action="store", help="Input the Connect info,like h=192.168.244.10,P=3306,u=root,p=123456")
    parser.add_option("--default",action="store", help="Input the mysqld's path,like '/usr/local/mysql/bin'\
                                                          You can also specify mysqld if mysqld in $PATH"
                                                           )
    # args = ['--f1', 'my.cnf','--instance','h=192.168.244.10,P=3306,u=root,p=123456']
    # args = ['--f1', 'my.cnf','--default','/usr/local/mysql/bin/']
    options, args = parser.parse_args()
    # (options, args) = parser.parse_args()
    config_file_one=options.f1
    config_file_two=options.f2
    instance=options.instance
    default=options.default
    if config_file_one and config_file_two:
        variables_one=get_variables(config_file_one)
        variables_two=get_variables(config_file_two)
        column_name=["Variable",config_file_one,config_file_two]
    elif config_file_one and instance:
        variables_one=get_variables(config_file_one)
        connect_info=convert_connect_info(instance)
        variables_two=get_variables_from_instance(**connect_info)
        column_name = ["Variable", config_file_one, "Instance"]
    elif config_file_one and default:
        variables_one=get_variables(config_file_one)
        temp=get_variables_from_mysqld_help(default)
        variables_two=get_variables(temp[1])
        os.close(temp[0])
        column_name = ["Variable", config_file_one, "Default"]
    elif instance and default:
        connect_info = convert_connect_info(instance)
        variables_one = get_variables_from_instance(**connect_info)
        temp=get_variables_from_mysqld_help(default)
        variables_two=get_variables(temp[1])
        os.close(temp[0])
        column_name = ["Variable","Instance", "Default"]

    convert_variables_one=convert_variable_name(variables_one)
    convert_variables_two = convert_variable_name(variables_two)
    set_variables_one=set(convert_variables_one.keys())
    set_variables_two=set(convert_variables_two.keys())

    common_variables=set_variables_one & set_variables_two

    pt = prettytable.PrettyTable(column_name)
    pt.align='l'
    pt.padding_width = 1  # One space between column edges and contents (default)
    pt.max_width=40
    for each_variable in sorted(common_variables):
        if convert_variable_value(convert_variables_one[each_variable]) == convert_variable_value(convert_variables_two[each_variable]):
            pt.add_row([each_variable,convert_variables_one[each_variable],convert_variables_two[each_variable]])
    row = ''.join(['-' for num in xrange(10)])
    pt.add_row([row,row,row])
    
    for each_variable in sorted(common_variables):
        if convert_variable_value(convert_variables_one[each_variable]) != convert_variable_value(convert_variables_two[each_variable]):
            pt.add_row([each_variable,convert_variables_one[each_variable],convert_variables_two[each_variable]])
            # print each_variable.ljust(25),convert_variables_one[each_variable].ljust(25),convert_variables_two[each_variable].ljust(25)
    if config_file_one and config_file_two:
        variables_one_only=set_variables_one - set_variables_two
        row = ''.join(['-' for num in xrange(10)])
        pt.add_row([row,row,row])
        for each_variable in sorted(variables_one_only):
            pt.add_row([each_variable,convert_variables_one[each_variable],''])
            # print each_variable.ljust(30),convert_variables_one[each_variable].ljust(30)
        variables_two_only= set_variables_two - set_variables_one
        pt.add_row([row,row,row])
        for each_variable in sorted(variables_two_only):
            pt.add_row([each_variable, '',convert_variables_two[each_variable]])
            # print each_variable.ljust(30), '--'.ljust(30),convert_variables_two[each_variable].ljust(30)
    print pt
    #print pt.get_html_string()
if __name__ == '__main__':
    main()
