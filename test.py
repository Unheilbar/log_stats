#Script for parsing logs files and writing statistics into clickhouse database
#Example of log line is

#2020-11-10 16:06:00  10.19.201.110   109.126.58.173  list 15    https://venetia.iad.appboy.com (151.101.12.233:443)

#All constant variables are written in .ENV file (should be in the same directory with script)

from configparser import ConfigParser
from datetime import datetime
import os
import subprocess

def config(section, filename = '.ENV'): #constants initizalization
    # create a parser
    parser = ConfigParser()
    # read config file
    parser.read(filename)

    # get section
    result = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            result[param[0]] = param[1]
    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, filename))

    return result

def parse_log_line(line): #function for parsing a single line from log
    splitted = line.split()
    if len(splitted) == 8:
        date = ' '.join([splitted[0],splitted[1]])
        ip_abon = splitted[2]
        ip_nat = splitted[3]
        id_nat_list = int(splitted[5])
        url = splitted[6].split('://')[1]
        protocol = splitted[6].split('://')[0]
        if(protocol == 'http'):
            protocol_type = '1'
        elif (protocol == 'https'):
            protocol_type = '2'
        else:
            protocol_type = '3'
 
        ip_server = splitted[7][1:-1].split(':')[0]

        return [date, ip_abon, ip_nat, url, ip_server, protocol_type]
        
    return

def parse_logs_from_file(file_name): #parse logs from a single file = filename
    parsed_logs = []
    try:
        with open(file_name) as file_handler:
            for line in file_handler:
                data = parse_log_line(line)
                if(data is None):
                    continue
                parsed_logs.append(data)
            
            return parsed_logs
    except IOError:
        print("an IOError has occured")

def rename_current_log(path, name): #changes name of the current log file
    newname = 'current_' + name
    os.rename(path+name, path+newname)
    return path+newname

def get_hostname(string):
    week_number = datetime.now().isocalendar()[1]

    hostnames = string.split()
    current_hostname = hostnames[week_number % 2]     #choose current hostname for inserting data in the table
                                                      #current hostname depends on parity of current week
    return current_hostname


def get_sql_command(table_name, parsed_data):
    values = '), ('.join(', '.join(x) for x in parsed_data)
    values = '(' + values + ')'
    command = 'INSERT INTO {0} (event_date, ip_server, url, ip_nat, ip_abon, protocol_type) VALUES {1}'.format(table_name, values)
    return command


def insert_into_db(parsed_data):
    #connecting to clickhouse database
    try:
        login_data = config('Clickhouse_db_login')

        hostname = get_hostname(login_data['hostnames'])

        user = login_data['user']
        database = login_data['database']
        password = login_data['password']
        port = login_data['port']

        table_name = config('Clickhouse_table_data')['table_name'] #get table name from config file

        sql_command = get_sql_command(table_name, parsed_data)

        print(sql_command)

        subprocess.run(['clickhouse-client','--host', hostname, '--database', database, '--password',password , '--query', sql_command]) #execute comand for inserting data

        print('Command has executed successfully')

    except OSError as error:
        print(error)

def delete_current_log(name):
    os.remove(name)
    return

def update_table_data(path, max_logs_per_execute):
    file_names = os.listdir(path)
    logs_count = 0
    for file_name in file_names:
        if(file_name.endswith('.log')):
            if(logs_count < max_logs_per_execute):

                logs_count+=1

                #newname = rename_current_log(path, file_name)
                
                newname = file_name #delete after tests

                data = parse_logs_from_file(newname)

                if(data):
                    insert_into_db(data)

                #    delete_current_log(newname)

def main():
    path = config('paths')['path_to_logs'] #path to dir with logs

    max_logs_per_execute = int(config('limits')['max_logs_per_execute']) #max logs for reading per one execute

    
    update_table_data(path, max_logs_per_execute) #update table data

main()
