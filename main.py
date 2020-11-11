#Script for parsing logs files and writing statistics into clickhouse database
#Example of the log' line is

#2020-11-10 16:06:00  10.19.201.110   109.126.58.173  list 15    https://venetia.iad.appboy.com (151.101.12.233:443)

#All constant variables are written in config.ini file (should be in the same directory with script)

from configparser import ConfigParser
from clickhouse_driver import Client
from datetime import datetime

def config(section, filename = 'config.ini'): #read constant variables
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
        date = datetime.strptime(' '.join([splitted[0],splitted[1]]), "%Y-%m-%d %H:%M:%S")
        ip_abon = splitted[2]
        ip_nat = splitted[3]
        id_nat_list = int(splitted[5])
        url = splitted[6].split('://')[1]
        protocol = splitted[6].split('://')[0]
        if(protocol == 'http'):
            protocol_type = 1
        elif (protocol == 'https'):
            protocol_type = 2
        else:
            protocol_type = 3
 
        ip_server = splitted[7][1:-1].split(':')[0]

        return {'event_date':date, 'ip_abon':ip_abon, 'ip_nat':ip_nat, 'id_nat_list':id_nat_list, 'url':url, 'ip_server':ip_server, 'protocol_type':protocol_type }
        
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


def getHostname(string):
    week_number = datetime.now().isocalendar()[1]

    hostnames = string.split()
    current_hostname = hostnames[week_number % 2]     #choose current hostname for insert data in table
                                                      #current hostname depends on parity of current week
    
    return current_hostname

def insert_into_db():

    #connecting to clickhouse database
    try:
        login_data = config('Clickhouse_db_login')

        hostname = getHostname(login_data['hostnames'])

        user = login_data['user']
        database = login_data['database']
        password = login_data['password']

        client = Client(host = hostname, user = user, database = database, password = password)
        
        print('Connected to clickhouse db')

        #parsed = parse_logs_from_file('head 2020-11-10_16:06.log')

        #client.execute('INSERT INTO test_logs (event_date, ip_server, url, ip_nat, ip_abon, id_nat_list, protocol_type) VALUES', parsed)

        print('Command has executed successfully')

    except (Exception, Client.DatabaseError) as error:
        print(error)

def main():
    insert_into_db()
    

main()
