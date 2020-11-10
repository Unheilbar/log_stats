from configparser import ConfigParser
from clickhouse_driver import Client

def config(section, filename = 'config.ini'): #read constant variables
    # create a parser
    parser = ConfigParser()
    # read config file
    parser.read(filename)

    # get section, default to postgresql
    result = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            result[param[0]] = param[1]
    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, filename))

    return result

def parse_log_line(line):
    splitted = line.split()
    if len(splitted) == 8:
        date = splitted[0]
        time = splitted[1]
        ip_abon = splitted[2]
        ip_nat = splitted[3]
        ip_nat_list = splitted[5]
        url = splitted[6]
        ip_server = splitted[7][1:-1]
        return [date, time, ip_abon, ip_nat, ip_nat_list, url, ip_server]
        
    return


def parse_log_from_file(file_name):
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

def insert_into_db():

    client = Client('localhost', user = 'default', database = 'testlogs', password = 'qwerty')
    
    result = client.execute('SELECT now(), version()')

    for t in result:
        print(" ROW: {0}: {1}".format(type(t), t))
    for v in t:
            print("  COLUMN: {0}: {1}".format(type(v), v))

def main():
    parsed = parse_log_from_file('head 2020-11-10_16:06.log')
    for i in range(5):
        print(parsed[i])
    insert_into_db()

main()
