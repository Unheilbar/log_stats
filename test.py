import subprocess

sql = "select * from test_logs limit 10"

cmd = 'clickhouse-client --password="qwerty" --query="select * from test_logs limit 10"'

def ch(cmd):
    #subprocess.run(['clickhouse-client', '--password', 'qwerty', '--query', 'select * from test_logs limit 10'])
    subprocess.Popen(cmd)

ch(cmd)