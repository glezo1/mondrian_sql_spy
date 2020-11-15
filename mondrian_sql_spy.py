

import sys
import signal
import subprocess
import sqlparse
import psycopg2
from tabulate import tabulate



process             =   None
cur					=	None
#-----------------------------------------------------------------------------------------
def main():
	tail_line		=	"sudo  tail -f /XXX/pentaho-server/tomcat/logs/mondrian_sql.log"
	conn			=	None

	signal.signal(signal.SIGINT,control_c_handler)
	try:
		conn = psycopg2.connect("dbname='XXX' user='XXX' host='XXX' password='XXX'")
	except Exception as e:
		print("I am unable to connect to the database")
		print(e)
		exit()
	cur				=	conn.cursor()
	process			=	subprocess.Popen(tail_line,stderr=subprocess.PIPE,stdout=subprocess.PIPE,shell=True)
	while(True):
		output_line =   process.stdout.readline()
		if(output_line):
			output_line     =   output_line.decode('utf-8')
			if(output_line.strip()!='' and "executing sql" in output_line):
				#print('!!!!!!!!!!!'+output_line.strip())
				tokens          =   output_line.split(' ')
				date            =   tokens[0]
				time            =   tokens[1]
				sql_query		=	output_line.split('executing sql [')[1][0:-2]
				sql_query		=	sql_query.replace('"','')
				print('## ##################################################################')
				print('## ##################################################################')
				print('## ##################################################################')
				#print(sqlparse.format(sql_query,reindent=True,indent_tabs=False,keyword_case='upper'))
				print(pretty_parse_sql(sqlparse.format(sql_query,reindent=True,indent_tabs=False,keyword_case='upper')))
				execute_and_print_query(cur,sql_query)
#---------------------------------------------------------
def execute_and_print_query(cur,sql_query):
	try:
		cur.execute(sql_query)
	except:
		print("QUERY FAILED!!!!!!!!!!!!!!!!!!!!")
		return None
	data				=	[]
	colnames			= 	[desc[0] for desc in cur.description]
	rows				=	cur.fetchall()
	for row in rows:
		row		=	list(row)
		data.append(row)
	print(tabulate(data,headers=colnames))
	print()
#---------------------------------------------------------
def pretty_parse_sql(sql_query):
	spaces_begin	=	9
	result			=	''
	for i in sql_query.split('\n'):
		if(not i.startswith(' ')):
			i		=	i.replace("SELECT ","SELECT   ")
			i		=	i.replace("FROM "  ,"FROM     ")
			i		=	i.replace("WHERE " ,"WHERE    ")
			result	+=	i +'\n'
		else:
			i		=	i.strip()
			i_new	=	''
			for _ in range(0,spaces_begin):
				i_new += ' '
			i_new	+=	i
			result += i_new +'\n'
	return result
#---------------------------------------------------------
def control_c_handler(signal,frame):
	print('#SIGINT DETECTED!')
	if(process!=None):
		process.terminate()
	if(cur != None):
		cur.close()
	sys.exit(0)
#---------------------------------------------------------
if(__name__=='__main__'):
    main()



