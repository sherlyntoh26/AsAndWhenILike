import sys
import os
import io
import json
import subprocess
import hashlib
import shutil
import tarfile
import MySQLdb
import csv
from subprocess import call


#########################################################################
##Function: main()
##Purpose: it all begins here
#########################################################################
def main():
	global CSVFilesDir;
	print("I am in main function");
	if len(sys.argv) < 2:
		print("usage : python loadData.py <Directory where the csv files reside>")
		sys.exit(1)
	CSVFilesDir = os.path.join(os.getcwd(),str(sys.argv[1]));
	if not (os.path.isdir(CSVFilesDir)):
		print("The CSV directory is not valid")
		sys.exit(1);
	if not (os.path.isfile(os.path.join(CSVFilesDir,"warehous.csv")) or os.path.isfile(os.path.join(CSVFilesDir,"district.csv")) or os.path.isfile(os.path.join(CSVFilesDir,"customer.csv")) or os.path.isfile(os.path.join(CSVFilesDir,"order.csv")) or os.path.isfile(os.path.join(CSVFilesDir,"item.csv")) or os.path.isfile(os.path.join(CSVFilesDir,"order-line.csv")) or os.path.isfile(os.path.join(CSVFilesDir,"stock.csv")) ):
		print("One of the input csv files is missing in the input directory")
		sys.exit(1);
		
	db = MySQLdb.connect(host="localhost", user="root", passwd="",db="d8_old")
	cursor = db.cursor()
	
	createOldSchema(cursor,db);
	loadDataIntoOldSchema(cursor,db);
	mySQLtoCSV(cursor,db);
	
	
def createOldSchema(cursor,db):
	global CSVFilesDir;
	cursor.execute("drop database if exists d8_old;");
	cursor.execute("create database d8_old;");
	cursor.execute("use d8_old;");
	cursor.execute("CREATE TABLE warehouse (w_id int,w_name varchar(10),w_street_1 varchar(20),w_street_2 varchar(20),w_city varchar(20),w_state varchar(2),w_zip varchar(9),w_tax decimal(4,4),w_ytd decimal(12,2),PRIMARY KEY(w_id));")
	cursor.execute("CREATE TABLE d8_old.district (d_w_id int,d_id int,d_name varchar(10),d_street_1 varchar(20),d_street_2 varchar(20),d_city varchar(20),d_state varchar(2),d_zip varchar(9),d_tax decimal(4,4),d_ytd decimal(12,2),d_next_o_id int,PRIMARY KEY(d_id,d_w_id));");
	cursor.execute("CREATE TABLE d8_old.customer (c_w_id int,c_d_id int,c_id int,c_first varchar(16),c_middle varchar(2),c_last varchar(16),c_street_1 varchar(20),c_street_2 varchar(20),c_city varchar(20),c_state varchar(2),c_zip varchar(9),c_phone varchar(16),c_since timestamp,c_credit varchar(2),c_credit_lim decimal(12,2),c_discount decimal(4,4),c_balance decimal(12,2),c_ytd_payment float,c_payment_cnt int,c_delivery_cnt int,c_data varchar(500),PRIMARY KEY(c_id,c_w_id,c_d_id));");
	cursor.execute("CREATE TABLE d8_old.orders (o_w_id int,o_d_id int,o_id int,o_c_id int,o_carrier_id int,o_ol_cnt decimal(2,0),o_all_local decimal(1,0),o_entry_d timestamp,PRIMARY KEY(o_id,o_d_id,o_w_id));");
	cursor.execute("CREATE TABLE d8_old.item (i_id int,i_name varchar(24),i_price decimal(5,2),i_im_id int,i_data varchar(50),PRIMARY KEY(i_id));");
	cursor.execute("CREATE TABLE d8_old.orderline (ol_w_id int,ol_d_id int,ol_o_id int,ol_number int,ol_i_id int,ol_delivery_d timestamp,ol_amount decimal(6,2),ol_supply_w_id int,ol_quantity decimal(2,0),ol_dist_info varchar(24),PRIMARY KEY(ol_number,ol_w_id,ol_d_id,ol_o_id));");
	cursor.execute("CREATE TABLE d8_old.stock (s_w_id int,s_i_id int,s_quantity decimal(4,0),s_ytd decimal(8,2),s_order_cnt int,s_remote_cnt int,s_dist_01 varchar(24),s_dist_02 varchar(24),s_dist_03 varchar(24),s_dist_04 varchar(24),s_dist_05 varchar(24),s_dist_06 varchar(24),s_dist_07 varchar(24),s_dist_08 varchar(24),s_dist_09 varchar(24),s_dist_10 varchar(24),s_data varchar(50),PRIMARY KEY(s_w_id,s_i_id));");
	db.commit();

def loadDataIntoOldSchema(cursor,db):
	global CSVFilesDir;
	cursor.execute("load data infile '"+os.path.join(CSVFilesDir,"warehouse.csv")+"' replace into table warehouse fields terminated by ',';");	
	cursor.execute("load data infile '"+os.path.join(CSVFilesDir,"district.csv")+"' replace into table district fields terminated by ',';");	
	cursor.execute("load data infile '"+os.path.join(CSVFilesDir,"customer.csv")+"' replace into table customer fields terminated by ',';");	
	cursor.execute("load data infile '"+os.path.join(CSVFilesDir,"order.csv")+"' replace into table orders fields terminated by ',';");	
	cursor.execute("load data infile '"+os.path.join(CSVFilesDir,"item.csv")+"' replace into table item fields terminated by ',';");	
	cursor.execute("load data infile '"+os.path.join(CSVFilesDir,"order-line.csv")+"' replace into table orderline fields terminated by ',';");	
	cursor.execute("load data infile '"+os.path.join(CSVFilesDir,"stock.csv")+"' replace into table stock fields terminated by ',';");	
	db.commit();

def mySQLtoCSV(cursor,db):
	global CSVFilesDir;
	""""cursor.execute("select w.w_id,d.d_id,w.w_name,w.w_street_1,w.w_street_2,w.w_city,w.w_state,w.w_zip,d.d_name,d.d_street_1,d.d_street_2,d.d_city,d.d_state,d.d_zip from warehouse w, district d where w.w_id = d.d_w_id into outfile '"+os.path.join(CSVFilesDir,"new_warehousedistrict.csv")+"' fields terminated by ',' lines terminated by '\n';");
	cursor.execute("select s.s_w_id,s.s_i_id,i.i_name,i.i_price,i.i_im_id,i.i_data,s.s_quantity,s.s_ytd,s.s_order_cnt,s.s_remote_cnt,s.s_dist_01,s.s_dist_02,s.s_dist_03,s.s_dist_04,s.s_dist_05,s.s_dist_06,s.s_dist_07,s.s_dist_08,s.s_dist_09,s.s_dist_10,s.s_data from stock s, item i  where i.i_id = s.s_i_id into outfile '"+os.path.join(CSVFilesDir,"new_inventory.csv")+"' fields terminated by ',' lines terminated by '\n';");
	cursor.execute("select c.c_w_id,c.c_d_id,c.c_id,c.c_first,c.c_middle,c.c_last,c.c_street_1,c.c_street_2,c.c_city,c.c_state,c.c_zip,c.c_phone,c.c_since,c.c_credit,c.c_credit_lim,c.c_discount,c.c_balance,c.c_ytd_payment,c.c_payment_cnt,c.c_delivery_cnt,c.c_data,w.w_name,d.d_name from customer c,warehouse w, district d where w.w_id = d.d_w_id and c.c_d_id = d.d_id and c.c_w_id = d.d_w_id into outfile '"+os.path.join(CSVFilesDir,"new_customer.csv")+"' fields terminated by ',' lines terminated by '\n';");
	"""
	cursor.execute("select ol.ol_w_id,ol.ol_d_id,ol.ol_o_id,ol.ol_number,ol.ol_i_id,ol.ol_amount,ol.ol_supply_w_id,ol.ol_quantity,ol.ol_dist_info,i.i_name from orderline ol, item i where i.i_id = ol.ol_i_id into outfile '"+os.path.join(CSVFilesDir,"new_orderline.csv")+"' fields terminated by ',' lines terminated by '\n';");	
	"""cursor.execute("select o.*, sum(ol.ol_amount),ol.ol_delivery_d from orders o, orderline ol where o.o_w_id=ol.ol_w_id and o.o_d_id=ol.ol_d_id and o.o_id=ol.ol_o_id group by o.o_w_id,o.o_d_id,o.o_id,o.o_c_id,o.o_Carrier_id,o.o_ol_cnt,o.o_all_local,o.o_entry_d into outfile '"+os.path.join(CSVFilesDir,"new_order.csv")+"' fields terminated by ',' lines terminated by '\n';");
	warehouseDistrictInfoToCSV(cursor,db);
	"""
def warehouseDistrictInfoToCSV(cursor,db):
	global CSVFilesDir;
	print("I am here");
	cursor.execute("SELECT w_id,w_tax,w_ytd FROM warehouse")
	numrows = int(cursor.rowcount)
	with open('/mnt/c/Users/enigma/workspace/Test/D8-data/new_warehousedistrictinfo.csv', 'w') as file:
		for x in range(0,numrows):
			row = cursor.fetchone()
			disNextOID=[];disTax=[];disYTD=[];
			print( "row is :"+str(row[0])+", "+str(row[1])+", "+str(row[2]) );
			cursor2 = db.cursor()
			for i in range(1,11):
				cursor2.execute("select d_next_o_id,d_tax,d_ytd from district where d_w_id="+str(row[0])+" and d_id="+str(i));
				distValues = cursor2.fetchone();
				disNextOID.append(str(distValues[0]));disTax.append(str(distValues[1]));disYTD.append(str(distValues[2]));
			file.write(str(row[0])+","+str(row[1])+","+str(row[2])+","+(",".join(disNextOID))+","+(",".join(disTax))+","+(",".join(disYTD))+"\n");
				
#Set the global variables here
if __name__ == '__main__':
	main()
	CSVFilesDir = ""
	
