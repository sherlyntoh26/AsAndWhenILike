DROP KEYSPACE d8;
CREATE KEYSPACE d8 WITH replication = {'class':'SimpleStrategy', 'replication_factor':3};

CREATE TABLE d8.inventory (
	i_w_id int,
	i_id int,
	i_name varchar,
	i_price decimal,
	i_im_id int,
	i_b_data varchar,
	i_quantity decimal,
	i_ytd decimal,
	i_order_cnt int,
	i_remote_cnt int,
	i_dist_01 varchar,
	i_dist_02 varchar,
	i_dist_03 varchar,
	i_dist_04 varchar,
	i_dist_05 varchar,
	i_dist_06 varchar,
	i_dist_07 varchar,
	i_dist_08 varchar,
	i_dist_09 varchar,
	i_dist_10 varchar,
	i_data varchar,
	PRIMARY KEY(i_w_id, i_id)
);

CREATE TABLE d8.orders (
	o_w_id int,
	o_d_id int,
	o_id int,
	o_c_id int, 
	o_carrier_id int,
	o_ol_cnt decimal,
	o_all_local decimal,
	o_entry_d timestamp,
	o_total_amount float,
	o_delivery_d timestamp,
	PRIMARY KEY(o_w_id, o_d_id, o_id)
);

CREATE TABLE d8.orderLine (
	ol_w_id int,
	ol_d_id int,
	ol_o_id int,
	ol_number int,
	ol_i_id int,
	ol_amount decimal,
	ol_supply_w_id int,
	ol_quantity decimal,
	ol_dist_info varchar,
	ol_i_name varchar,
	PRIMARY KEY(ol_w_id, ol_d_id, ol_o_id, ol_number)
);

CREATE TABLE d8.customer (
	c_w_id int,
	c_d_id int,
	c_id int,
	c_first varchar,
	c_middle varchar,
	c_last varchar,
	c_street_1 varchar,
	c_street_2 varchar,
	c_city varchar,
	c_state varchar,
	c_zip varchar,
	c_phone varchar,
	c_since timestamp,
	c_credit varchar,
	c_credit_lim decimal,
	c_discount decimal,
	c_balance decimal,
	c_ytd_payment float,
	c_payment_cnt int,
	c_delivery_cnt int,
	c_data varchar,
	c_w_name varchar,
	c_d_name varchar,
	PRIMARY KEY(c_w_id, c_d_id, c_id)
);

CREATE TABLE d8.warehouseDistrict(
	wd_w_id int,
	wd_d_id int,
	wd_w_name varchar,
	wd_w_street_1 varchar,
	wd_w_street_2 varchar,
	wd_w_city varchar,
	wd_w_state varchar,
	wd_w_zip varchar,
	wd_d_name varchar,
	wd_d_street_1 varchar,
	wd_d_street_2 varchar,
	wd_d_city varchar,
	wd_d_state varchar,
	wd_d_zip varchar,
	PRIMARY KEY(wd_w_id, wd_d_id)
);

CREATE TABLE d8.warehouseDistrictInfo(
	wdi_w_id int PRIMARY KEY,
	wdi_w_tax decimal,
	wdi_w_ytd decimal,
	wdi_d_next_o_id_01 int,
	wdi_d_next_o_id_02 int,
	wdi_d_next_o_id_03 int,
	wdi_d_next_o_id_04 int,
	wdi_d_next_o_id_05 int,
	wdi_d_next_o_id_06 int,
	wdi_d_next_o_id_07 int,
	wdi_d_next_o_id_08 int,
	wdi_d_next_o_id_09 int,
	wdi_d_next_o_id_10 int,
	wdi_d_tax_01 decimal,
	wdi_d_tax_02 decimal,
	wdi_d_tax_03 decimal,
	wdi_d_tax_04 decimal,
	wdi_d_tax_05 decimal,
	wdi_d_tax_06 decimal,
	wdi_d_tax_07 decimal,
	wdi_d_tax_08 decimal,
	wdi_d_tax_09 decimal,
	wdi_d_tax_10 decimal,
	wdi_d_ytd_01 decimal,
	wdi_d_ytd_02 decimal,
	wdi_d_ytd_03 decimal,
	wdi_d_ytd_04 decimal,
	wdi_d_ytd_05 decimal,
	wdi_d_ytd_06 decimal,
	wdi_d_ytd_07 decimal,
	wdi_d_ytd_08 decimal,
	wdi_d_ytd_09 decimal,
	wdi_d_ytd_10 decimal
);

use d8;

COPY inventory (i_w_id,i_id,i_name,i_price,i_im_id,i_b_data,i_quantity,i_ytd,i_order_cnt,i_remote_cnt,i_dist_01,i_dist_02,i_dist_03,i_dist_04,i_dist_05,i_dist_06,i_dist_07,i_dist_08,i_dist_09,i_dist_10,i_data) from './DATA/D8-data/new_inventory.csv';
COPY orders (o_w_id,o_d_id,o_id,o_c_id,o_carrier_id,o_ol_cnt,o_all_local,o_entry_d,o_total_amount,o_delivery_d) from './DATA/D8-data/new_order.csv' with NULL='0000-00-00 00:00:00';
COPY orderLine (ol_w_id,ol_d_id,ol_o_id,ol_number,ol_i_id,ol_amount,ol_supply_w_id,ol_quantity,ol_dist_info,ol_i_name) from './DATA/D8-data/new_orderline.csv';
COPY customer (c_w_id,c_d_id,c_id,c_first,c_middle,c_last,c_street_1,c_street_2,c_city,c_state,c_zip,c_phone,c_since,c_credit,c_credit_lim,c_discount,c_balance,c_ytd_payment,c_payment_cnt,c_delivery_cnt,c_data,c_w_name,c_d_name) from './DATA/D8-data/new_customer.csv';
COPY warehouseDistrict (wd_w_id,wd_d_id,wd_w_name,wd_w_street_1,wd_w_street_2,wd_w_city,wd_w_state,wd_w_zip,wd_d_name,wd_d_street_1,wd_d_street_2,wd_d_city,wd_d_state,wd_d_zip) from './DATA/D8-data/new_warehousedistrict.csv';
COPY warehouseDistrictInfo (wdi_w_id,wdi_w_tax,wdi_w_ytd,wdi_d_next_o_id_01,wdi_d_next_o_id_02,wdi_d_next_o_id_03,wdi_d_next_o_id_04,wdi_d_next_o_id_05,wdi_d_next_o_id_06,wdi_d_next_o_id_07,wdi_d_next_o_id_08,wdi_d_next_o_id_09,wdi_d_next_o_id_10,wdi_d_tax_01,wdi_d_tax_02,wdi_d_tax_03,wdi_d_tax_04,wdi_d_tax_05,wdi_d_tax_06,wdi_d_tax_07,wdi_d_tax_08,wdi_d_tax_09,wdi_d_tax_10,wdi_d_ytd_01,wdi_d_ytd_02,wdi_d_ytd_03,wdi_d_ytd_04,wdi_d_ytd_05,wdi_d_ytd_06 ,wdi_d_ytd_07,wdi_d_ytd_08,wdi_d_ytd_09,wdi_d_ytd_10) from './DATA/D8-data/new_warehousedistrictinfo.csv';

