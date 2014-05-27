drop table if exists ebilling;
create external table ebilling ( BILLINGDATE string,NETQTY int,REVENUE float,CUSTOMER_KEY string,PRODUCT string,VERSION string,PRODLANGUAGE string,PRODID int,CUSTOMER string,STATE string,COUNTRY string) row format delimited fields terminated by '|' lines terminated by '\n' stored as textfile location 's3://ms.data/data/tables/billing/';
drop table if exists ecustomer;
create external table ecustomer (CUSTOMER_KEY string,DATE_RECORD_CREATED string,NAME string,TERRITORY string,LOCATIONname string,REGION string,REGION_NAME string,COUNTRY_CODE string,COUNTRY_NAME string,CUSTOMER_ACCT_GROUP_NAME string) row format delimited fields terminated by '|' lines terminated by '\n' stored as textfile location 's3://ms.data/data/tables/customer/';
drop table if exists eproduct;
create external table eproduct (MATERIAL string,PRODUCT_KEY string,LONG_DESCRIPTION string,PRODUCT_NAME string,PRODUCT_DESCRIPTION string,PRODUCT_VERSION string,PRODUCT_VERSION_DESC string,PRODUCT_LANGUAGE string,MATERIAL_DESCRIPTION string,PRODUCT_NAME_DESCRIPTION str

drop table if exists billing;
create table billing ( BILLINGDATE string,NETQTY int,REVENUE float,CUSTOMER_KEY string,PRODUCT string,VERSION string,PRODLANGUAGE string,PRODID int,CUSTOMER string,STATE string,COUNTRY string) row format delimited fields terminated by '|' lines terminated by '\n';
drop table if exists customer;
create table customer (CUSTOMER_KEY string,DATE_RECORD_CREATED string,NAME string,TERRITORY string,LOCATIONname string,REGION string,REGION_NAME string,COUNTRY_CODE string,COUNTRY_NAME string,CUSTOMER_ACCT_GROUP_NAME string) row format delimited fields terminated by '|' lines terminated by '\n';
drop table if exists product;
create table product (MATERIAL string,PRODUCT_KEY string,LONG_DESCRIPTION string,PRODUCT_NAME string,PRODUCT_DESCRIPTION string,PRODUCT_VERSION string,PRODUCT_VERSION_DESC string,PRODUCT_LANGUAGE string,MATERIAL_DESCRIPTION string,PRODUCT_NAME_DESCRIPTION string, REVENUE_MIX string);

drop table sales;
create table sales as select c.name as custname, c.customer_key as custkey, c.region, c.region_name, b.state,  p.product_name, p.product_key prodkey, netqty, revenue
  from customer c left outer join billing b on c.customer_key = b.customer_key left outer join product p on b.prodid =p.product_key;
	
	select case sum(netqty)	
create table salesrolltmp as select custkey as key, sum(netqty) totprod, 
	case sum(netqty) when < lifeqty_bin, sum(revenue) liferev, life
	from cust c left outer join billing b on c.customer_key = b.customer_key left outer join product on b.prodid =p.product_key;

create table maxmin_prodown as select max(totprod) as max_totprod, min(totprod) as min_totprod from salesrolltmp;
drop table if exists prodownbin;
create table prodownbin (	batchid	int,	custkey	int, endval int);
insert into table prodownbin select ${batchid}, floor(max_totprod* ${nprodownbins})

create table salesroll as select (max(totprod)-min(totprod)/${nprodownbins} as key, sum(netqty) tot_prod_own_cnt, 

create table custsales as select CUSTOMER_KEY as key, 
	(cnt group by 


class 

hive> select histogram_numeric(a.cust_netqty,10) from (select sum(netqty) cust_netqty from billing group by customer_key) a ;

[{"x":336.42752424508063,"y":30831.0},{"x":340970.5384615385,"y":13.0},{"x":651232.0,"y":3.0},{"x":915040.0,"y":2.0},{"x":1368629.0,"y":1.0},{"x":2403310.0,"y":1.0},{"x":2900171.0,"y":1.0},{"x":3266321.0,"y":1.0},{"x":4306124.0,"y":1.0},{"x":1.3693019E7,"y":1.0}]
Time taken: 180.589 seconds


create table netqty_bkt (bukts Array<Map<string,float>>);

create table revbkt as select histogram_numeric(a.cust_revtotal,10) from (select sum(revenue) cust_revtotal from billing group by customer_key) a ;
408 355 4211


INDV_ID|ADR_ID|FST_NME|MID_NME|LAS_NME|BRTH_DT|GNDR_CDC|MARRIED_IND|SOLC_IND|ZIP_PLS_4
personid|adressid|firstname|middle|lastname|birthdate|gender|married_ind|single_ind|zip
1|1380712|HEATHER||STRUM|1973|20|0|0|95066

hdfs dfs -copyFromLocal /data0/shared/individuals.txt /hive/tables/person/

drop table eperson;
create external table eperson (personid int,addressid int,firstname string,middlename  string,lastname  string,birthdate string,gender string,married_ind int,single_ind int,zip int) 
 row format delimited fields terminated by '|' lines terminated by '\n' stored as textfile location '/hive/tables/person/';

set hivevar:localdata=/data0/shared/;
load data local infile '${localdata}/individuals.txt overwrite into table eperson;

drop table if exists person;
create table person (personid int,addressid int,firstname string,middlename  string,lastname  string,birthdate string,gender string,zip int);
insert overwrite table person select personid, addressid, firstname, middlename, lastname, birthdate, gender, zip from eperson;

hdfs dfs -mkdir /hive/tables/address/
hdfs dfs -copyFromLocal /data0/shared/address.txt /hive/tables/address/

drop table if exists eaddress;
create external table eaddress (id int, address string, city string, zip int) 
 row format delimited fields terminated by '|' lines terminated by '\n' stored as textfile location '/hive/tables/address/';

drop table if exists address;
create table address as select * from eaddress; 

hdfs dfs -copyFromLocal /data0/shared/emailfiles.txt /hive/tables/email/
drop table if exists eemail;
create external table eemail (email string, joindate string,client_unsubscribe bool, unsubscribe int,valid int|birthdate string)
 row format delimited fields terminated by '|' lines terminated by '\n' stored as textfile location '/hive/tables/person/';
create table email as select email, valid, birthdate from eemail;

email|valid:bool|birthdate
rmullins@zone.com|False|
balongnecker@zone.com|False|1973-12-05 00:00:00

 

