# !/usr/bin/env Python
# coding=utf-8
import pymysql
import pandas as pd

conn_1 = pymysql.Connection(host='192.168.1.47', port=3306, user='root', password='root', db='网签清洗', charset="utf8")
cur1 = conn_1.cursor()

sql = """
LOAD DATA INFILE 'D:/resultData/guangzhou0101_2020_07_23/guangzhou0101_pro_2020_07_23.csv'
                 IGNORE INTO TABLE signproj_guagnzhou
                 CHARACTER SET utf8
                 FIELDS TERMINATED BY ','
                 OPTIONALLY ENCLOSED BY '"'
                 lines terminated by '\r\n'
                 ignore 1 lines
                 (proname,company,position,ca_num,region,city,province,dataTable,spider_time,pjid_original,resid_onsalenum_original,resid_unsoldnum_original,district_original,area_m2_original,scale_m2_original,certificate_no_original,use_type_original,appro_num_original,appro_area_original,soldtomum_original,unsoldnum_original,sold_area_m2_original,unsold_area_m2_original,url_original,updatatime_original)
"""
try:
    conn_1.ping(reconnect=True)
except:
    print('self.conn_1连接失败')


cur1.execute(sql)
conn_1.commit()
# df = pd.read_csv(r'D:/resultData/111guangzhou0101_2020_07_23/111guangzhou0101_pro_2020_07_23.csv')
# for value in df.values:
#     sql = """insert into signproj_guagnzhou_copy1(proname,company,position,ca_num,region,city,province,dataTable,spider_time,pjid_original,resid_onsalenum_original,resid_unsoldnum_original,district_original,area_m2_original,scale_m2_original,certificate_no_original,use_type_original,appro_num_original,appro_area_original,soldtomum_original,unsoldnum_original,sold_area_m2_original,unsold_area_m2_original,url_original,updatatime_original) values({})"""