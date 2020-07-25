# !/usr/bin/env Python
# coding=utf_8
import datetime
import json
import os
import re
import sys
from multiprocessing import Process, Manager, Queue

import pandas as pd
import demjson
import redis
import time
import numpy as np
import utils.wang_tool as wang_tool
from utils.提取城市 import GetCityProvince

import pymysql
import prettytable as pt


class analyseTable:
    def __init__(self, data):
        self.redis_conn = redis.StrictRedis(host='127.0.0.1', port=6379, decode_responses=True, db=0)
        self.conn_1 = pymysql.Connection(**data['localDB'])
        self.cur1 = self.conn_1.cursor()

        self.conn_2 = pymysql.Connection(**data['origDB'])
        self.cur2 = self.conn_2.cursor()

        self.city_obj = GetCityProvince()
        self.data = data
        self.origTable_pro = data['origTable_pro']
        self.origTable_room = data['origTable_room']
        self.origTable_sta = data['origTable_sta']
        self.origTableName = self.origTable_pro.split('_')[0]

        self.projTableName = data['projTableName']
        self.dataTable = data['dataTable']
        self.dataTable1 = data['dataTable1']
        self.four_table = data['FourTable']
        self.five_table = data['FiveTable']
        self.province = data['province']
        self.city = data['city']
        self.projTextColumns = data['projTextColumns']
        self.roomUniqueField = data['roomUniqueField']
        self.staStatusField = data['staStatusField']

        self.proj_spider_time_field = data['proj_spider_time_field']
        self.room_spider_time_field = data['room_spider_time_field']
        self.sta_spider_time_field = data['sta_spider_time_field']
        self.proj_merge_columns = data['proj_merge_columns']
        self.room_merge_columns = data['room_merge_columns']

        self.orig_proj_columns = self.getTableColumns(self.origTable_pro, self.conn_2)
        if 'id' in self.orig_proj_columns:
            self.orig_proj_columns.remove('id')
        self.orig_room_columns = self.getTableColumns(self.origTable_room, self.conn_2)

        self.today = datetime.datetime.now().strftime('%Y_%m_%d')
        self.result_folder_path = r'D:\resultData\%s_%s' % (self.origTableName, self.today)
        if not os.path.exists(self.result_folder_path):
            os.makedirs(self.result_folder_path)

        self.proj_exists = False
        self.data_table_exists = False
        self.data_table1_exists = False
        self.four_table_exists = False
        self.five_table_exists = False
        self.roomStatusChange = False

        df_conn_1_tables = pd.read_sql('show tables;', con=self.conn_1)
        self.conn_1_tables = df_conn_1_tables.iloc[:, 0].tolist()
        if self.projTableName in self.conn_1_tables:
            self.proj_exists = True

        if self.dataTable in self.conn_1_tables:
            self.data_table_exists = True

        if self.dataTable1 in self.conn_1_tables:
            self.data_table1_exists = True

        if self.four_table in self.conn_1_tables:
            self.four_table_exists = True

        if self.five_table in self.conn_1_tables:
            self.five_table_exists = True

        self.now_time = str(datetime.datetime.now().strftime('%Y-%m-%d'))
        error_txt_path = os.path.join(self.result_folder_path, '%s_%s.txt' % ('errorTXT', self.today))
        self.f = open(error_txt_path, 'w')  # 创建一个文本

    def getTableColumns(self, tableName, con):
        sql = """show full columns FROM `{}`""".format(tableName)
        df_column = pd.read_sql(sql, con=con)
        columns = df_column['Field'].tolist()
        return columns

    def creat_table(self):
        sql_table1 = """CREATE TABLE IF NOT EXISTS {Table} (
                    id INT(11) UNSIGNED AUTO_INCREMENT, 
                    cleantime datetime DEFAULT NULL,
                    PRIMARY KEY (id)            
                    )ENGINE=InnoDB DEFAULT CHARSET=utf8;""".format(Table=self.projTableName)

        sql_table2 = """CREATE TABLE IF NOT EXISTS {Table} (
                    id INT(11) UNSIGNED AUTO_INCREMENT, 
                    projID INT(11) DEFAULT NULL,
                    cleantime datetime DEFAULT NULL,
                    PRIMARY KEY (id)            
                    )ENGINE=InnoDB DEFAULT CHARSET=utf8;""".format(Table=self.dataTable)

        sql_table3 = """CREATE TABLE IF NOT EXISTS {Table} (
                    id INT(11) UNSIGNED AUTO_INCREMENT, 
                    projID INT(11) DEFAULT NULL,
                    unitID INT(11) DEFAULT NULL,
                    sales_status VARCHAR(10) DEFAULT NULL,
                    spider_time datetime DEFAULT NULL,
                    cleantime datetime DEFAULT NULL,
                    PRIMARY KEY (id)            
                    )ENGINE=InnoDB DEFAULT CHARSET=utf8;""".format(Table=self.dataTable1)

        # 创建表四结构
        sql_table4 = """CREATE TABLE IF NOT EXISTS`{}`  (
                  `id` int(11) NOT NULL AUTO_INCREMENT,
                  `buildID` int(11) NULL DEFAULT NULL COMMENT 'signdata_xianID',
                  `deal_status` varchar(10) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '状态',
                  `spider_time` datetime NULL DEFAULT NULL COMMENT '更新时间',
                  `cleantime` datetime NULL DEFAULT NULL COMMENT '清洗时间',
                  PRIMARY KEY (`id`) USING BTREE
                ) ENGINE = InnoDB AUTO_INCREMENT = 1298 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;""".format(
            self.four_table)

        # 创建表五结构
        sql_table5 = """CREATE TABLE `{}`  (
              `proname` varchar(100) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '项目名称',
              `deal_status` varchar(1) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '交易类型',
              `dealnum` int(11) NULL DEFAULT NULL COMMENT '套数',
              `dealarea` double(255, 2) NULL DEFAULT NULL COMMENT '面积',
              `dealamount` double(255, 2) NULL DEFAULT NULL COMMENT '金额',
              `spider_time` datetime NULL DEFAULT NULL COMMENT '爬取更新时间',
              `cleantime` datetime NULL DEFAULT NULL COMMENT '清洗时间'
            ) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;""".format(self.five_table)

        if not self.proj_exists:
            self.cur1.execute(sql_table1)
        if not self.data_table_exists:
            self.cur1.execute(sql_table2)
        if not self.data_table1_exists:
            self.cur1.execute(sql_table3)
        if not self.four_table_exists:
            self.cur1.execute(sql_table4)
        if not self.five_table_exists:
            self.cur1.execute(sql_table5)

        table_exists = [self.proj_exists, self.data_table_exists, self.data_table1_exists,self.four_table_exists,
                        self.five_table_exists]
        if all(table_exists):
            print('表都存在')
        else:
            self.conn_1.commit()
            print('建表完成')

    def alter_table(self):
        """
        读取表创建表结构
        :return:
        """
        # table1排除id
        if not self.proj_exists:
            for column in list(self.data['projcolumns_dict'].values()):
                if column in self.projTextColumns:
                    sql = """ALTER TABLE {} ADD COLUMN `{}` text DEFAULT NULL""".format(self.projTableName, column)
                else:
                    sql = """ALTER TABLE {} ADD COLUMN `{}` VARCHAR(255) DEFAULT NULL""".format(self.projTableName, column)
                self.cur1.execute(sql)
                self.conn_1.commit()

        # table2
        if not self.data_table_exists:
            for column in list(self.data['roomcolumns_dict'].values()):
                sql = """ALTER TABLE {} ADD COLUMN {} VARCHAR(255) DEFAULT NULL""".format(self.dataTable, column)
                self.cur1.execute(sql)
                self.conn_1.commit()

            # if self.roomUniqueField:
            #     sql_table2_index = """ALTER TABLE {Table} ADD unique(projID,{field});""".format(Table=self.dataTable,
            #                                                                                     field=self.roomUniqueField)
            #     self.cur1.execute(sql_table2_index)

        if all([self.proj_exists, self.data_table_exists]):
            print('无需修改表结构')
        else:
            print('新增列完成')

    def getAddDate(self, tableName, origTableName, spider_time ,orig_spider_time, conn_te):
        """
        获取数据库新增日期列表
        :return:
        """
        sql_table_date = """select distinct DATE_FORMAT({spider_time},'%Y-%m-%d')  from {Table}"""
        # print(sql_table_date.format(spider_time=spider_time, Table=tableName))
        df_table_date = pd.read_sql(sql_table_date.format(spider_time=spider_time, Table=tableName), con=self.conn_1)
        df_orig_table_date = pd.read_sql(sql_table_date.format(spider_time=orig_spider_time, Table=origTableName), con=conn_te)

        table_date = {te for te in df_table_date.iloc[:, 0].tolist() if str(te) != 'None'}
        orig_table_date = {te for te in df_orig_table_date.iloc[:, 0].tolist() if str(te) != 'None'}
        add_date = list(orig_table_date - table_date)
        return add_date

    def readOrigTable(self):
        room_columns = ','.join(self.orig_proj_columns)
        sql_proj = """select {columns}
                 from {proTable}
                 """.format(proTable=self.origTable_pro,columns=room_columns)
        sql_room = """select * from {table}""".format(table=self.origTable_room)
        sql_sta = """select * from {table}""".format(table=self.origTable_sta)

        try:
            self.conn_1.ping(reconnect=True)
        except:
            print('self.conn_1连接失败')

        if self.proj_exists:
            orig_add_date = self.getAddDate(self.projTableName, self.origTable_pro,
                                            self.data['signproj_spider_time_field'], self.proj_spider_time_field, self.conn_2)
            if orig_add_date:
                if len(orig_add_date) == 1:
                    dateList = "('{}')".format(orig_add_date[0])
                else:
                    dateList = str(tuple(orig_add_date))
                sql_proj = """select {columns}
                             from {proTable}
                             where DATE_FORMAT({spider_time},'%Y-%m-%d') in {dateList}
                             """.format(proTable=self.origTable_pro,columns=room_columns,
                                        dateList=dateList, spider_time=self.proj_spider_time_field)
            else:
                sql_proj = ''

        if self.data_table_exists:
            room_add_date = self.getAddDate(self.dataTable, self.origTable_room, self.room_spider_time_field, self.room_spider_time_field, self.conn_2)
            if room_add_date:
                if len(room_add_date) == 1:
                    dateList = "('{}')".format(room_add_date[0])
                else:
                    dateList = str(tuple(room_add_date))
                sql_room = """select *
                              from {table}
                              where DATE_FORMAT({spider_time},'%Y-%m-%d') in {dateList}
                              """.format(table=self.origTable_room, dateList=dateList, spider_time=self.room_spider_time_field)
            else:
                sql_room = ''

        if self.data_table1_exists:
            sta_add_date = self.getAddDate(self.dataTable1, self.origTable_sta, self.data['table1_spider_time_field'], self.sta_spider_time_field, self.conn_2)
            if sta_add_date:
                if len(sta_add_date) == 1:
                    dateList = "('{}')".format(sta_add_date[0])
                else:
                    dateList = str(tuple(sta_add_date))
                sql_sta = """select *
                              from {table}
                              where DATE_FORMAT({spider_time},'%Y-%m-%d') in {dateList}
                              """.format(table=self.origTable_sta, dateList=dateList, spider_time=self.sta_spider_time_field)
            else:
                sql_sta = ''
        if sql_proj:
            self.df_proj_data = pd.read_sql(sql_proj, con=self.conn_2)
            self.df_proj_data.replace('', np.nan, inplace=True)
            # self.df_proj_data.columns = list(self.data['projcolumns_dict'].values())
        else:
            self.df_proj_data = ''
        print('df_proj_data', len(self.df_proj_data))

        if sql_room:
            self.df_room = pd.read_sql(sql_room, con=self.conn_2)
            self.df_room.replace('', np.nan, inplace=True)
        else:
            self.df_room = ''
        print('df_room', len(self.df_room))
        if sql_sta:
            self.df_sta = pd.read_sql(sql_sta, con=self.conn_2)
            self.df_sta.replace('', np.nan, inplace=True)
        else:
            self.df_sta = ''
        print('df_sta', len(self.df_sta))

        self.rewrite_read()
        print('原始表读取完成')

    def rewrite_read(self):
        """
        对读取重写
        :return:
        """
        pass

    def checkColumns(self, df, errOutColumns: list, FieldCheckData):
        # 列为空个数
        if isinstance(df, pd.DataFrame):
            orig_columns = df.columns.tolist()
            orig_data_len = len(df)
            for column in orig_columns:
                if column not in FieldCheckData:
                    FieldCheckData[column] = {'空值行': []}
                FieldCheckData[column]['列值为空占比'] = \
                    df[column].isnull().sum() / orig_data_len * 100
                if FieldCheckData[column]['列值为空占比'] > 0:
                    df_empty = df[df[column].isnull()]
                    df_empty = df_empty.reset_index(drop=True)
                    for i in range(min(10, len(df_empty))):
                        err_data = []
                        for err_column in errOutColumns:
                            data = df_empty.loc[i, err_column]
                            err_data.append(data)
                        FieldCheckData[column]['空值行'].append(err_data)
            # print(FieldCheckData)
            return FieldCheckData

    def checkOrigTable(self):
        projOrigFieldCheckData = {}
        roomOrigFieldCheckData = {}
        staOrigFieldCheckData = {}
        projOrigFieldCheckData = self.checkColumns(self.df_proj_data, self.data['proj_err_out_columns'], projOrigFieldCheckData)
        roomOrigFieldCheckData = self.checkColumns(self.df_room, self.data['room_err_out_columns'], roomOrigFieldCheckData)
        staOrigFieldCheckData = self.checkColumns(self.df_sta, self.data['sta_err_out_columns'], staOrigFieldCheckData)

        def saveColumnNanValue(FieldcheckData: dict):
            for column, data in FieldcheckData.items():
                self.f.write('列名:%s\t占比:%.2f%%\n' % (column, data['列值为空占比']))
                if data['空值行']:
                    for err_line in data['空值行']:
                        self.f.write('列关键信息:%s\n' % (', '.join(err_line)))
                self.f.write('\n' + '-'*50 + '\n')

        self.f.write('proj表 空行及空行占比\n')
        saveColumnNanValue(projOrigFieldCheckData)
        self.f.write('room表 空行及空行占比\n')
        saveColumnNanValue(roomOrigFieldCheckData)
        self.f.write('sta表 空行及空行占比\n')
        saveColumnNanValue(staOrigFieldCheckData)

    def save_text(self, contents):
        """
        保存错误文本
        :param file_path: 文件地址
        :param contents: 保存内容
        :return:
        """
        for content in contents:
            self.f.write('\t'.join(content) + '\n')

    def cleanOrigTable(self):
        """清洗列表"""
        def extral_number(x, columns_name, table_name):
            if str(x) == 'nan':
                return ''
            res_list = re.findall(r'[.\d]+', str(x))
            if len(res_list) > 1 or len(res_list) == 0:
                if len(error_list) <= 10:
                    error_list.append([table_name, columns_name, str(x), '转换数字大于一个或者为空'])
                res_list = []
            re_str = ''.join(res_list)
            return re_str

        # 清洗项目表
        proj_number_columns_list = self.data['proj']['number']
        if proj_number_columns_list:
            for te_columns in proj_number_columns_list:
                error_list = []
                self.df_proj_data[te_columns] = self.df_proj_data[te_columns].map(lambda x: extral_number(x, te_columns, '项目表'))
                if len(error_list):
                    self.save_text(error_list)

        # 清洗房间表
        room_number_columns_list = self.data['room']['number']
        if room_number_columns_list:
            for room_columns in room_number_columns_list:
                error_list = []
                self.df_room[room_columns] = self.df_room[room_columns].map(lambda x: extral_number(x, te_columns, '房间表'))
                if len(error_list):
                    self.save_text(error_list)

    def loadCsvData(self, csv_path, columns, table):

        sql = """LOAD DATA INFILE '{csvPath}'
                 IGNORE INTO TABLE {table}
                 FIELDS TERMINATED BY ','
                 OPTIONALLY ENCLOSED BY '"'
                 lines terminated by '\r\n'
                 ignore 1 lines
                 ({columns});""".format(csvPath=csv_path.replace('\\','/'),columns=','.join(columns),
                                        table=table)
        # print(sql)
        try:
            self.conn_1.ping(reconnect=True)
        except:
            print('self.conn_1连接失败')

        try:
            self.cur1.execute(sql)
            self.conn_1.commit()
        except pymysql.Error as e:
            print(e)
        finally:
            self.conn_1.close()

    def origDataReplace(self, df):
        columns = df.columns.tolist()
        for column in columns:
            if isinstance(df.loc[0, column], str):
                try:
                    df[column] = df[column].str.replace('\\', '')
                except:
                    pass
        return df

    def insertProj(self):
        """
        插入项目表
        """
        if isinstance(self.df_proj_data, pd.DataFrame):
            sort_columns = ['cleantime'] + list(self.data['projcolumns_dict'].keys())
            new_columns = ['cleantime'] + list(self.data['projcolumns_dict'].values())
            self.df_proj_data['cleantime'] = self.now_time
            self.df_proj_data = self.df_proj_data[sort_columns]
            self.df_proj_data.fillna('-', inplace=True)
            self.df_proj_data = self.origDataReplace(self.df_proj_data)

            csv_path = os.path.join(self.result_folder_path, '%s_%s.csv' % (self.origTable_pro, self.today))
            self.df_proj_data.to_csv(csv_path, index=False, encoding='utf_8')

            self.loadCsvData(csv_path, new_columns, self.projTableName)
            print('proj表插入成功')
        else:
            print('proj表无更新数据')

    def insetDataTable(self):
        """
        插入第1张数据表
        """
        if isinstance(self.df_room, pd.DataFrame):
            sort_columns = ['projID', 'cleantime'] + list(self.data['roomcolumns_dict'].keys())
            new_columns = ['projID', 'cleantime'] + list(self.data['roomcolumns_dict'].values())
            sql_proj = """select * from {table}""".format(table=self.projTableName)
            try:
                self.conn_1.ping(reconnect=True)
            except:
                print('self.conn_1连接失败')

            self.df_proj = pd.read_sql(sql_proj, con=self.conn_1)
            self.df_proj.rename(columns={'id': 'projID'}, inplace=True)
            self.df_room = self.df_room.merge(self.df_proj[['projID'] + self.proj_merge_columns],
                                              left_on=self.room_merge_columns, right_on=self.proj_merge_columns,
                                              how='left')
            self.df_room['cleantime'] = self.now_time
            self.df_room = self.df_room[sort_columns]

            self.df_room_err = self.df_room[self.df_room['projID'].isnull()]
            if not self.df_room_err.empty:
                df_room_err_csv_path = os.path.join(self.result_folder_path,
                                                   '%s_%s_%s.csv' % (self.origTable_room, '匹配不到id', self.today))
                self.df_room_err.to_csv(df_room_err_csv_path, index=False, encoding='utf-8')

            self.df_room = self.df_room[self.df_room['projID'].notnull()]

            csv_path = os.path.join(self.result_folder_path, '%s_%s.csv' % (self.origTable_room, self.today))
            self.df_room.to_csv(csv_path, index=False, encoding='utf-8')
            self.loadCsvData(csv_path, new_columns, self.dataTable)
            print('room表插入成功')
        else:
            print('room表无更新数据')

    def insetDataTable_one(self):
        """
        插入第二张数据表
        """
        if isinstance(self.df_sta, pd.DataFrame):
            try:
                self.conn_1.ping(reconnect=True)
            except:
                print('self.conn_1连接失败')

            sql_table_data = """select * from {table}""".format(table=self.dataTable)
            self.df_data_table = pd.read_sql(sql_table_data, con=self.conn_1)
            self.df_data_table.rename(columns={'id': 'buildID'}, inplace=True)
            self.df_data_table = self.df_data_table[['projID', 'buildID', self.roomUniqueField]]

            self.df_sta = self.df_sta.merge(self.df_data_table, on=[self.roomUniqueField], how='left')
            self.df_sta_err = self.df_sta[self.df_sta['buildID'].isnull()]
            if not self.df_sta_err.empty:
                df_sta_err_csv_path = os.path.join(self.result_folder_path,
                                                   '%s_%s_%s.csv' % (self.origTable_sta, '匹配不到id', self.today))
                self.df_sta_err.to_csv(df_sta_err_csv_path, index=False, encoding='utf-8')

            self.df_sta = self.df_sta[self.df_sta['buildID'].notnull()]
            sort_columns = ['projID', 'buildID', self.staStatusField, self.sta_spider_time_field]
            self.df_sta = self.df_sta[sort_columns]
            self.df_sta['cleantime'] = self.now_time

            csv_path = os.path.join(self.result_folder_path, '%s_%s.csv' % (self.origTable_sta, self.today))
            self.df_sta.to_csv(csv_path, index=False, encoding='utf-8')

            db_columns = ['projID', 'unitID', 'sales_status', 'spider_time', 'cleantime']
            self.loadCsvData(csv_path, db_columns, self.dataTable1)
            print('sta表插入成功')
        else:
            print('sta表无更新数据')

    # 开始生成第四第五张表
    def getFourDate(self, tableName, origTableName, spider_time, orig_spider_time):
        """
        获取数据库新增日期列表
        :return:
        """
        sql_table_date = """select distinct DATE_FORMAT({spider_time},'%Y-%m-%d')  from {Table}"""
        df_table_date = pd.read_sql(sql_table_date.format(spider_time=spider_time, Table=tableName), con=self.conn_1)
        df_orig_table_date = pd.read_sql(sql_table_date.format(spider_time=orig_spider_time, Table=origTableName), con=self.conn_1)

        table_date = {te for te in df_table_date.iloc[:, 0].tolist() if str(te) != 'None' and not str(te).startswith('0000')}
        orig_table_date = {te for te in df_orig_table_date.iloc[:, 0].tolist() if str(te) != 'None' and not str(te).startswith('0000')}
        add_date = list(orig_table_date - table_date)  # 库中的第四张表和原始库中的状态表日期的差值

        # 筛选出来当前库中需要更新的日期和前一天额日期
        result_set = set()
        sort_orig_table_list = list(orig_table_date)
        sort_orig_table_list.sort()
        for now_date in add_date:
            now_index = sort_orig_table_list.index(now_date)
            try:
                if now_index == 0:
                    continue
                last_date = sort_orig_table_list[now_index -1]
                result_set.add(now_date)
                result_set.add(last_date)
            except:
                pass
        return list(result_set)
    
    def create_four(self):
        """
        生成第四张表
        """
        try:
            self.conn_1.ping(reconnect=True)
        except:
            print('self.conn_1连接失败')
        self.roomStatusChange = False

        def create_dict_mess(x, result_dic):
            dic = {wang_tool.date_number(x.values[0][4]): x.values.tolist()}
            result_dic.update(dic)

        def read_three_mess():
            """
            读取状态表
            :return:
            """
            dict_mess = {}
            dateList = self.getFourDate(self.four_table, self.dataTable1, self.data['FourTable_spider_time_field'],
                                        self.data['table1_spider_time_field'])
            # print(dateList)
            if dateList:
                sql = """select * from `{three_table}` where DATE_FORMAT({spider_time},'%Y-%m-%d') in {dateList}
                               """.format(three_table=self.dataTable1, spider_time=self.data['table1_spider_time_field'],
                                          dateList=tuple(dateList))
                city_df = pd.read_sql(sql, con=self.conn_1)
                city_df.groupby(['spider_time']).apply(create_dict_mess, dict_mess)
            return dict_mess

        dict_mess = read_three_mess()
        keys = list(dict_mess.keys())
        keys.sort()
        # print(keys)
        index_ = 0
        while index_ + 1 < len(keys):
            datas = dict_mess[keys[index_]] + dict_mess[keys[index_ + 1]]
            result_dict = {}
            for data in datas:
                projID, buildID, status, spider_time = data[1], data[2], data[3], str(data[4])
                key = ','.join(map(lambda x: str(x), [projID, buildID]))
                if key not in result_dict:
                    result_dict[key] = [spider_time, status, None]
                else:
                    last_spider_time, last_status = result_dict[key][0], result_dict[key][1]
                    if wang_tool.date_number(last_spider_time) < wang_tool.date_number(spider_time):
                        if self.data['status_top_all'][str(last_status)] > self.data['status_top_all'][str(status)]:
                            result_dict[key][2] = '2'
                        elif self.data['status_top_all'][str(last_status)] < self.data['status_top_all'][str(status)]:
                            result_dict[key][2] = '1'
                        else:
                            pass
                        # 更新成最新的状态
                        result_dict[key][0], result_dict[key][1] = [spider_time, status]

                    elif wang_tool.date_number(last_spider_time) > wang_tool.date_number(spider_time):
                        if self.data['status_top_all'][str(last_status)] > self.data['status_top_all'][str(status)]:
                            result_dict[key][2] = '1'
                        elif self.data['status_top_all'][str(last_status)] < self.data['status_top_all'][str(status)]:
                            result_dict[key][2] = '2'
                        else:
                            pass

                    else:
                        if self.data['status_top_all'][str(last_status)] > self.data['status_top_all'][str(status)]:
                            result_dict[key][2] = '1'
                        elif self.data['status_top_all'][str(last_status)] < self.data['status_top_all'][str(status)]:
                            result_dict[key][2] = '2'
                        else:
                            pass

            for key, value_list in result_dict.items():
                projID, buildID = key.split(',')
                spider_time, status, result_status = value_list
                if result_status:
                    self.roomStatusChange = True
                    param = [int(buildID), result_status, wang_tool.get_date_y_m_d(spider_time), self.now_time]
                    temp_str = ','.join(map(lambda x: '%s', param))
                    sql = """insert into {}(buildID, deal_status, spider_time, cleantime) values({})""".format(self.four_table, temp_str)
                    self.cur1.execute(sql, param)
                    self.conn_1.commit()
            index_ += 1
        print('生成第四张表完成')

    def create_five(self):
        """
        第五张表
        :return:
        """
        if self.roomStatusChange:
            dataList = self.getAddDate( self.five_table, self.four_table, self.data['FiveTable_spider_time_field'],
                                        self.data['FourTable_spider_time_field'], self.conn_1)
            if len(dataList) == 1:
                self.five_dateList = "('{}')".format(dataList[0])
            else:
                self.five_dateList = str(tuple(dataList))

            self.save_five()

    def save_five(self):
        """
        重写函数
        :return:
        """
        pass

    def countOrigTable(self):
        count_txt_path = os.path.join(self.result_folder_path, '%s_%s.txt' % ('countTXT', self.today))
        # 项目数
        sql_proj_num = """select count(*) as projTotalnum from {table}""".format(table=self.origTable_pro)
        df_sql_proj_num = pd.read_sql(sql_proj_num, con=self.conn_2)

        # 项目增加/减少数
        sql_table_date = """select distinct DATE_FORMAT({spider_time},'%Y-%m-%d')  from {Table} 
                            where DATE_FORMAT({spider_time},'%Y-%m-%d') != '0000-00-00'"""
        df_orig_table_date = pd.read_sql(sql_table_date.format(spider_time=self.data['table1_spider_time_field'],
                                                               Table=self.dataTable1), con=self.conn_1)
        df_orig_table_date = [i for i in df_orig_table_date.iloc[:, 0].tolist() if i != 'None']
        df_orig_table_date = pd.to_datetime(df_orig_table_date)
        recent_two_day = sorted(df_orig_table_date)[-2:]
        recent_two_day = list(map(lambda x:x.strftime('%Y-%m-%d'), recent_two_day))

        sql_orig_proj = """select DATE_FORMAT({spider_time},'%Y-%m-%d') as '日期',count(distinct projID) as '项目数',
                                count(distinct unitID) as '房间数'
                           from {Table}
                           where DATE_FORMAT({spider_time},'%Y-%m-%d') in {date_list}
                           group by DATE_FORMAT({spider_time},'%Y-%m-%d')
                        """.format(spider_time=self.data['table1_spider_time_field'], Table=self.dataTable1,
                                   date_list=tuple(recent_two_day))
        df_recent_two_day = pd.read_sql(sql_orig_proj, con=self.conn_1)

        # 房间数
        sql_orig_room = """select DATE_FORMAT({spider_time},'%Y-%m-%d') as date,deal_status as '房间状态变化',
                                count(deal_status) as '状态变化数量'
                           from {Table}
                           group by DATE_FORMAT({spider_time},'%Y-%m-%d'), deal_status
                        """.format(spider_time=self.data['FourTable_spider_time_field'], Table=self.four_table)
        df_sql_orig_room = pd.read_sql(sql_orig_room, con=self.conn_1)

        with open(count_txt_path, 'w') as f:
            f.write('\n项目数\n')
            for i in df_recent_two_day[['日期', '项目数']].values.tolist():
                s = '日期:%s,  项目数:%s\n' % (i[0], i[1])
                f.write(s)

            proj_num_before = df_recent_two_day.loc[df_recent_two_day['日期']==recent_two_day[0], '项目数'].values[0]
            proj_num_last = df_recent_two_day.loc[df_recent_two_day['日期']==recent_two_day[1], '项目数'].values[0]
            proj_diff_num = proj_num_last - proj_num_before
            if proj_diff_num >= 0:
                f.write('最近两天项目增加数: %d\n' % proj_diff_num)
            else:
                f.write('最近两天项目减少数: %d\n' % proj_diff_num)

            f.write('\n房间数\n')
            for i in df_recent_two_day[['日期', '房间数']].values.tolist():
                s = '日期:%s,  房间数:%s\n' % (i[0], i[1])
                f.write(s)

            room_num_before = df_recent_two_day.loc[df_recent_two_day['日期']==recent_two_day[0], '房间数'].values[0]
            room_num_last = df_recent_two_day.loc[df_recent_two_day['日期']==recent_two_day[1], '房间数'].values[0]
            room_diff_num = room_num_last - room_num_before
            if proj_diff_num >= 0:
                f.write('最近两天房间增加数: %d\n' % room_diff_num)
            else:
                f.write('最近两天房间减少数: %d\n' % room_diff_num)

            f.write('\n房间状态变化\n')
            for i in df_sql_orig_room.values.tolist():
                s = '日期:%s,  房间状态:%s,  状态变化数量:%s\n' % (i[0], i[1], i[2])
                f.write(s)


    def anlyse(self):
        self.creat_table()
        self.alter_table()
        self.readOrigTable()  # 读取
        self.checkOrigTable()
        self.cleanOrigTable()  # 清洗数据
        self.insertProj()
        self.insetDataTable()
        self.insetDataTable_one()
        self.create_four()  # 生成第四张表
        self.create_five()  # 生成第五张表
        self.countOrigTable()  # 统计函数
        self.f.close()

if __name__ == '__main__':
    start = time.time()
    from data.StatusDict import *
    t1 = analyseTable(shenzhen)
    t1.anlyse()
    print(time.time() - start)
