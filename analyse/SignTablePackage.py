import datetime
import json
import os
import sys
from multiprocessing import Process
import pandas as pd
import demjson
import redis
import time

import my_package.update_base_data.wang_tool as wang_tool
from 获取城市信息.提取城市 import GetCityProvince

import pymysql


class analyseTable:
    def __init__(self, data):
        self.conn_1 = pymysql.Connection(host='192.168.1.47', port=3306, user='root', password='root', db='网签清洗',
                                    charset="utf8")
        self.cur1 = self.conn_1.cursor()

        self.conn_2 = pymysql.Connection(host=data['orgiIP'], port=3306, user='root', password=data['orgipassword'], db='wangqian',
                                    charset="utf8")
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
        self.columnIndex = data['columnIndex']
        self.province = data['province']
        self.city = data['city']
        self.projTextColumns = data['projTextColumns']
        self.roomUniqueField = data['roomUniqueField']
        self.roomStatusField = data['roomStatusField']

        self.orig_proj_columns = self.getTableColumns(self.origTable_pro, self.conn_2)
        if 'id' in self.orig_proj_columns:
            self.orig_proj_columns.remove('id')
        self.orig_room_columns = self.getTableColumns(self.origTable_room, self.conn_2)

        self.orig_proj_columns_modify = [i.replace('-','_') + '_original' for i in self.orig_proj_columns]
        self.orig_room_columns_modify = [i.replace('-','_') for i in self.orig_room_columns]

        self.today = datetime.datetime.now().strftime('%Y_%m_%d')
        self.result_folder_path = r'D:\resultData\%s_%s' % (self.origTableName, self.today)
        if not os.path.exists(self.result_folder_path):
            os.makedirs(self.result_folder_path)

    def getTableColumns(self, tableName, con):
        sql = """show full columns FROM `{}`""".format(tableName)
        df_column = pd.read_sql(sql, con=con)
        columns = df_column['Field'].tolist()
        return columns

    def creat_table(self):
        sql_table1 = """CREATE TABLE IF NOT EXISTS {Table} (
                    id INT(11) UNSIGNED AUTO_INCREMENT, 
                    proname VARCHAR(255) DEFAULT NULL,
                    company VARCHAR(100) DEFAULT NULL,
                    position VARCHAR(255) DEFAULT NULL,
                    ca_num VARCHAR(255) DEFAULT NULL,
                    region VARCHAR(20) DEFAULT NULL,
                    city VARCHAR(20) DEFAULT NULL,
                    province VARCHAR(20) DEFAULT NULL,
                    dataTable VARCHAR(255) DEFAULT NULL,
                    spider_time VARCHAR(255) DEFAULT NULL,
                    UNIQUE INDEX (proname, company, position, city, ca_num),
                    PRIMARY KEY (id)            
                    )ENGINE=InnoDB DEFAULT CHARSET=utf8;""".format(Table=self.projTableName)

        sql_table2 = """CREATE TABLE IF NOT EXISTS {Table} (
                    id INT(11) UNSIGNED AUTO_INCREMENT, 
                    projID INT(11) DEFAULT NULL,
                    PRIMARY KEY (id)            
                    )ENGINE=InnoDB DEFAULT CHARSET=utf8;""".format(Table=self.dataTable)

        sql_table3 = """CREATE TABLE IF NOT EXISTS {Table} (
                    id INT(11) UNSIGNED AUTO_INCREMENT, 
                    projID INT(11) DEFAULT NULL,
                    buildID INT(11) DEFAULT NULL,
                    status VARCHAR(10) DEFAULT NULL,
                    状态变化 VARCHAR(10) DEFAULT NULL,
                    spider_time VARCHAR(10) DEFAULT NULL,
                    UNIQUE INDEX (projID, buildID, spider_time),
                    PRIMARY KEY (id)            
                    )ENGINE=InnoDB DEFAULT CHARSET=utf8;""".format(Table=self.dataTable1)

        # 创建表四结构
        sql_table4 = """CREATE TABLE IF NOT EXISTS`{}`  (
                  `id` int(11) NOT NULL AUTO_INCREMENT,
                  `projID` int(11) NULL DEFAULT NULL COMMENT '项目ID',
                  `buildID` int(11) NULL DEFAULT NULL COMMENT 'signdata_xianID',
                  `change_status` varchar(10) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '状态',
                  `spider_time` datetime(0) NULL DEFAULT NULL COMMENT '更新时间',
                  PRIMARY KEY (`id`) USING BTREE
                ) ENGINE = InnoDB AUTO_INCREMENT = 1298 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;""".format(
            self.four_table)

        # 创建表五结构
        sql_table5 = """CREATE TABLE IF NOT EXISTS `{}`  (
          `id` int(11) NOT NULL AUTO_INCREMENT,
          `city` varchar(10) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '城市',
          `sale_num` int(11) NULL DEFAULT NULL COMMENT '销售套数',
          `sale_area` double NULL DEFAULT NULL COMMENT '销售面积',
          `sale_money` double NULL DEFAULT NULL COMMENT '销售金额',
          `spider_time` datetime(0) NULL DEFAULT NULL COMMENT '更新时间',
          PRIMARY KEY (`id`) USING BTREE
        ) ENGINE = InnoDB AUTO_INCREMENT = 3 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;""".format(self.five_table)

        self.cur1.execute(sql_table1)
        self.cur1.execute(sql_table2)
        self.cur1.execute(sql_table3)
        self.cur1.execute(sql_table4)
        self.cur1.execute(sql_table5)
        self.conn_1.commit()
        print('建表完成')

    def alter_table(self):
        """
        读取表创建表结构
        :return:
        """
        # table1排除id
        for column in self.orig_proj_columns_modify:
            if column in self.projTextColumns:
                sql = """ALTER TABLE {} ADD COLUMN {} text DEFAULT NULL""".format(self.projTableName, column)
            else:
                sql = """ALTER TABLE {} ADD COLUMN {} VARCHAR(255) DEFAULT NULL""".format(self.projTableName, column)
            self.cur1.execute(sql)
            self.conn_1.commit()

        # table2
        for column in self.orig_room_columns_modify:
            sql = """ALTER TABLE {} ADD COLUMN {} VARCHAR(255) DEFAULT NULL""".format(self.dataTable, column)
            self.cur1.execute(sql)
            self.conn_1.commit()

        if self.roomUniqueField:
            sql_table2_index = """ALTER TABLE {Table} ADD unique(projID,{field});""".format(Table=self.dataTable,
                                                                                            field=self.roomUniqueField)
            self.cur1.execute(sql_table2_index)
            self.conn_1.commit()
        print('新增列完成')

    def insertProjTable(self):
        """
        入库项目表
        """
        for column in self.columnIndex.keys():
            self.df_proj_data[column] = ''

        for i in range(len(self.df_proj_data)):
            for column in self.columnIndex.keys():
                if self.columnIndex[column]:
                    self.df_proj_data.loc[i, column] = self.df_proj_data.loc[i, self.columnIndex[column]]
                else:
                    self.df_proj_data.loc[i, column] = '-'

            if self.columnIndex['position']:
                position = self.df_proj_data.loc[i, self.columnIndex['position']]
                city, region, province = self.city_obj.get_city_no_province(position, extral_province=self.province)
                if city is None or province is None or city != self.city:
                    city = self.city
                    province = self.province
                self.df_proj_data.loc[i, 'region'] = region
                self.df_proj_data.loc[i, 'city'] = city
                self.df_proj_data.loc[i, 'province'] = province
                self.df_proj_data.loc[i, 'dataTable'] = self.origTable_pro
        print('proj表数据处理完成')

    def loadCsvData(self, csv_path, columns, table):
        sql = """LOAD DATA INFILE '{csvPath}'
                 IGNORE INTO TABLE {table}
                 FIELDS TERMINATED BY ','
                 OPTIONALLY ENCLOSED BY '"'
                 lines terminated by '\r\n'
                 ignore 1 lines
                 ({columns});""".format(csvPath=csv_path.replace('\\','/'),columns=','.join(columns),
                                        table=table)
        try:
            self.cur1.execute(sql)
            self.conn_1.commit()
        except pymysql.Error as e:
            print(e)
        finally:
            self.conn_1.close()

    def insertProj(self):
        """
        插入项目表
        """
        room_columns = ','.join(self.orig_proj_columns)
        dataTable_columns = ','.join(self.orig_proj_columns_modify)
        sql_insert_room = """select {columns}
                 from {proTable}
                 """.format(proTable=self.origTable_pro,columns=room_columns)
        self.df_proj_data = pd.read_sql(sql_insert_room, con=self.conn_2)
        self.df_proj_data.fillna('', inplace=True)
        self.df_proj_data.columns = self.orig_proj_columns_modify

        self.insertProjTable()
        sort_columns = list(self.columnIndex.keys()) + self.orig_proj_columns_modify
        self.df_proj_data = self.df_proj_data[sort_columns]
        self.df_proj_data.replace('', '-',inplace=True)

        csv_path = os.path.join(self.result_folder_path, '%s_%s.csv' % (self.origTable_pro, self.today))
        self.df_proj_data.to_csv(csv_path, index=False, encoding='utf-8-sig')

        self.loadCsvData(csv_path, sort_columns, self.projTableName)
        print('proj表插入成功')

    def insetDataTable(self):
        """
        插入第一张数据表
        """
        sort_columns = ['projID'] + self.orig_room_columns_modify
        sql_proj = """select * from {table}""".format(table=self.projTableName)
        sql_room = """select * from {table}""".format(table=self.origTable_room)
        try:
            self.conn_1.ping(reconnect=True)
        except:
            print('self.conn_1连接失败')

        self.df_room = pd.read_sql(sql_room, con=self.conn_2)
        room_columns = self.df_room.columns.tolist()

        self.df_proj = pd.read_sql(sql_proj, con=self.conn_1)
        self.df_proj.rename(columns={'id':'projID'},inplace=True)
        self.df_room = self.df_room.merge(self.df_proj[['projID', 'proname','ca_num']], on=['proname','ca_num'], how='left')

        room_columns_new = ['projID'] + room_columns
        self.df_room = self.df_room[room_columns_new]

        csv_path = os.path.join(self.result_folder_path, '%s_%s.csv' % (self.origTable_room, self.today))
        self.df_room.to_csv(csv_path, index=False, encoding='utf-8-sig')

        self.loadCsvData(csv_path, sort_columns, self.dataTable)
        print('room表插入成功')

    def insetDataTable_one(self):
        """
        插入第二张数据表
        """
        try:
            self.conn_1.ping(reconnect=True)
        except:
            print('self.conn_1连接失败')

        sql_table_data = """select * from {table}""".format(table=self.dataTable)
        self.df_data_table = pd.read_sql(sql_table_data, con=self.conn_1)
        self.df_data_table.rename(columns={'id': 'buildID', self.roomStatusField:'status'}, inplace=True)
        self.df_data_table = self.df_data_table[['projID', 'buildID', 'status', 'spidertime']]

        csv_path = os.path.join(self.result_folder_path, '%s_%s.csv' % (self.origTable_sta, self.today))
        self.df_data_table.to_csv(csv_path, index=False, encoding='utf-8-sig')

        sort_columns = ['projID', 'buildID', 'status', 'spider_time']
        self.loadCsvData(csv_path, sort_columns, self.dataTable1)
        print('sta表插入成功')

    # 开始生成第四第五张表
    def read_three_mess(self):
        """
        读取状态表
        :return:
        """
        self.dict_mess = {}
        sql = """select * from `{}`""".format(self.dataTable1)
        # sql = """select * from `{}` where id in (1, 3360470, 5704678)""".format(self.three_table)
        city_df = pd.read_sql(sql, con=self.conn_1)
        city_df.groupby(['spider_time']).apply(self.create_dict_mess, self.dict_mess)

    def create_dict_mess(self, x, result_dic):
        dic = {wang_tool.date_number(x.values[0][5]): x.values.tolist()}
        result_dic.update(dic)

    def create_four(self):
        """
        生成第三张表
        """
        self.read_three_mess()
        keys = list(self.dict_mess.keys())
        keys.sort()
        index_ = 0
        while index_ + 1 < len(keys):
            datas = self.dict_mess[keys[index_]] + self.dict_mess[keys[index_ + 1]]
            result_dict = {}
            for data in datas:
                projID, buildID, status, spider_time = data[1], data[2], data[3], str(data[5])
                key = ','.join(map(lambda x: str(x), [projID, buildID]))
                if key not in result_dict:
                    result_dict[key] = [spider_time, status, None]
                else:
                    last_spider_time, last_status = result_dict[key][0], result_dict[key][1]
                    if wang_tool.date_number(last_spider_time) < wang_tool.date_number(spider_time):
                        if self.data['status_top_all'][str(last_status)] > self.data['status_top_all'][str(status)]:
                            result_dict[key][2] = '退房'
                        elif self.data['status_top_all'][str(last_status)] < self.data['status_top_all'][str(status)]:
                            result_dict[key][2] = '成交'
                        else:
                            pass
                        # 更新成最新的状态
                        result_dict[key][0], result_dict[key][1] = [spider_time, status]

                    elif wang_tool.date_number(last_spider_time) > wang_tool.date_number(spider_time):
                        if self.data['status_top_all'][str(last_status)] > self.data['status_top_all'][str(status)]:
                            result_dict[key][2] = '成交'
                        elif self.data['status_top_all'][str(last_status)] < self.data['status_top_all'][str(status)]:
                            result_dict[key][2] = '退房'
                        else:
                            pass

                    else:
                        if self.data['status_top_all'][str(last_status)] > self.data['status_top_all'][str(status)]:
                            result_dict[key][2] = '成交'
                        elif self.data['status_top_all'][str(last_status)] < self.data['status_top_all'][str(status)]:
                            result_dict[key][2] = '退房'
                        else:
                            pass

            for key, value_list in result_dict.items():
                projID, buildID = key.split(',')
                spider_time, status, result_status = value_list
                if result_status:
                    param = [int(projID), int(buildID), result_status, wang_tool.get_date_y_m_d(spider_time)]
                    temp_str = ','.join(map(lambda x: '%s', param))
                    sql = """insert into {}(projID, buildID, change_status, spider_time) values({})""".format(self.four_table, temp_str)
                    self.cur1.execute(sql, param)
                    self.conn_1.commit()
            index_ += 1
        print('生成第四张表完成')

    def create_five(self):
        """
        生成第五张表
        :return:
        """
        sql = """select id, {} from `{}`""".format(self.data['area'], self.dataTable)
        self.cur1.execute(sql)
        dic = {data[0]: data[1] for data in self.cur1.fetchall()}

        re_dic = {}
        sql = """select * from `{}` where change_status = '成交'""".format(self.four_table)
        self.cur1.execute(sql)
        datas = self.cur1.fetchall()
        for data in datas:
            build = data[2]
            spider_time = wang_tool.get_date_y_m_d(str(data[4]))
            bu_area = dic[build]
            if spider_time not in re_dic:
                re_dic[spider_time] = [0, 0, 0]
            re_dic[spider_time][0] += 1
            try:
                re_dic[spider_time][1] += float(bu_area)
            except:
                pass
        for spi_time, value in re_dic.items():
            sale_num, sale_Area, sale_money = value
            param = [self.city, sale_num, sale_Area, sale_money, spi_time]
            temp_str = ','.join(map(lambda x: '%s', param))
            sql = """insert into {}(city, sale_num, sale_area, sale_money, spider_time) values({})""".format(self.five_table ,temp_str)
            self.cur1.execute(sql, param)
            self.conn_1.commit()

        print('生成第五张表完成')

    def update_four(self, last_time, now_time):
        """
        更新第四张表
        :param last_time: 上一天时间
        :param now_time: 当前时间
        :return:
        """
        result_dict = {}
        sql = """select * from `{}` where spider_time in ('{}', '{}')""".format(self.dataTable1, last_time, now_time)
        self.cur1.execute(sql)
        datas = self.conn_1.commit()
        for data in datas:
            projID, buildID, status, spider_time = data[1], data[2], data[3], str(data[5])
            key = ','.join(map(lambda x: str(x), [projID, buildID]))
            if key not in result_dict:
                result_dict[key] = [spider_time, status, None]
            else:
                last_spider_time, last_status = result_dict[key][0], result_dict[key][1]
                if wang_tool.date_number(last_spider_time) < wang_tool.date_number(spider_time):
                    if self.data['status_top'][str(last_status)] > self.data['status_top'][str(status)]:
                        result_dict[key][2] = '退房'
                    elif self.data['status_top'][str(last_status)] < self.data['status_top'][str(status)]:
                        result_dict[key][2] = '成交'
                    else:
                        pass
                    # 更新成最新的状态
                    result_dict[key][0], result_dict[key][1] = [spider_time, status]

                elif wang_tool.date_number(last_spider_time) > wang_tool.date_number(spider_time):
                    if self.data['status_top'][str(last_status)] > self.data['status_top'][str(status)]:
                        result_dict[key][2] = '成交'
                    elif self.data['status_top'][str(last_status)] < self.data['status_top'][str(status)]:
                        result_dict[key][2] = '退房'
                    else:
                        pass

                else:
                    if self.data['status_top'][str(last_status)] > self.data['status_top'][str(status)]:
                        result_dict[key][2] = '成交'
                    elif self.data['status_top'][str(last_status)] < self.data['status_top'][str(status)]:
                        result_dict[key][2] = '退房'
                    else:
                        pass

        for key, value_list in result_dict.items():
            projID, buildID = key.split(',')
            spider_time, status, result_status = value_list
            if result_status:
                param = [int(projID), int(buildID), result_status, wang_tool.get_date_y_m_d(spider_time)]
                temp_str = ','.join(map(lambda x: '%s', param))
                sql = """insert into {}(projID, buildID, change_status, spider_time) values({})""".format(
                    self.four_table, temp_str)
                self.cur1.execute(sql, param)
                self.conn_1.commit()

        # 更新第五张表
        sql = """select id, {} from `{}`""".format(self.data['area'], self.dataTable)
        self.cur1.execute(sql)
        dic = {data[0]: data[1] for data in self.cur1.fetchall()}

        re_dic = {}
        sql = """select * from `{}` where change_status = '成交' and spider_time = '{}'""".format(self.four_table, now_time)
        print(sql)
        self.cur1.execute(sql)
        datas = self.cur1.fetchall()
        for data in datas:
            build = data[2]
            spider_time = wang_tool.get_date_y_m_d(str(data[4]))
            bu_area = dic[build]
            if spider_time not in re_dic:
                re_dic[spider_time] = [0, 0, 0]
            re_dic[spider_time][0] += 1
            try:
                re_dic[spider_time][1] += float(bu_area)
            except:
                pass
        for spi_time, value in re_dic.items():
            sale_num, sale_Area, sale_money = value
            param = [self.city, sale_num, sale_Area, sale_money, spi_time]
            temp_str = ','.join(map(lambda x: '%s', param))
            sql = """insert into {}(city, sale_num, sale_area, sale_money, spider_time) values({})""".format(
                self.five_table, temp_str)
            self.cur1.execute(sql, param)
            self.conn_1.commit()

    def anlyse(self):
        # self.creat_table()
        # self.alter_table()
        # self.insertProj()
        # self.insetDataTable()
        # self.insetDataTable_one()
        self.create_four()  # 生成第四张表
        self.create_five()  # 生成第五张表


# if __name__ == '__main__':
#     from 网签处理框架.data.StatusDict import *
#     test = analyseThreeTable(beijing)
#     test.anlyse()
