# !/usr/bin/env Python
# coding=utf-8
import os
import unittest
import pandas as pd

from analyse.sign.SignTablePackage import analyseTable
from data.StatusDict import *


class Guangzhou(analyseTable):
    def __init__(self, guangzhou):
        super().__init__(guangzhou)

    def getAddDate(self, tableName, origTableName, spider_time,orig_spider_time):
        """
        获取数据库新增日期列表
        :return:
        """
        sql_table_date = """select distinct DATE_FORMAT({spider_time},'%Y-%m-%d')  from {Table}"""
        print(sql_table_date.format(spider_time=spider_time, Table=tableName))
        df_table_date = pd.read_sql(sql_table_date.format(spider_time=spider_time, Table=tableName), con=self.conn_1)
        df_orig_table_date = pd.read_sql(sql_table_date.format(spider_time=orig_spider_time, Table=origTableName), con=self.conn_1)

        table_date = {te for te in df_table_date.iloc[:, 0].tolist() if str(te) != 'None'}
        orig_table_date = {te for te in df_orig_table_date.iloc[:, 0].tolist() if str(te) != 'None'}
        add_date = list(table_date - orig_table_date)
        return add_date

    def rewrite_read(self):
        te_sql_sta = """select * from {table}""".format(table=self.origTable_sta)
        te_sta = pd.read_sql(te_sql_sta, con=self.conn_2)
        te_sta.drop_duplicates(subset=['unitID'], inplace=True)
        print(len(te_sta))
        self.df_room = self.df_room.merge(te_sta[['unitID', 'building']], on=['unitID'], how='left')
        print(len(self.df_room))
        self.df_room['floor'] = ''
        print('原始表读取完成')

    def insertProj(self):
        """
        插入项目表
        """
        if isinstance(self.df_proj_data, pd.DataFrame):
            sort_columns = ['cleantime'] + list(self.data['projcolumns_dict'].keys())
            new_columns = ['cleantime'] + list(self.data['projcolumns_dict'].values())
            self.df_proj_data['cleantime'] = self.now_time
            print(list(self.df_proj_data.columns))
            self.df_proj_data = self.df_proj_data[sort_columns]
            self.df_proj_data['appe'] = 1
            self.df_proj_data.replace('', '-', inplace=True)
            self.df_proj_data = self.origDataReplace(self.df_proj_data)
            csv_path = os.path.join(self.result_folder_path, '%s_%s.csv' % (self.origTable_pro, self.today))
            self.df_proj_data.to_csv(csv_path, index=False, encoding='utf_8')

            self.loadCsvData(csv_path, new_columns, self.projTableName)
            print('proj表插入成功')
        else:
            print('proj表无更新数据')

    def save_five(self):
        """
        重写第五张表
        :return:
        """
        sql = """SELECT one.proname, two.expected_area, four.* FROM `{table4}` as four
                            left JOIN `{table2}` as two on two.id = four.buildID
                            left JOIN `{table1}` as one on one.id = two.projID 
                            where DATE_FORMAT(four.{spider_time},'%Y-%m-%d') in {dateList} """ \
            .format(table1=self.projTableName, table2=self.dataTable, table4=self.four_table,
                    spider_time=self.data['FourTable_spider_time_field'], dateList=self.five_dateList)

        self.df_five = pd.read_sql(sql, con=self.conn_1)
        self.df_five['expected_area'] = self.df_five['expected_area'].map(lambda x: float(x))
        sum_df = self.df_five.groupby(['proname', 'deal_status', 'spider_time'], as_index=False)['expected_area'].sum()
        len_df = self.df_five.groupby(['proname', 'deal_status', 'spider_time'], as_index=False).count()
        df_new = sum_df.merge(len_df[['proname', 'deal_status', 'spider_time', 'id']], on=['proname', 'deal_status', 'spider_time'], how='left')
        for value in df_new.values:
            param = [str(te) for te in value] + [self.now_time]
            temp_str = ','.join(map(lambda x: '%s', param))
            print(param)
            sql = """insert into `{table5}`(proname,deal_status,spidertime,dealarea,dealnum,cleantime) values({temp_str})""".format(table5=self.five_table, temp_str=temp_str)
            self.cur1.execute(sql, param)
            self.conn_1.commit()


class guangzhouTestCase(unittest.TestCase):

    def setUp(self):
        print("setUp")

    def tearDown(self):
        print("tearDown")

    def test_guagnzhou(self):
        t1 = Guangzhou(guangzhou)
        t1.anlyse()