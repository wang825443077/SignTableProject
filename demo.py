# !/usr/bin/env Python
# coding=utf-8
import os
# import unittest
import pandas as pd

from analyse.sign.SignTablePackage import analyseTable
from data.StatusDict import *


class Guangzhou(analyseTable):
    def __init__(self, guangzhou):
        super().__init__(guangzhou)

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


# class guangzhouTestCase(unittest.TestCase):
#
#     def setUp(self):
#         print("setUp")
#
#     def tearDown(self):
#         print("tearDown")
#
#     def test_guagnzhou(self):
#         t1 = Guangzhou(guangzhou)
#         t1.anlyse()


if __name__ == '__main__':
    t1 = Guangzhou(guangzhou)
    t1.anlyse()