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

    def insertProj(self):
        """
        插入项目表
        """
        if isinstance(self.df_proj_data, pd.DataFrame):
            self.insertProjTable()
            sort_columns = list(self.columnIndex.keys()) + self.orig_proj_columns_insert
            self.df_proj_data = self.df_proj_data[sort_columns]
            self.df_proj_data.replace('', '-', inplace=True)
            self.df_proj_data = self.origDataReplace(self.df_proj_data)
            self.df_proj_data['appe'] = 1
            csv_path = os.path.join(self.result_folder_path, '%s_%s.csv' % (self.origTable_pro, self.today))
            text_path = os.path.join(self.result_folder_path, '%s_%s.txt' % (self.origTable_pro, self.today))
            self.df_proj_data.to_csv(csv_path, index=False, encoding='utf_8')
            self.df_proj_data.to_csv(text_path, index=False, encoding='utf_8')
            # self.df_proj_data.to_csv(csv_path, index=False, encoding='gbk')

            self.loadCsvData(csv_path, sort_columns, self.projTableName)
            print('proj表插入成功')
        else:
            print('proj表无更新数据')


class guangzhouTestCase(unittest.TestCase):

    def setUp(self):
        print("setUp")

    def tearDown(self):
        print("tearDown")

    def test_guagnzhou(self):
        t1 = Guangzhou(guangzhou)
        t1.anlyse()

