# -*- coding:utf-8
# @Author   :王悦磊
# @Time     :2020/7/25 10:16
# @Software :Pycharm
# @File     :test_shenzhen.py
import os
import unittest
import pandas as pd

from analyse.sign.SignTablePackage import analyseTable
from data.StatusDict import *


class ShenZhen(analyseTable):
    def __init__(self, shenzhen):
        super().__init__(shenzhen)


class guangzhouTestCase(unittest.TestCase):

    def setUp(self):
        print("setUp")

    def tearDown(self):
        print("tearDown")

    def test_guagnzhou(self):
        t1 = ShenZhen(shenzhen)
        t1.anlyse()


def main():
    pass


if __name__ == '__main__':
    import time

    start = time.time()
    main()
    print("程序用时:{}".format(time.time() - start))



