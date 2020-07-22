# coding:utf-8

import unittest

from 网签处理.analyse.sign.SignTablePackage import analyseTable
from 网签处理.data.StatusDict import *


class beijingTestCase(unittest.TestCase):

    def setUp(self):
        print("setUp")

    def tearDown(self):
        print("tearDown")

    def test_beijing(self):
        t1 = analyseTable(beijing)
        t1.anlyse()

