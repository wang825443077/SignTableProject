# coding:utf-8

import unittest

from SignTableProject.analyse.sign.SignTablePackage import analyseTable
from SignTableProject.data.StatusDict import *


class beijingTestCase(unittest.TestCase):

    def setUp(self):
        print("setUp")

    def tearDown(self):
        print("tearDown")

    def test_beijing(self):
        t1 = analyseTable(beijing)
        t1.anlyse()

