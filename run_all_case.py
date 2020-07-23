# coding:utf-8
import unittest
import HTMLTestReportCN as HTMLTestReport
import os
from case.sign.test_beijing import beijingTestCase

# 用例路径
case_path = os.path.join(os.getcwd(), "case")
# 报告存放路径
report_path = os.path.join(os.getcwd(), "report")


def all_case():
    # 运行所有test开头的文件
    discover = unittest.defaultTestLoader.discover(case_path,
                                                    pattern="test*.py",
                                                    top_level_dir=None)
    print(discover)
    return discover


if __name__ == "__main__":
    report_html_path = os.path.join(report_path, 'result.html')

    if not os.path.exists(report_path):
        os.makedirs(report_path)

    fp = open(report_html_path, 'w', encoding='utf-8')
    # 生成报告的Title,描述
    runner = HTMLTestReport.HTMLTestRunner(stream=fp, title='Python Test Report', description='This  is Python  Report')

    suite = unittest.TestSuite()
    suite.addTest(beijingTestCase("test_beijing"))
    runner.run(suite)

    # runner.run(all_case())