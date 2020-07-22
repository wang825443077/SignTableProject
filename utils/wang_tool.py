# -*- coding:utf-8
# @Author   :王悦磊
# @Time     :2019/11/11 9:38
# @Software :Pycharm
# @File     :wang_tool.py
import difflib
import pickle
import re
import time
from math import radians, sin, asin, sqrt, cos
import datetime
import pymysql


# 获取文件路径
def get_path(city):
    import os
    # 获取当前文件路径
    current_path = os.path.abspath(__file__)
    # 获取当前文件的父目录
    father_path = os.path.abspath(os.path.dirname(current_path) + os.path.sep + ".")
    # config.ini文件路径,获取当前目录的父目录的父目录与congig.ini拼接
    config_file_path = os.path.join(os.path.abspath(os.path.dirname(current_path) + os.path.sep + "."), '{}.xlsx'.format(city))
    return config_file_path


def judge_null(data):
    if str(data) == 'None' or data == '' or data == '-' or str(data) == 'nan':
        return None
    else:
        return data


# 计算两个年限间相差的天数
def get_date_y_m_d(res):
    res = str(res)
    local_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())[2: 4]
    local_time_year = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())[0: 4]
    if res is None or res == '':
        return None
    res_list = re.findall(r'(\d+)', res)
    if len(res_list) == 0:
        # print('没有数字', res)
        return None
    if len(res_list) == 1:
        temp_ = res_list[0]
        try:
            if temp_.startswith('20'):
                param = [temp_[:4]] + [temp_[4:6]] + [temp_[6:8]]
            else:
                param = [temp_[:2]] + [temp_[2:4]] + [temp_[4:6]]
            res_list = param
        except:
            return None
    res_ = ''
    new_list = []
    block = False
    if len(res_list[0]) == 2 and int(res_list[0]) <= int(local_time):
        res_list[0] = '20' + str(res_list[0])
    for i in res_list:
        if i.startswith('20') and len(i) == 4:
            block = True
        if block:
            new_list.append(i)
    for i in new_list[:3]:
        if len(i) == 1:
            i = '0' + i
        res_ += i
    reslut = res_.split(' ')[0]
    try:
        if 2 < int(reslut[0]):
            return None
    except:
        return None
    try:
        day = int(reslut[6:8])
        if day == 0:
            new_day = str(0) + str(1)
        else:
            if len(str(day)) == 1:
                new_day = str(0) + str(day)
            else:
                new_day = day
    except:
        # print('错误', res)
        new_day = '01'

    try:
        month = reslut[4:6]
        if int(month) > 12 or int(month) == 0:
            month = '01'
    except:
        month = '01'
    month_31 = ['01', '03', '05', '07', '08', '10', '12']
    if str(month) == '02':
        if int(new_day) > 28:
            if int(local_time_year) % 4:
                new_day = 28
            else:
                new_day = 29
    elif month in month_31:
        if int(new_day) > 31:
            new_day = 31
    else:
        if int(new_day) > 30:
            new_day = 30
    return reslut[:4] + '-' + month + '-' + str(new_day)


# print(get_date_y_m_d('20200629'))


def dayofyear(date_1, date_2):
    """
    :param date_1: 第一个日期
    :param date_2: 第二个日期
    :return:
    """
    date_1 = get_date_y_m_d(date_1)
    date_2 = get_date_y_m_d(date_2)
    if date_1 is None or date_2 is None or date_2 == '00:00:00' or date_2 == '99990909':
        return 0
    res_list_1 = re.findall(r'(\d+)', date_1)
    res_list_2 = re.findall(r'(\d+)', date_2)
    try:
        year_1, month_1, day_1 = res_list_1[:3]
        year_2, month_2, day_2 = res_list_2[:3]
    except:
        return 0
    date1 = datetime.date(year=int(year_1), month=int(month_1), day=int(day_1))
    date2 = datetime.date(year=int(year_2), month=int(month_2), day=int(day_2))
    return (date2 - date1).days+1


# print(dayofyear('20170922', '20171001'))


# 判断相似度的方法，用到了difflib库
def get_equal_rate_1(str1, str2):
    if judge_null(str1) is None or judge_null(str2) is None:
        return 0
    return difflib.SequenceMatcher(None, str1, str2).quick_ratio()


# 中英文转化
def chinese_to_english(t):
    if t is None:
        return None
    table = {ord(f): ord(t) for f, t in zip(u'，。！？【】（）％＃＠＆１２３４５６７８９０ＸＧＡ－：', u',.!?[]()%#@&1234567890XGA-:')}
    # t = u'中国，中文，标点符号！你好？１２３４５＠＃【】+=-（）'
    try:
        t2 = t.translate(table)
    except:
        t2 = ''
    t2 = t2.upper()
    t2 = t2.strip()
    return t2


def hanzi_to_number(t):
    if str(t) == 'None' or t == '':
        return None
    dict_ = {
        '0': 0, '一': 1, '二': 2, '三': 3, '四': 4, '五': 5, '六': 6, '七': 7, '八': 8, '九': 9, '十': 10
    }
    param_list = re.findall(r"[^年月日]+", t)
    result_list = []
    for te in param_list:
        if '十' in te:
            num = None
            for i in te:
                number = dict_.get(str(i), None)
                if i != '十':
                    if num is None:
                        num = number
                    else:
                        if number is not None:
                            num += number
                else:
                    if num:
                        num *= 10
                    else:
                        num = 10
            if num:
                result_list.append(num)
        else:
            for i in te:
                number = dict_.get(str(i), None)
                if number is not None:
                    if len(te) == 1:
                        number = '0' + str(number)
                    result_list.append(number)

    if len(result_list) == 0:
        return None
    else:
        return ''.join(map(lambda x: str(x), result_list))


def add_mess(res_list1, res_list):
    for res in res_list1:
        if len(res) < 4:
            continue
        else:
            if res not in res_list and bool(re.search(r'[a-zA-Z\-]', res)):
                res_list.append(res)
    return res_list


# 土地便编码
def get_tudi_bianma(position):
    """沙坪坝区西永组团Ja分区Ja07-2/02、Ja11-1/02"""
    if position is None or position == 'None' or position == '' or position == 'NoneNone':
        return ['我是空的']
    position_list = list(position)
    for k in range(len(position_list)):
        if position_list[k] == '，':
            position_list[k] = '、'
    position = ''.join(position_list)
    position = position + '、'
    res_list = []
    res_list1 = re.findall(r"([a-zA-Z]+\d+.*?)[、,\u4e00-\u9fa5（()）]", position)
    res_list_2 = re.findall(r"([a-zA-Z]+.?\d+.*?)[、,\u4e00-\u9fa5（()）]", position)
    res_list_3 = re.findall(r"([\da-zA-Z]+.?\d+.*?)[、,\u4e00-\u9fa5（()）]", position)
    # res_list_4 = re.findall(r'([\da-zA-z].*?)[、,\u4e00-\u9fa5（()]', position)
    res_list = add_mess(res_list1, res_list)
    res_list = add_mess(res_list_2, res_list)
    res_list = add_mess(res_list_3, res_list)
    # res_list = add_mess(res_list_4, res_list)
    res_list = list(set(res_list))
    # print(res_list)
    new_list = []
    for res in res_list:
        for res_1 in res_list:
            if res == res_1:
                continue
            if res in res_1:
                if res not in new_list:
                    new_list.append(res)
                    continue
            if res_1 in res:
                if res_1 not in new_list:
                    new_list.append(res_1)
                    continue
    # print(res_list)
    # print(new_list)
    for mess in new_list:
        res_list.remove(mess)
    return res_list


# print(get_tudi_bianma('TD2018(NH)WG0032佛山市南海区桂城街道三山科技创意产业园NH-A-12-03-02-27号地块'))


# 已知经纬度求坐标
def get_diatance(lon1, lat1, lon2, lat2):
    # 将十进制数转化为弧度
    try:
        lon1, lat1, lon2, lat2 = map(radians, [float(lon1), float(lat1), float(lon2), float(lat2)])
    except:
        return 99999999

    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
    c = 2 * asin(sqrt(a))
    r = 6371.137  # 地球平均半径，单位为公里
    return float('%.2f' % (c * r))      # （单位为千米）


def get_area(data):
    """
    提取武汉的特殊面积
    :return:
    """
    end_list = ['万平方米', '平方米', '㎡','m²', 'm2', '平方', '公顷', '（平方米）', '(平方米)', '(㎡)', '（㎡）', '(平方)', '（平方）', '(m2)', '（m2）']
    start_list = ['面积', '面积:', '面积：']
    # print(data)
    try:
        a = re.findall(r'[.\d]+', data)
    except:
        return None
    new_list = []
    # print(a)
    for te in a:
        te_list = data.split(te)
        # print(te_list)
        te_start = te_list[0].replace(' ', '')
        if len(te_list) > 1:
            te_end = te_list[1].replace(' ', '')
            for end in end_list:
                block = False
                if te_end.startswith(end):
                    if end == '公顷' or end == '万平方米':
                        try:
                            new_list.append(str(float(te) * 10000))
                        except:
                            new_list.append(te)
                    else:
                        new_list.append(te)
                    break
                else:
                    for start in start_list:
                        if te_start.endswith(start) or (te_start == '' and te_end == ''):
                            new_list.append(te)
                            block = True
                            break
                if block:
                    break
    if len(new_list) == 0:
        return None
    max = 0
    for count in new_list:
        try:
            if float(count) > max:
                max = float(count)
        except:
            pass
    if max == 0 or max > 9999999:
        max = None
    return max

# print(get_area('51380.37（平方米）'))


def ex_price(data):
    """
    针对房天下提取金额
    :return:
    """
    price_list = re.findall(r'(\d+?)元/平方米', data)
    if len(price_list) == 0:
        price_list = re.findall(r'(\d+?)元/㎡', data)
        if len(price_list) == 0:
            return None
    try:
        if len(price_list) > 1:
            result = max(price_list)
            return float(result)
        else:
            result = price_list[0]
            return float(result)
    except:
        return None


def get_tiqu_shuzi(data):
    if str(data) is None or data == '':
        return None
    data = str(data)
    try:
        a = re.findall(r'(\d.*\d)', data)
    except:
        a = None

    try:
        max = float(a[0])
    except:
        try:
            a = re.findall(r'[.\d]+', data)
        except:
            return None
    if len(a) == 0:
        return None
    max = 0
    for count in a:
        try:
            if float(count) > max:
                max = float(count)
        except:
            pass
    if max == 0:
        max = None
    return max


# print(get_tiqu_shuzi('506'))


# 拆分时间
def date(res):
    local_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())[2: 4]
    local_time_year = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())[0: 4]
    if res is None or res == '':
        return None
    res_list = re.findall(r'(\d+)', res)
    if len(res_list) == 0:
        # print('没有数字', res)
        return None
    if len(res_list) == 1:
        temp_ = res_list[0]
        try:
            if temp_.startswith('20'):
                param = [temp_[:4]] + [temp_[4:6]] + [temp_[6:8]]
            else:
                param = [temp_[:2]] + [temp_[2:4]] + [temp_[4:6]]
            res_list = param
        except:
            return None
    res_ = ''
    new_list = []
    block = False
    if len(res_list[0]) == 2 and int(res_list[0]) <= int(local_time):
        res_list[0] = '20' + str(res_list[0])
    for i in res_list:
        if i.startswith('20') and len(i) == 4:
            block = True
        if block:
            new_list.append(i)
    for i in new_list[:3]:
        if len(i) == 1:
            i = '0' + i
        res_ += i
    reslut = res_.split(' ')[0]
    # print('****', res_list)
    try:
        if 2 < int(reslut[0]):
            # print('格式错误', res)
            return None
    except:
        # print('长度太小', res)
        return None
    try:
        day = int(reslut[6:8])
        if day == 0:
            new_day = str(0) + str(1)
        else:
            if len(str(day)) == 1:
                new_day = str(0) + str(day)
            else:
                new_day = day
    except:
        new_day = '01'

    try:
        month = reslut[4:6]
        if int(month) > 12 or int(month) == 0:
            month = '01'
    except:
        month = '01'
    month_31 = ['01', '03', '05', '07', '08', '10', '12']
    if str(month) == '02':
        if int(new_day) > 28:
            # if int(local_time_year) % 4:
            new_day = 28
            # else:
            #     new_day = 29
    elif month in month_31:
        if int(new_day) > 31:
            new_day = 31
    else:
        if int(new_day) > 30:
            new_day = 30
    return reslut[:4] + '-' + month + '-' + str(new_day)

# print(get_date_y_m_d('20181120'))
# b = date('')
# print(a > b)
# print(date('2014-09-23至2016-08-27'))
# print(date('20200101'))
# print(date('190502'))


# 词性标注，nr为人名, 加判断少数民族姓名
def getFirstName(messageContent):
    if len(messageContent) <= 4:
        return True
    # if '·' in messageContent:
    #     return True
    error_list = ['职工自然人', '发起人股东', '有限合伙人', '(发起人)', '(有限合伙)']
    for error in error_list:
        if error in messageContent:
            if len(messageContent.split(error)) <= 4:
                return True
    if '等' in messageContent:
        for mess in messageContent.split('等'):
            if len(mess) <= 4:
                return True
    # words = pseg.cut(messageContent)
    # for word, flag in words:
    #     if flag == 'nr' and len(word) > 1:  # 单字姓名去掉
    #         return word
    return False


# 拆分时间
def date_number(res):
    res = str(res)
    # res_date = res[5]
    if res is None:
        return 99990909
    if res.isdigit():
        return int(res)
    res_list = re.findall(r'(\d+)', res)
    if len(res_list) < 3:
        return 99990909
    res_ = ''
    for i in res_list[:3]:
        if len(i) == 1:
            i = '0' + i
        res_ += i
    reslut = res_.split(' ')[0]
    if int(reslut[0]) > 2:
        return 99990909
    return int(reslut)

# print(date_number(None))


def zhuanhua_percent(percent):
    if '%' not in percent:
        res = str(round(float(percent) * 100, 2)) + '%'
    else:
        percent = str(percent).split('%')[0]
        res = round(float(percent)/100, 2)
    return res


# print(zhuanhua_percent('0.593'))

# print(date('2014-09-04 00:00:00.000'))
# print(date('2014/5/26'))

# 从不规则日期中提取规则日期
def get_date(res):
    # print(res)
    # res_date = res[5]
    if res is None or res == '':
        return None
    res_list = re.findall(r'(\d+)', res)
    if len(res_list) < 3:
        return None
    res_ = ''
    new_list = []
    block = False
    for i in res_list:
        if i.startswith('20') and len(i) == 4:
            block = True
        if block:
            new_list.append(i)

    for i in new_list[:3]:
        if len(i) == 1:
            i = '0' + i
        res_ += i
    reslut = res_.split(' ')[0]
    if int(reslut[0]) > 2:
        return None
    return reslut[:4] + '-' + reslut[4:6] + '-' + reslut[6:]


# print(get_province('成都市朗融房地产开发有限公司'))


# 判断两个公司是否有关系
def get_ciceng(dic_500w, comp, list_, n, dic_stop):
    n += 1
    if n == 4:
        return list_
    if comp in dic_500w:
        for i in dic_500w[comp]:
            if i in dic_stop:
                list_.append(i)
                continue
            if i in list_:
                continue
            else:
                list_.append(i)
                list_ = get_ciceng(dic_500w, i, list_, n, dic_stop)
        return list_
    else:
        return list_


def old_name_list(comp_list, dic_old_name, comp_old_list):
    if comp_list is None:
        return comp_old_list
    if len(comp_list) == 0:
        return comp_old_list
    for comp in comp_list:
        if comp in dic_old_name:
            old_list = dic_old_name[comp]
            comp_old_list += old_list
    return comp_old_list


# 判断两个公司列表之间是否存在包含和被包含关系
def panduan_comp_list(comp_list, mess_comp_list):
    """
    判断两个公司列表之间是否存在包含和被包含关系
    :param comp_list:
    :param mess_comp_list:
    :return:
    """
    for comp in comp_list:
        for mess_comp in mess_comp_list:
            if comp in mess_comp or mess_comp in comp:
                return True
    return False


# 通过上下级关系判断两个公司列表的关系
def guanxi_comp_list(comp_list, fu_dict, zi_dic, comp_list_fu, comp_list_zi, dic_stop):
    for compNm in comp_list:
        comp_list_fu += get_ciceng(fu_dict, compNm, [compNm], 0, dic_stop)
        comp_list_zi += get_ciceng(zi_dic, compNm, [compNm], 0, dic_stop)
    return [comp_list_fu, comp_list_zi]


def new_pipei(comp, mess_comp, fu_dict, zi_dic, dic_old_name, dic_stop):
    comp_list = split_comp(comp)
    mess_comp_list = split_comp(mess_comp)
    comp_old_list = old_name_list(comp_list, dic_old_name, [])
    mess_comp_old_list = old_name_list(mess_comp_list, dic_old_name, [])
    if comp and comp != '' and comp != 'None' and mess_comp != '' and mess_comp and mess_comp != 'None':
        # 直接判断公司列表或公司和曾用名列表间是否有关系
        if panduan_comp_list(comp_list, mess_comp_list) or panduan_comp_list(comp_list, mess_comp_old_list) or \
                panduan_comp_list(mess_comp_list, comp_old_list) or panduan_comp_list(comp_old_list, mess_comp_old_list):
            return True
        # 判断上下级关系之间是否有关系
        else:
            comp_list_fu, comp_list_zi = guanxi_comp_list(comp_list, fu_dict, zi_dic, [], [], dic_stop)
            old_comp_list_zi, old_comp_list_fu = guanxi_comp_list(comp_old_list, fu_dict, zi_dic, [], [], dic_stop)
            comp_list_all = comp_list_zi + comp_list_fu + old_comp_list_zi + old_comp_list_fu
            mess_comp_list_fu, mess_comp_list_zi = guanxi_comp_list(mess_comp_list, fu_dict, zi_dic, [], [], dic_stop)
            mess_old_comp_list_zi, mess_old_comp_list_fu = guanxi_comp_list(mess_comp_old_list, fu_dict, zi_dic, [], [], dic_stop)
            mess_comp_list_all = mess_comp_list_fu + mess_comp_list_zi + mess_old_comp_list_zi + mess_old_comp_list_fu
            for comp_new in comp_list_all:
                for mess_comp_new in mess_comp_list_all:
                    if (comp_new in mess_comp_new or mess_comp_new in comp_new) and (len(comp_new) > 3 and len(mess_comp_new) > 3):
                        return True
            else:
                return False
    else:
        return False


# 判断两个公司是否有关系
def pipei_(comp_, tudi_old_name, zheng_old_name, mess_comp, fu_dict, zi_dic, dic_old_name):
    # 深圳市金众房地产有限公司深圳市金之林投资发展有限公司深圳市五建开发公司深圳市五建房地产开发公司深圳市华腾房地产公司深圳市金众房地产公司深圳市华业房地产公司深圳市一体投资控股集团有限公司
    comp_list = split_comp(comp_)
    mess_comp_list = split_comp(mess_comp)
    comp_old_list = []
    mess_comp_old_list = []
    for comp in comp_list:
        if comp in dic_old_name:
            old_list = dic_old_name[comp]
            comp_old_list += old_list
    for mess_comp in mess_comp_list:
        if mess_comp in dic_old_name:
            mess_old_list = dic_old_name[mess_comp]
            mess_comp_old_list += mess_old_list
    tudi_old_name_mess = ','.join(map(lambda x: x, tudi_old_name))
    zheng_old_name_mess = ','.join(map(lambda x: x, zheng_old_name))
    if comp_ and comp_ != '' and mess_comp != '' or mess_comp:
        if comp_ == mess_comp or comp_ == zheng_old_name_mess or tudi_old_name_mess == mess_comp or tudi_old_name_mess == zheng_old_name_mess or\
                comp_ in mess_comp or mess_comp in comp_ or zheng_old_name_mess in comp_ or comp_ in zheng_old_name_mess or comp_list == mess_comp_list:
            return True
        else:
            comp_list_fu = []
            comp_list_zi = []
            for comp in comp_list:
                comp_list_fu += get_ciceng(fu_dict, comp, [], 0)
                comp_list_zi += get_ciceng(zi_dic, comp, [], 0)
            old_comp_list_zi = []
            old_comp_list_fu = []
            for comp in tudi_old_name:
                old_comp_list_zi += get_ciceng(zi_dic, comp, [], 0)
                old_comp_list_fu += get_ciceng(fu_dict, comp, [], 0)
            comp_list = comp_list_zi + comp_list_fu + old_comp_list_zi + old_comp_list_fu
            for comp in comp_list:
                if comp in mess_comp or mess_comp in comp:
                    return True
            else:
                mess_comp_list_fu = get_ciceng(fu_dict, mess_comp, [], 0)
                mess_comp_list_zi = get_ciceng(zi_dic, mess_comp, [], 0)
                old_mess_comp_list_zi = []
                old_mess_comp_list_fu = []
                for zheng_comp in zheng_old_name:
                    old_mess_comp_list_zi += get_ciceng(zi_dic, zheng_comp, [], 0)
                    old_mess_comp_list_fu += get_ciceng(zi_dic, zheng_comp, [], 0)
                mess_comp_list = mess_comp_list_fu + mess_comp_list_zi + old_mess_comp_list_zi + old_mess_comp_list_fu
                print(mess_comp_list)
                for comp in mess_comp_list:
                    if comp_ in comp or comp in comp_:
                        return True
                else:
                    return False
    else:
        return False


def extra_comp(comp):
    if comp == 'None' or comp == '' or comp is None:
        return None
    comp = comp + ','
    result = re.findall(r'(.*?)[\[、，/\\；,&与：]', comp)
    return result


def judge_none(mess):
    if mess is None or mess == '' or mess == 'None':
        return True
    else:
        return False

# def extra_comp(comp):
#     if comp == 'None' or comp == '' or comp is None:
#         return None
#     # print(comp)
#     if '公司' in comp:
#         comp = comp.split('公司')[0] + '公司'
#     if '成立于' in comp:
#         comp = comp.split('成立于')[0]
#     if '始建于' in comp:
#         comp = comp.split('始建于')[0]
#     if '所属地区' in comp:
#         comp = comp.split('所属地区')[0]
#     if '担保方' in comp:
#         comp = comp.split('担保方')[1]
#     comp = comp + ','
#     result = re.findall(r'(.*?)[\[、，/\\；,&与：为是\.:]', comp)
#     return result


def removal_comp(comp):
    """
    过滤重复字段
    :param comp:
    :return:
    """
    mess_list = ''
    for i in range(len(comp)-1):
        mess = comp[i]
        for k in range(i+1, len(comp)-1):
            if comp.count(mess + comp[k]) > 1:
                mess += comp[k]
            else:
                break
        if len(mess) > len(mess_list):
            mess_list = mess
    if len(mess_list) <= 5 and '中置鑫通' != mess_list:
        return comp
    else:
        res_comp = mess_list + ''.join(map(lambda x: str(x), comp.split(mess_list)))
        return res_comp


def remove_top(comp):
    """
    判断公司中是否有百强  top
    :return:
    """
    res_list_1 = re.findall(r".*?[\d]+年", comp)
    if len(res_list_1) != 0:
        return None
    res_list_2 = re.findall(r".*?([百\d]+强)", comp)
    if len(res_list_2) != 0:
        return None
    res_list_3 = re.findall(r".*?(TOP[\d]+)", comp)
    if len(res_list_3) != 0:
        return None
    res_list_4 = re.findall(r"[A-Za-z\d-]+", comp)
    if len(res_list_4) != 0:
        if res_list_4[0] == comp:
            return None
    return True


def recursion_comp(comp, re_list):
    """
    递归过滤以不规则信息开头
    :param comp:
    :param re_list:
    :return:
    """
    for res in re_list:
        if comp.startswith(res):
            comp = res.join(map(lambda x: str(x), comp.split(res)[1:]))
            comp = recursion_comp(comp, re_list)
    return comp


# print(removal_comp('广州华粤一号投资合伙企业(有限合伙)广州华粤一号投资合伙企业(有限合伙)'))


def split_comp(comp):
    # 不规则字段开头的列表
    res_list = ['[', '、', '，', '/', '\\', ',', '&', '与', '(', ')', '()', '--', '-', '.', '开  发  商',
                '发起人', '和', '及', '融资人股东', '融资方:', '旗下', '由', '债务人', '交易对手——', '股权出质人:', '非上市股份',
                '保证人——', '收购', '融资人', '质押人——', '借款人', '暨抵押人', ':', '融资方', '–', '股东', '项目公司', '——', '前称“', '即', ';', '；']
    if comp == 'None' or comp == '' or comp is None:
        return None
    comp = recursion_comp(comp, res_list)
    not_split_list = ['分公司', '全资子公司', '子公司']
    block_comp = True
    if comp.count('公司') > 1:
        for not_split in not_split_list:
            if not_split in comp:
                if not_split == '全资子公司' or not_split == '子公司':
                    comp_list = comp.split('公司')
                    new_list = list(map(lambda x: x + '公司', comp_list[:-1]))
                    comp = new_list[0]
                block_comp = False
                break
        if block_comp:
            comp_list = comp.split('公司')
            new_list = list(map(lambda x: x+'公司', comp_list[:-1]))
            comp = ','.join(map(lambda x: x, new_list))
            # print(comp)
    temp_list = re.findall(r'[(](.*?)[)]', comp)
    # print(temp_list)
    if len(temp_list) == 0:
        temp_list = comp.split(',')
    result_list = []
    block = False
    # print(temp_list)
    for temp in temp_list:
        del_list = ['下称“', '以下简称“', '以“', '简称“', '股权投资', '入驻', '拟上市', '《', '全资持', '以下简称', '项目公司']
        block_del = False
        for del_mess in del_list:
            if temp.startswith(del_mess):
                block_del = True
                break
        if block_del:
            continue
        if len(temp) >= 5:
            block = True
            if '(' in temp and ')' not in temp:
                temp = temp + ')'
            list1 = extra_comp(temp)
            result_list.extend(list1)
            temp_list2 = comp.split('({})'.format(temp))
            for temp_ in temp_list2:
                if len(temp_) >= 5:
                    list2 = extra_comp(temp_)
                    result_list.extend(list2)
    if not block:
        list_ = extra_comp(comp)
        if list_ is not None:
            result_list.extend(list_)
    new_list = []
    # print(result_list)
    comp_mark = ['公司', '企业', '有限合伙', '控股']
    # print(result_list)
    for comp in result_list:
        del_list = ['下称“', '以下简称“', '以“', '简称“', '股权投资', '入驻', '拟上市', '全资持']
        if len(comp) < 4:
            continue
        block_del = False
        for del_mess in del_list:
            if comp.startswith(del_mess):
                block_del = True
                break
        if block_del:
            continue
        comp = recursion_comp(comp, res_list)
        # for res in res_list:
        #     if comp.startswith(res):
        #         comp = res.join(map(lambda x: str(x), comp.split(res)[1:]))
        if len(comp) > 3:
            try:
                comp_1 = re.findall(r'([\u4e00-\u9fa5()]+)', comp)[0]
                for mark in comp_mark:
                    if mark in comp or mark in comp_1:
                        break
                else:
                    continue
            except:
                try:
                    float(comp)
                    continue
                except:
                    pass
                pass
            if comp.endswith('('):
                comp = comp.split('(')[0]
            elif comp.startswith(')'):
                comp = ')'.join(map(lambda x: str(x), comp.split(')')[1:]))
            if '%' in comp:
                continue
            # 过滤掉重复的公司名
            # print(comp)
            comp = removal_comp(comp)
            if not remove_top(comp):
                continue
            if comp not in new_list:
                new_list.append(comp)
    return new_list

# print(split_comp('青阳县住房和城乡建设委员会'))


def get_first_comp(comp, fu_comp, dic_son_name, stop_to_last, stop_dic, time):
    for to_last in stop_to_last:
        if to_last in fu_comp or len(fu_comp) < 4:
            return comp
    for stop_str in stop_dic:
        if stop_str in fu_comp:
            return fu_comp
    time += 1
    if time == 20:
        return fu_comp
    if comp in dic_son_name:
        try:
            comp = get_first_comp(comp, list(dic_son_name[comp].keys())[0], dic_son_name, stop_to_last, stop_dic, time)
        except:
            print(dic_son_name[comp])
            return comp
        return comp
    return comp

# # 2019.12.24 自下而上股权关系
# def recursion(comp, zi_comp, guquan, dic_old_name, dic_son_name, times, dic_temp, stop_dic, stop_to_last, dic_res):
#     """
#
#     :param comp: 父公司
#     :param zi_comp: 子公司
#     :param guquan: 乘出来的股权
#     :param dic_old_name: 曾用名
#     :param dic_son_name: 子对父关系的字典
#     :param times: 次数
#     :param dic_temp: 每一层关系的字典
#     :param stop_dic: 停用词字典
#     :param stop_to_last: 停用词找下级
#     :param dic_res: 返回的字典
#     :return:
#     """
#     for to_last in stop_to_last:
#         if '等' in comp and '人' in comp:
#             comp = zi_comp
#             if comp not in dic_res:
#                 dic_res.update({comp: guquan})
#             else:
#                 dic_res[comp] += guquan
#             return [dic_temp, dic_res]
#         if to_last in comp or len(comp) < 4:
#             comp = zi_comp
#             if comp not in dic_res:
#                 dic_res.update({comp: guquan})
#             else:
#                 dic_res[comp] += guquan
#             return [dic_temp, dic_res]
#
#     if len(comp) < 4:
#         comp = zi_comp
#         if comp not in dic_res:
#             dic_res.update({comp: guquan})
#         else:
#             dic_res[comp] += guquan
#         return [dic_temp, dic_res]
#
#     for stop_str in stop_dic:
#         if stop_str in comp:
#             if comp not in dic_res:
#                 dic_res.update({comp: guquan})
#             else:
#                 dic_res[comp] += guquan
#             return [dic_temp, dic_res]
#
#     times += 1
#     if times > 20:
#         if comp not in dic_res:
#             dic_res.update({comp: guquan})
#         else:
#             dic_res[comp] += guquan
#         return [dic_temp, dic_res]
#     if guquan < 0.01:
#         if comp not in dic_res:
#             dic_res.update({comp: guquan})
#         else:
#             dic_res[comp] += guquan
#         return [dic_temp, dic_res]
#     if comp in dic_son_name:
#         fu_comp_dic = dic_son_name[comp]
#     elif comp in dic_old_name:
#         fu_comp_dic = {}
#         for comp_old in dic_old_name[comp]:
#             if comp_old in dic_son_name:
#                 if len(fu_comp_dic) == 0:
#                     fu_comp_dic = dic_son_name[comp_old]
#                 else:
#                     fu_comp_dic.update(dic_son_name[comp_old])
#             else:
#                 continue
#         if len(fu_comp_dic) == 0:
#             if comp not in dic_res:
#                 dic_res.update({comp: guquan})
#             else:
#                 dic_res[comp] += guquan
#             return [dic_temp, dic_res]
#     else:
#         if comp not in dic_res:
#             dic_res.update({comp: guquan})
#         else:
#             dic_res[comp] += guquan
#         return [dic_temp, dic_res]
#
#     for fu_comp, one_guquan in fu_comp_dic.items():
#         # 判断这一层如果出现人名，整层返回
#         panduan = False
#         for fucomp in fu_comp_dic:
#             for to_last in stop_to_last:
#                 if len(fucomp) < 4 or to_last in fucomp:
#                     panduan = True
#                     break
#             if panduan is True:
#                 break
#         if panduan:
#             if comp in dic_res:
#                 dic_res[comp] += guquan
#             else:
#                 dic_res.update({comp: guquan})
#             break
#
#         percent = guquan * one_guquan
#         # 判断父子关系
#         if fu_comp not in dic_temp:
#             pass
#         else:
#             if fu_comp in dic_res:
#                 for to_last in stop_to_last:
#                     if to_last in fu_comp or len(fu_comp) < 4:
#                         fu_comp = comp
#                         break
#                 if fu_comp in dic_res:
#                     dic_res[fu_comp] += percent
#                 else:
#                     dic_res.update({fu_comp: percent})
#                 continue
#             else:
#                 pass
#
#         # if percent < 0.01:
#         #     fu_comp = get_first_comp(fu_comp, fu_comp, dic_son_name, stop_to_last, stop_dic, 0)
#         #     if fu_comp not in dic_res:
#         #         dic_res.update({fu_comp: percent})
#         #     else:
#         #         dic_res[fu_comp] += percent
#         #     continue
#
#         dic_temp.update({fu_comp: percent})
#         dic_temp = \
#         recursion(fu_comp, comp, percent, dic_old_name, dic_son_name, times, dic_temp, stop_dic, stop_to_last,
#                   dic_res)[0]
#
#     return [dic_temp, dic_res]
#
#
# # 新股权20200511
# # 获取新的股权
# def get_new_hebing(comp, dic_son_name, stop_dic, stop_to_last, dic_changyongming, dic_old_name):
#     user_dic_1 = hebing(comp, dic_son_name, stop_dic, stop_to_last, dic_changyongming, dic_old_name)
#     print(user_dic_1)
#     dic_not_comp = {}
#     for use_comp in user_dic_1:
#         for comp, use_name in dic_changyongming.items():
#             if use_name == use_comp:
#                 break
#         else:
#             dic_not_comp[use_comp] = 1
#     if len(dic_not_comp) == 0:
#         return user_dic_1
#     for comp_i in dic_not_comp:
#         comp_dic = hebing(comp_i, dic_son_name, stop_dic, stop_to_last, dic_changyongming, dic_old_name)
#         res_comp = None
#         for key, values in comp_dic.items():
#             key = chinese_to_english(key)
#             for comp_k, chang_name in dic_changyongming.items():
#                 if chang_name == key:
#                     if values > 0.1:
#                         res_comp = key
#                     break
#             break
#         if res_comp is None:
#             continue
#         else:
#             if res_comp in user_dic_1:
#                 user_dic_1[res_comp] += user_dic_1[comp_i]
#                 user_dic_1.pop(comp_i)
#             else:
#                 user_dic_1[res_comp] = user_dic_1[comp_i]
#                 user_dic_1.pop(comp_i)
#     return user_dic_1
#
#
# # 获取股权关系替换常用名合并股权
# def hebing(comp, dic_son_name, stop_dic, stop_to_last, dic_changyongming, dic_old_name):
#     """
#     获取股权关系替换常用名合并股权
#     :param comp: 项目公司
#     :param dic_son_name: 子对父的关系表
#     :param stop_dic: 停用词
#     :param stop_to_last: 通用词找下级
#     :param dic_changyongming: 常用名字典{'公司名': '常用名'}
#     :param dic_old_name: 曾用名
#     :return:
#     """
#     comp = chinese_to_english(comp)
#     jituan_dic = recursion(comp, comp, 1, dic_old_name, dic_son_name, 0, {comp: 1}, stop_dic, stop_to_last, {})
#     jituan_list = sorted(jituan_dic[1].items(), key=lambda x: x[1], reverse=True)
#     dic_new = {}
#     for mess in jituan_list:
#         key, values = mess
#         key = chinese_to_english(key)
#         if key in dic_changyongming:
#             key = dic_changyongming[key]
#         else:
#             for comp, chang_name in dic_changyongming.items():
#                 if comp in key:
#                     key = chang_name
#                     break
#                 else:
#                     continue
#         if key not in dic_new:
#             dic_new[key] = values
#         else:
#             dic_new[key] += values
#     return dic_new


# 时间转换
# 2019.12.24 自下而上股权关系
def recursion(comp, zi_comp, guquan, dic_old_name, dic_son_name, times, dic_temp, stop_dic, stop_to_last, dic_res):
    """

    :param comp: 父公司
    :param zi_comp: 子公司
    :param guquan: 乘出来的股权
    :param dic_old_name: 曾用名
    :param dic_son_name: 子对父关系的字典
    :param times: 次数
    :param dic_temp: 每一层关系的字典
    :param stop_dic: 停用词字典
    :param stop_to_last: 停用词找下级
    :param dic_res: 返回的字典
    :return:
    """
    comp = str(comp)
    for to_last in stop_to_last:
        if '等' in comp and '人' in comp:
            comp = zi_comp
            if comp not in dic_res:
                dic_res.update({comp: guquan})
            else:
                dic_res[comp] += guquan
            return [dic_temp, dic_res]
        if to_last in comp or len(comp) < 4 or getFirstName(comp):
            comp = zi_comp
            if comp not in dic_res:
                dic_res.update({comp: guquan})
            else:
                dic_res[comp] += guquan
            return [dic_temp, dic_res]

    if len(comp) < 4:
        comp = zi_comp
        if comp not in dic_res:
            dic_res.update({comp: guquan})
        else:
            dic_res[comp] += guquan
        return [dic_temp, dic_res]

    for stop_str in stop_dic:
        if stop_str in comp:
            if comp not in dic_res:
                dic_res.update({comp: guquan})
            else:
                dic_res[comp] += guquan
            return [dic_temp, dic_res]

    times += 1
    if times > 20:
        if comp not in dic_res:
            dic_res.update({comp: guquan})
        else:
            dic_res[comp] += guquan
        return [dic_temp, dic_res]
    if guquan < 0.01:
        # print(comp, guquan)
        if comp not in dic_res:
            dic_res.update({comp: guquan})
        else:
            dic_res[comp] += guquan
        return [dic_temp, dic_res]
    # print(comp)
    if comp in dic_son_name:
        fu_comp_dic = dic_son_name[comp]
    elif comp in dic_old_name:
        fu_comp_dic = {}
        for comp_old in dic_old_name[comp]:
            if comp_old in dic_son_name:
                if len(fu_comp_dic) == 0:
                    fu_comp_dic = dic_son_name[comp_old]
                else:
                    fu_comp_dic.update(dic_son_name[comp_old])
            else:
                continue
        if len(fu_comp_dic) == 0:
            if comp not in dic_res:
                dic_res.update({comp: guquan})
            else:
                dic_res[comp] += guquan
            return [dic_temp, dic_res]
    else:
        if comp not in dic_res:
            dic_res.update({comp: guquan})
        else:
            dic_res[comp] += guquan
        return [dic_temp, dic_res]

    for fu_comp, one_guquan in fu_comp_dic.items():
        # 判断这一层如果出现人名，整层返回
        panduan = False
        for fucomp in fu_comp_dic:
            for to_last in stop_to_last:
                if len(fucomp) < 4 or to_last in fucomp:
                    panduan = True
                    break
            if panduan is True:
                break
        if panduan:
            if comp in dic_res:
                dic_res[comp] += guquan
            else:
                dic_res.update({comp: guquan})
            break

        percent = guquan * one_guquan
        # 判断父子关系
        if fu_comp not in dic_temp:
            pass
        else:
            if fu_comp in dic_res:
                for to_last in stop_to_last:
                    if to_last in fu_comp or len(fu_comp) < 4:
                        fu_comp = comp
                        break
                if fu_comp in dic_res:
                    dic_res[fu_comp] += percent
                else:
                    dic_res.update({fu_comp: percent})
                continue
            else:
                pass

        # if percent < 0.01:
        #     fu_comp = get_first_comp(fu_comp, fu_comp, dic_son_name, stop_to_last, stop_dic, 0)
        #     if fu_comp not in dic_res:
        #         dic_res.update({fu_comp: percent})
        #     else:
        #         dic_res[fu_comp] += percent
        #     continue

        dic_temp.update({fu_comp: percent})
        dic_temp = \
        recursion(fu_comp, comp, percent, dic_old_name, dic_son_name, times, dic_temp, stop_dic, stop_to_last,
                  dic_res)[0]

    return [dic_temp, dic_res]


def get_new_dic(dic_new, jituan_list, dic_changyongming):
    for mess in jituan_list:
        key, values = mess
        key = chinese_to_english(key)
        if key in dic_changyongming:
            key = dic_changyongming[key]
        else:
            for comp, chang_name in dic_changyongming.items():
                if str(comp) in str(key):
                    key = chang_name
                    break
        if key not in dic_new:
            dic_new[key] = values
        else:
            dic_new[key] += values
    return dic_new


# 获取股权关系替换常用名合并股权
def hebing(comp, dic_son_name, stop_dic, stop_to_last, dic_changyongming, dic_old_name):
    """
    获取股权关系替换常用名合并股权
    :param comp: 项目公司
    :param dic_son_name: 子对父的关系表
    :param stop_dic: 停用词
    :param stop_to_last: 通用词找下级
    :param dic_changyongming: 常用名字典{'公司名': '常用名'}
    :param dic_old_name: 曾用名
    :return:
    """
    comp = chinese_to_english(comp)
    jituan_dic = recursion(comp, comp, 1, dic_old_name, dic_son_name, 0, {comp: 1}, stop_dic, stop_to_last, {})
    # print(jituan_dic[1])
    jituan_list = sorted(jituan_dic[1].items(), key=lambda x: x[1], reverse=True)
    dic_new = get_new_dic({}, jituan_list, dic_changyongming)

    # 将股权小于1% 的  加到最大的上面
    # dic_new_i = {}
    # for new_jituan, new_percent in dic_new.items():
    #     if new_percent < 0.01:
    #         dic_new_i[list(dic_new.keys())[0]] += new_percent
    #     else:
    #         dic_new_i[new_jituan] = new_percent
    return dic_new


# 获取新的股权
def get_new_hebing(comp, dic_son_name, stop_dic, stop_to_last, dic_changyongming, dic_old_name):
    user_dic_1 = hebing(comp, dic_son_name, stop_dic, stop_to_last, dic_changyongming, dic_old_name)
    dic_not_comp = {}
    for use_comp in user_dic_1:
        for comp, use_name in dic_changyongming.items():
            if use_name == use_comp:
                break
        else:
            dic_not_comp[use_comp] = 1
    if len(dic_not_comp) == 0:
        return user_dic_1
    for comp_i in dic_not_comp:
        comp_dic = hebing(comp_i, dic_son_name, stop_dic, stop_to_last, dic_changyongming, dic_old_name)
        res_comp = None
        for key, values in comp_dic.items():
            key = chinese_to_english(key)
            for comp_k, chang_name in dic_changyongming.items():
                if chang_name == key:
                    if values > 0.1:
                        res_comp = key
                    break
            break
        if res_comp is None:
            continue
        else:
            if res_comp in user_dic_1:
                user_dic_1[res_comp] += user_dic_1[comp_i]
                user_dic_1.pop(comp_i)
            else:
                user_dic_1[res_comp] = user_dic_1[comp_i]
                user_dic_1.pop(comp_i)
    return user_dic_1


def stampToTime(stamp):
    """
    给出时间戳，将其转化为年月日
    :param stamp:
    :return:
    """
    datatime = time.strftime("%Y-%m-%d", time.localtime(float(str(stamp)[0:10])))
    return datatime


def update_old_name():
    with open(r'message/曾用名_dic.log', 'rb') as f:
        old_dic_ = pickle.load(f)
    dic = {'深圳市德莱顿供应链管理有限公司': '深圳市德莱顿实业有限公司', '盈信投资集团股份有限公司': '深圳市盈信创业投资股份有限公司',
           '实地地产集团有限公司': '广州实地房地产开发有限公司', '融创华北发展集团有限公司': '融创华北区域房地产集团有限公司',
           '中天美好集团有限公司': '浙江中天房地产集团有限公司', '广东中海地产有限公司': '中信华南（集团）有限公司'}
    for key, values in dic.items():
        if key not in old_dic_:
            old_dic_[key] = [values]
        else:
            if values not in old_dic_[key]:
                old_dic_[key].append(values)
            else:
                continue
    with open(r'message/曾用名_dic.log', 'wb') as f:
        pickle.dump(old_dic_, f)


def split_position(position):
    if position is None:
        return [position]
    if '楼盘地址' in position and '查看地图' in position:
        position = re.findall(r'楼盘地址：(.*?)\[查看地图]', position)[0]
    position = chinese_to_english(position)
    erro_list = ['暂无资料', 'nan']
    result = []
    if '（' in position or '）' in position or '(' in position or ')' in position:
        te_list1 = re.findall(r'[(（](.*?)[)）]', position)[0]
        te_list2 = position.split('(')[0]
        result.append(te_list1)
        result.append(te_list2)
    elif ',' in position:
        te_list3 = position.split(',')
        for te in te_list3:
            if te not in result and te not in erro_list:
                result.append(te)
    elif '、' in position:
        te_list3 = position.split('、')
        for te in te_list3:
            if te not in result and te not in erro_list:
                result.append(te)
    else:
        result.append(position)

    return result


def chuli_polt(plot):
    if plot == '' or plot == None:
        return None, None
    if plot.startswith('等于'):
        res_plot = plot.split('等于')[1]
        return float(res_plot), float(res_plot)
    plot = plot + '等'
    res_list = re.findall(r'(\d.*?)[\u4E00-\u9FA5]', plot)
    plot_down = None
    plot_up = None
    if len(res_list) == 2:
        if float(res_list[0]) > float(res_list[1]):
            plot_up = float(res_list[0])
            plot_down = float(res_list[1])
        else:
            plot_up = float(res_list[1])
            plot_down = float(res_list[0])
    else:
        if '大于' in plot and '小于' not in plot and len(res_list) == 1:
            plot_down = float(res_list[0])
        elif '小于' in plot and '大于' not in plot and len(res_list) == 1:
            plot_up = float(res_list[0])
    return plot_down, plot_up


# down, up = chuli_polt('大于或等于1并且小于或等于1.8')
# print(up)

def judge_comp_name(comp):
    comp = re.findall(r'([\u4e00-\u9fa5A-Za-z\d]+)', comp)
    if len(comp) == 0:
        return False
    comp = ''.join(map(lambda x: str(x), comp))
    comp_number = re.findall(r'(\d+)', comp)
    if len(comp_number) == 0:
        return True
    else:
        comp_number = ''.join(map(lambda x: str(x), comp_number))
        if len(comp_number) / len(comp) >= 0.6:
            return False
    return True


def get_date_week(value):
    te_datetime = datetime.datetime.strptime(str(value), '%Y-%m-%d')
    mess_tuple = te_datetime.isocalendar()
    year, week_th, weekness = mess_tuple
    begin_date = str((te_datetime - datetime.timedelta(days=weekness - 1)).date())
    end_date = str((te_datetime + datetime.timedelta(days=7 - weekness)).date())
    return week_th, begin_date, end_date


def get_date_week2(value):
    """
    拼接当前日期周次
    :param value:
    :return:
    """
    if str(value) == 'None' or value == '':
        return None
    try:
        te_datetime = datetime.datetime.strptime(str(value), '%Y-%m-%d')
        mess_tuple = te_datetime.isocalendar()
        year, week_th, weekness = mess_tuple
        begin_date = (te_datetime - datetime.timedelta(days=weekness - 1)).date()
        year = str(te_datetime.year)
        if len(str(week_th)) == 1:
            week_th = '0' + str(week_th)
        # print(week_th)
        if str(value)[5:7] == '01' and int(week_th) > 5:
            year_begin = str(begin_date.year)
            result_ = year_begin + str(week_th)
        else:
            result_ = year + str(week_th)
    except Exception as e:
        print(value, e)
        result_ = None
    return result_


def get_groupNm_list(sql_parm=None):
    conn_i = pymysql.Connection(host='192.168.1.62', port=3306, user='root', password='root', db='srhcenter', charset="utf8")
    cur_i = conn_i.cursor()
    if sql_parm is None:
        sql = "SELECT * FROM schcomp WHERE topList <= 201 and compType = '房地产业'"
    else:
        sql = sql_parm
    cur_i.execute(sql)
    res_all = cur_i.fetchall()
    gro_dic = {}
    for res in res_all:
        comp_name = res[1]
        gro_dic[comp_name] = res[0]
    return gro_dic
# print(judge_comp_name('None'))
# print(get_date_week('2020-04-26')[0])


# print(get_date_week2('2018-01-03'))
# print(get_date_week('2018-01-03'))

# print(len('中低价位、中小套型普通商品住房用地'))

# print(get_date_y_m_d('2020/6/4商品房日成交情况'))
# print(get_date_y_m_d('浔售许字（2020）第00020号'))
# print(chinese_to_english('铜住建委（2019）预字第（0065）号，铜住建委（2019）预字第（0072）号，'))