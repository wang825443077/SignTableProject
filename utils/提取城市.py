import pickle
import jieba
import os

log_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'utils\\message')

class GetCityProvince(object):
    def __init__(self, city=None, province=None, region=None):
        """
        :param city: 返回城市
        :param province: 返回省份

        """
        with open(r'{}\province_to_city_dict.log'.format(log_path), 'rb') as f:
            city_to_province_dict = pickle.load(f)
        with open(r'{}\region_to_city.log'.format(log_path), 'rb') as f:
            region_to_city = pickle.load(f)
        for region in city_to_province_dict:
            # if '市本级' in region:
            #     continue
            jieba.add_word(region)
        for region in region_to_city:
            # if '市本级' in region:
            #     continue
            jieba.add_word(region)
        self.city_to_province_dict = city_to_province_dict
        self.region_to_city = region_to_city
        self.city = city
        self.province = province
        self.region = region

    # 判断是空 或 None
    def judge_none(self, mess):
        if mess is None or mess == '' or mess == 'None' or str(mess) == 'nan' or str(mess) == 'null' or mess == ' ':
            return True
        else:
            return False

    def update_province(self, mess):
        """
        传进来一个省份
        :param mess:
        :return:
        """
        if self.judge_none(mess):
            return None
        province_dic = {'上海': 1, '云南': 1, '内蒙古': 1, '北京': 1, '吉林': 1, '四川': 1, '天津': 1, '宁夏': 1, '安徽': 1,
                        '山东': 1, '山西': 1, '广东': 1, '广西': 1, '新疆': 1, '江苏': 1, '江西': 1, '河北': 1, '河南': 1,
                        '浙江': 1, '海南': 1, '湖北': 1, '湖南': 1, '甘肃': 1, '福建': 1, '西藏': 1, '贵州': 1, '辽宁': 1,
                        '重庆': 1, '陕西': 1, '青海': 1, '黑龙江': 1}
        for pro in province_dic:
            if pro in mess:
                mess = pro
                return mess
        return None

    def get_city_no_province(self, loc, extral_province=None):
        """

        :param loc:
        :param extral_province:  给定的省份,输出城市必须是此省份
        :return:
        """
        self.city = None
        self.province = None
        self.region = None
        dic = {}
        seg_list = jieba.lcut(str(loc))
        # print(seg_list)
        for res in seg_list:
            if len(res) < 2:
                continue
            new_res = str(res) + '市'
            if res in self.city_to_province_dict:
                # return res
                te_provin = self.city_to_province_dict[res]
                if extral_province is not None:

                    if te_provin not in extral_province and extral_province not in te_provin:
                        # print(te_provin, extral_province)
                        continue
                if res not in dic:
                    dic[res] = 2
                else:
                    dic[res] += 2
            if new_res in self.city_to_province_dict:
                # return new_res
                te_provin = self.city_to_province_dict[new_res]
                if extral_province is not None:
                    if te_provin not in extral_province and extral_province not in te_provin:
                        # print(te_provin, extral_province)
                        continue
                if new_res not in dic:
                    dic[new_res] = 2
                else:
                    dic[new_res] += 2

            elif res in self.region_to_city:
                city = self.region_to_city[res]
                if not self.region:
                    self.region = res
                elif '本级' in self.region:
                    self.region = res
                if city in self.city_to_province_dict:
                    te_provin = self.city_to_province_dict[city]
                    if extral_province is not None:
                        if te_provin not in extral_province and extral_province not in te_provin:
                            # print(te_provin, extral_province)
                            continue
                else:
                    continue
                # return city
                # print(city)
                if city not in dic:
                    dic[city] = 1
                else:
                    dic[city] += 1
                # if len(city) < 3:
                #     return None
            else:
                list_ = ['市', '县', '区', '地区', '本级']

                for add_ in list_:
                    new_ = res + add_
                    if new_ in self.region_to_city:
                        city = self.region_to_city[new_]
                        if not self.region:
                            self.region = new_
                        elif '本级' in self.region:
                            self.region = new_
                        if city in self.city_to_province_dict:
                            te_provin = self.city_to_province_dict[city]
                            # te_provin = self.city_to_province_dict[new_res]
                            if extral_province is not None:
                                if te_provin not in extral_province and extral_province not in te_provin:
                                    # print(te_provin, extral_province)
                                    continue
                        else:
                            continue
                        if city not in dic:
                            dic[city] = 1
                        else:
                            dic[city] += 1
                        break
        else:
            # print(dic)
            # return None
            if len(dic) == 0:
                self.city = None
            else:
                city = max(dic, key=lambda k: dic[k])
                self.city = city
                if city in self.city_to_province_dict:
                    province = self.city_to_province_dict[city]
                    self.province = province
        self.province = self.update_province(self.province)
        zhixia_dic = {'上海市', '重庆市', '北京市', '天津市'}
        for zhixia in zhixia_dic:
            if zhixia in str(self.city):
                self.city = zhixia
                self.province = zhixia[:-1]
        try:
            if not self.judge_none(self.city) and not self.judge_none(self.region):
                if self.region_to_city[self.region] != self.city:
                    self.region = None
        except:
            print(loc, '区县错误')
            pass
        return self.city, self.region, self.province


if __name__ == '__main__':
    loc = '涧西区'
    class_ = GetCityProvince()
    print(class_.get_city_no_province(loc))



