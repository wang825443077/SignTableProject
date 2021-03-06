
mysql_address = {
    '34': {'host':'192.168.1.34', 'port':3306, 'user':'root', 'password':'root', 'db':'网签清洗', 'charset':"utf8"},    # 左宁宁
    '47': {'host':'192.168.1.47', 'port':3306, 'user':'root', 'password':'root', 'db':'网签清洗', 'charset':"utf8"},    # 王盟盟
    '62': {'host':'192.168.1.62', 'port':3306, 'user':'root', 'password':'root', 'db':'网签清洗', 'charset':"utf8"},    # 王悦磊

    '80': {'host':'192.168.1.80', 'port':3306, 'user':'root', 'password':'root', 'db':'wangqian', 'charset':"utf8"},
    '150': {'host':'192.168.1.150', 'port':3306, 'user':'root', 'password':'1234', 'db':'wangqian', 'charset':"utf8"},
    '139': {'host':'192.168.1.139', 'port':3306, 'user':'root', 'password':'root', 'db':'wangqian', 'charset':"utf8"},      # 卢长路
    '77': {'host':'192.168.1.77', 'port':3306, 'user':'root', 'password':'798236031', 'db':'wangqian', 'charset':"utf8"},   # 吴林汇
    '140': {'host':'192.168.1.77', 'port':3306, 'user':'root', 'password':'123456', 'db':'wangqian', 'charset':"utf8"},     # 卢长路本机
    '6': {'host':'192.168.1.6', 'port': 3306, 'user': 'root', 'password':'root', 'db':'wangqian', 'charset':"utf8"},     # 卢长路服务器
}


beijing = {
    # 数据库连接
    'origDB': mysql_address['47'],   # 网签原始数据库
    'localDB': mysql_address['80'],  # 保存处理结果数据库
    # 原始表名
    'origTable_pro': 'beijing0101_pro',
    'origTable_room': 'beijing0101_room',
    'origTable_sta': 'beijing0101_sta',
    # 生成表名
    'projTableName': 'signproj_beijing',  # 第一张表
    'dataTable': 'signdata_beijing',  # 第二张表
    'dataTable1': 'signdata_beijing_1',  # 第三张表
    'FourTable': 'signdata_beijing_3_copy1',  # 第四张表
    'FiveTable': 'signdata_beijing_4_copy1',  # 第五张表
    # 爬取时间列
    'proj_spider_time_field': 'spidertime',
    'room_spider_time_field': 'spidertime',
    'sta_spider_time_field': 'spider_time',
    # 其他
    'columnIndex': {
                    'proname': 'proname_original',
                    'company': 'company_original',
                    'position': 'position_original',
                    'ca_num': 'ca_num_original',
                    'region': '',
                    'city': '',
                    'province': '',
                    'dataTable': '',
                    'spider_time': 'spidertime_original',
                }
,    # 生成的proj表,固定列对应列名
    'province': '北京',
    'city': '北京市',
    'projTextColumns': ['appro_building_original', 'constru_landnum_original'],  # 生成的proj表,这些列格式设置为text
    'roomUniqueField': 'md5',       # room/sta表标识唯一的列
    'staStatusField': 'sales_status2',  # sta表标识状态的列名
    'status_top_all': {'已预定': 5, '资格核验中': 5, '已预订': 5, '可售': 1, '网上联机备案': 1, '不可售': 1},  # 城市的销售状态等级
    'area': 'scale',    # 面积对应字段
    'orgiIP': '192.168.1.80',
    'orgipassword': 'root',
}

guangzhou = {
    # 数据库连接
    'origDB': mysql_address['6'],   # 网签原始数据库
    'localDB': mysql_address['47'],  # 保存处理结果数据库
    # 原始表名
    'origTable_pro': '111guangzhou0101_pro',
    'origTable_room': '111guangzhou0101_room',
    'origTable_sta': '111guangzhou0101_sta',
    # 生成表名
    'projTableName': 'signproj_guagnzhou',  # 第一张表
    'dataTable': 'signdata_guagnzhou_2',  # 第二张表
    'dataTable1': 'signdata_guagnzhou_3',  # 第三张表
    'FourTable': 'signdata_guagnzhou_4',  # 第四张表
    'FiveTable': 'signdata_guagnzhou_5',  # 第五张表
    # 爬取时间列
    'proj_spider_time_field': 'spidertime',
    'room_spider_time_field': 'spidertime',
    'sta_spider_time_field': 'spidertime',
    'signproj_spider_time_field': 'spidertime',
    'dataTable_spider_time_field': 'spidertime',
    'table1_spider_time_field': 'spider_time',
    'FourTable_spider_time_field': 'spider_time',
    'FiveTable_spider_time_field': 'spider_time',
    # 表连接字段
    'proj_merge_columns': ['pjid'],  # 生成的proj表
    'room_merge_columns': ['pjid'],  # 原始room表
    # 其他
    # 有空列输出列
    'proj_err_out_columns': ['pro_name', 'url'],
    'room_err_out_columns': ['url'],
    'sta_err_out_columns': ['pjid'],

    # 生成的proj表,固定列对应列名
    'province': '广东',
    'city': '广州市',
    'projTextColumns': ['position'],  # 生成的proj表,这些列格式设置为text
    'roomUniqueField': 'unitID',       # room/sta表标识唯一的列origTable_room
    'staStatusField': 'sales_status',  # sta表标识状态的列名
    'status_top_all': {'已过户': 5, '预售可售': 1, '强制冻结': 1, '确权不可售': 5, '不可销售': 5, '确权可售': 1},  # 城市的销售状态等级

    # 清洗列
    'proj': {'number': ['area_m2', 'scale_m2']},
    'room': {'number': ['Floor_height', 'Expected_area_m2', 'Expected_Inside_area_m2', 'Expected_Shared_area_m2', 'Actual_area_m2', 'actual_inside_area_m2', 'actual_shared_area_m2']},
    'projcolumns_dict': {'pjid': 'pjid', 'pro_name': 'proname', 'company': 'company', 'ca_num': 'ca_num', 'position': 'position',
                         'district': 'district', 'area_m2': 'area', 'scale_m2': 'scale', 'certificate_no': 'certificate_no',
                         'use_type': 'use_', 'spidertime': 'spidertime'},

    'roomcolumns_dict': {'unitID': 'unitID', 'pjid': 'pjid', 'building': 'building', 'floor': 'floor', 'RoomNum2': 'RoomNum2', 'roomuse': 'roomuse',
                         'Actual_floor': 'actual_floor', 'Named_floor': 'Named_floor', 'Floor_height': 'floor_height',
                         'Roomtype': 'roomtype', 'Room_structure': 'room_structu', 'Expected_area_m2': 'expected_area',
                         'Expected_Inside_area_m2': 'expected_inside_area', 'Expected_Shared_area_m2': 'expected_shared_area',
                         'Actual_area_m2': 'actual_area', 'actual_inside_area_m2': 'actual_inside_area',
                         'actual_shared_area_m2': 'actual_shared_area', 'spidertime': 'spidertime'}
}


shenzhen = {
    # 数据库连接
    'origDB': mysql_address['80'],  # 网签原始数据库
    'localDB': mysql_address['62'],  # 保存处理结果数据库
    # 原始表名
    'origTable_pro': 'shenzhen0101_pro',
    'origTable_room': 'shenzhen0101_room',
    'origTable_sta': 'shenzhen0101_sta',
    # 生成表名
    'projTableName': 'signproj_shenzhen',  # 第一张表
    'dataTable': 'signdata_shenzhen_2',  # 第二张表
    'dataTable1': 'signdata_shenzhen_3',  # 第三张表
    'FourTable': 'signdata_shenzhen_4',  # 第四张表
    'FiveTable': 'signdata_shenzhen_5',  # 第五张表
    # 爬取时间列
    'proj_spider_time_field': 'spidertime',
    'room_spider_time_field': 'spidertime',
    'sta_spider_time_field': 'spidertime',
    'signproj_spider_time_field': 'spidertime',
    'dataTable_spider_time_field': 'spidertime',
    'table1_spider_time_field': 'spider_time',
    'FourTable_spider_time_field': 'spider_time',
    'FiveTable_spider_time_field': 'spider_time',
    # 表连接字段
    'proj_merge_columns': ['pro_link'],  # 生成的proj表
    'room_merge_columns': ['pro_link'],  # 原始room表
    # 其他

    # 生成的proj表,固定列对应列名
    'province': '广东',
    'city': '深圳市',
    'projTextColumns': ['position'],  # 生成的proj表,这些列格式设置为text
    'roomUniqueField': 'hu_link',  # room/sta表标识唯一的列origTable_room
    'staStatusField': 'sale_state',  # sta表标识状态的列名
    'status_top_all': {
        '已签预售合同': 5, '已备案': 1, '期房待售': 1, '已签认购书': 1, '司法查封': 1, '区局锁定': 1, '系统自动锁定': 1,
        '未知': 1, '初始登记': 1
    },  # 城市的销售状态等级

    # # 有空列输出列
    # 'proj_err_out_columns': ['proname', 'url'],
    # 'room_err_out_columns': ['url'],
    # 'sta_err_out_columns': ['pjid'],

    # 清洗列
    'proj': {'number': ['base_area', 'ancestral_area', 'toscale', 'tonum', 'toarea', 'tonum_onsale_']},
    'room': {'number': ['sale_price', 'scale', 'inside_area1', 'shared_area_start', 'inside_area2',
                        'shared_area_end', 'scale_end']},
    'projcolumns_dict': {
        'url': 'pro_link', 'proname': 'pro_name', 'company': 'company', 'ca_num': 'ca_num',
        'land_location': 'land_location', 'approval_time': 'approval_time', 'land_num2': 'land_num',
        'district': 'district', 'land_date': 'land_date', 'region': 'region', 'ownership_source': 'ownership_source',
        'approval_auth': 'approval_auth', 'contract_doc': 'contract_doc', 'durable_yeas': 'durable_yeas',
        'sup_agreement': 'sup_agreement', 'permit': 'permit', 'room_use': 'room_use', 'land_use': 'land_use',
        'land_lever': 'land_lever', 'base_area': 'base_area', 'ancestral_area': 'ancestral_area', 'toscale': 'toscale',
        'tonum': 'tonum', 'toarea': 'toarea', 'tonum_onsale': 'tonum_onsale', 'tonum_onsale_': 'tonum_onsale_',
        'spidertime': 'spidertime'
                         },

    'roomcolumns_dict': {
        'hu_link': 'hu_link', 'pro_link': 'pro_link', 'Building_name': 'building_name', 'projectnum': 'projectnum',
        'construnum': 'construnum', 'seatnum': 'seatnum', 'floor': 'floor', 'roomnum': 'roomnum',
        'sale_state': 'Sale_state', 'project_vul': 'Project_vul', 'contract_num': 'contract_num',
        'sale_price': 'sale_price', 'use': 'use_', 'scale': 'scale', 'inside_area1': 'inside_area1',
        'shared_area_start': 'shared_area_start', 'inside_area2': 'inside_area2', 'shared_area_end': 'shared_area_end',
        'scale_end': 'Scale_end', 'spidertime': 'spidertime'
    }
}

