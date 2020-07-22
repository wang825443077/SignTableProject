columns_dict = {
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

beijing = {
    # 原始表名
    'origTable_pro': 'beijing0101_pro_copy',
    'origTable_room': 'beijing0101_room_copy',
    'origTable_sta': 'beijing0101_sta_copy',
    # 生成表名
    'projTableName': 'signproj_beijing',  # 第一张表
    'dataTable': 'signdata_beijing',  # 第二张表
    'dataTable1': 'signdata_beijing_1',  # 第三张表
    'FourTable': 'signdata_beijing_3_copy1',  # 第四张表
    'FiveTable': 'signdata_beijing_4_copy1',  # 第五张表
    # 其他
    'columnIndex': columns_dict,
    'province': '北京',
    'city': '北京市',
    'projTextColumns': ['appro_building_original', 'constru_landnum_original'],
    'roomUniqueField': 'md5',
    'roomStatusField': 'sales_status2',
    'status_top_all': {'已预定': 5, '资格核验中': 5, '已预订': 5, '可售': 1, '网上联机备案': 1, '不可售': 1},
    'area': 'scale',
    'orgiIP': '192.168.1.80',
    'orgipassword': 'root',
}






