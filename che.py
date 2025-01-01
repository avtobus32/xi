import datetime
import os
import time

import numpy
import pandas

from translator import myTranslator
from utils import get, dump_parsed_data, to_sql
from constants import brands_ch_en, brands_en_ch, CARS_PER_SERIES, logger, MIN_REG_YEAR, DATA_FILES, TRANSLATE_CATS, \
    PARSING_DELAY_DAYS

DELAY = 0

def get_brands_data():
    """
    {
    "bid": 33,
    "name": "奥迪",
    "py": "aodi",
    "icon": "http://car3.autoimg.cn/cardfs/series/g28/M01/F6/71/100x100_autohomecar__CjIFVGTIoWqAdlH5AABfFDX2Peo393.png.webp",
    "sid": 0,
    "sname": "",
    "sicon": "",
    "dtype": 0,
    "url": "autohome://usedcar/buycarlist?brand=%7B%22brandid%22%3A%2233%22%2C%22bname%22%3A%22%E5%A5%A5%E8%BF%AA%22%7D&pvareaid=110917",
    "price": "0.9万",
    "on_sale_num": 50715879,
    "name_en": "Audi"
    }
    :return:
    """
    brands_data = {}
    req = get("https://api2scsou.che168.com/api/v2/getbrands", DELAY, params={"_appid": "2sc.m"})
    if req.status_code == 200 and req.json()['message'] == "成功":
        for brands_by_letter in req.json()['result']['brands']:
            for brand in brands_by_letter['brand']:
                brand_name = brand['name']
                if brand_name in brands_ch_en:
                    brands_data[brand_name] = brand
                    brands_data[brand_name]['name_en'] = brands_ch_en[brand_name]
    else:
        logger.critical(str(req.__dict__))
        logger.critical('failed get brands data')
        raise ConnectionError
    return brands_data

def get_series_data(brand_id):
    """
    {
    "sid": 3170,
    "py": "aodia3",
    "icon": "http://car2.autoimg.cn/cardfs/series/g32/M06/F6/22/400x300_autohomecar__ChxkPWckw4SAfaoMAAa1kzYilZ0864.png.webp",
    "pricerange": "3.98-24.00",
    "name": "奥迪A3"
    }
    :param brand_id:
    :return:
    """
    series_data = {}
    req = get("https://api2scsou.che168.com/api/v2/getseriesbybrandid", DELAY,
                params={"_appid": "2sc.m", "brandid": brand_id})
    if req.status_code == 200 and req.json()['message'] == "成功":
        for series in req.json()['result']['list'][0]['item']:
            series['name'] = series.pop('sname')
            series_data[series['name']] = series
    else:
        logger.critical(str(req.__dict__))
        logger.critical('failed get series data')
        raise ConnectionError
    return series_data

def get_cars_list(brand_id, series_id):
    """
    {
        "infoid": 52597744,
        "carname": "奥迪A3 2022款 Sportback 35 TFSI 时尚致雅型",
        "cname": "成都",
        "dealerid": 84424,
        "mileage": "3.55",
        "cityid": 510100,
        "seriesid": 3170,
        "specid": 53932,
        "price": "12.58",
        "saveprice": "",
        "discount": "",
        "firstregyear": "2022年",
        "fromtype": 40,
        "imageurl": "https://2sc2.autoimg.cn/escimg/auto/g32/M06/97/4E/400x300_c42_autohomecar__ChxkPmcgjuqAUc98AAF_W9kNk-I041.jpg.webp",
        "cartype": 30,
        "bucket": 10,
        "isunion": 0,
        "isoutsite": 0,
        "videourl": "",
        "car_level": 0,
        "dealer_level": "13年老店",
        "downpayment": "3.77",
        "url": "usedcar://rninsidebrowser?url=rn%3A%2F%2FUsedCar_Detail%2FUsedCar_Detail%3Finfoid%3D52597744%26pvareaid%3D108948%26cpcid%3D0%26isrecom%3D1%26queryid%3D1733402865%24B%24%2451049%243%26cartype%3D30%26cxextraparamsnew%3D%26offertype%3D0%26offertag%3D0%26activitycartype%3D1%26cstencryptinfo%3D%26encryptinfo%3D%26userareaid%3D0%26%26%26adfromid%3D29935300%26fromtag%3D0%26ext%3D%257B%2522urltype%2522%253A%2522%2522%257D",
        "position": 1,
        "isnewly": 0,
        "kindname": "普通车源",
        "usc_adid": 29935300,
        "particularactivity": 0,
        "livestatus": 0,
        "stra": "{\"biz_id\":\"52597744\",\"biz_type\":\"71\",\"exp\":\"\",\"object_p\":\"1\",\"pid\":\"101009\",\"pvid\":\"5b7858210e9a5e36d69bc7fe34ebc586\",\"query\":\"\",\"search_cate_id\":\"9\"}",
        "springid": "",
        "followcount": 1244,
        "cxctype": 0,
        "isfqtj": 0,
        "isrelivedbuy": 0,
        "photocount": 10,
        "isextwarranty": 0,
        "offertype": 0,
        "cpcinfo": {
            "adid": 0,
            "platform": 1,
            "cpctype": 0,
            "position": 0,
            "encryptinfo": ""
        },
        "displacement": "1.4T",
        "environmental": "国VI",
        "liveurl": "",
        "imuserid": "esc_b_|5355511",
        "consignment": {
            "isconsignment": 0,
            "endtime": 0,
            "imurl": "",
            "isyouxin": 0
        },
        "pv_extstr": "{\"perfectcar\":2,\"activecar\":2,\"promotecar\":2,\"cartype\":30,\"memberid\":29935300,\"cst_cartype\":0}",
        "act_discount": "",
        "cartags": {
            "p1": [],
            "p2": [],
            "p3": [],
            "p4": [
                {
                    "title": "品牌认证",
                    "bg_color": "#E5F3FF",
                    "bg_color_end": "#E5F3FF",
                    "font_color": "#0088FF",
                    "border_color": "",
                    "stype": "1",
                    "sort": 11
                },
                {
                    "title": "实车拍摄",
                    "bg_color": "#E5F3FF",
                    "bg_color_end": "#E5F3FF",
                    "font_color": "#0088FF",
                    "border_color": "",
                    "stype": "1",
                    "sort": 20
                },
                {
                    "title": "原厂质保",
                    "bg_color": "#F5F6FA",
                    "bg_color_end": "#F5F6FA",
                    "font_color": "#828CA0",
                    "border_color": "",
                    "stype": "2",
                    "sort": 25
                }
            ]
        },
        "act_tips": "",
        "showim": 0,
        "cstencryptinfo": "",
        "direct_descent_format": "",
        "flowcar": 0,
        "sameinfo": {
            "isdef": 0,
            "title": "",
            "subtitle": ""
        }
    }
    :param brand_id:
    :param series_id:
    :return:
    """
    req = get("https://api2scsou.che168.com/api/v11/search", DELAY,
              params={"_appid": "2sc.m", "pageindex": 1, "pagesize": CARS_PER_SERIES, "brandid": brand_id, "seriesid": series_id})
    if req.status_code == 200 and req.json()['message'] == "成功":
        return req.json()['result']['carlist']
    else:
        logger.critical(str(req.__dict__))
        logger.critical('failed get cars list')
        raise ConnectionError

def get_car_data(info_id):
    """
    {
    "params": {
        "首次上牌时间": [
            "2014-10-01"
        ],
        "车辆年审时间": [
            "2024-10"
        ],
        "交强险截止日期": [
            "2024-10"
        ],
        "车船使用税有效日期": [
            "-"
        ],
        "车型名称": [
            "奥迪A4L 2015款 35 TFSI 自动舒适型"
        ],
        "厂商指导价(元)": [
            "33.29万"
        ],
        "厂商": [
            "一汽奥迪"
        ],
        "级别": [
            "中型车"
        ],
        "能源类型": [
            "汽油"
        ],
        "环保标准": [
            "国IV(国V)",
            "国IV(国V)"
        ],
        "上市时间": [
            "2014.06"
        ],
        "最大功率(kW)": [
            "132",
            "132"
        ],
        "最大扭矩(N·m)": [
            "320",
            "320"
        ],
        "发动机": [
            "2.0T 180马力 L4"
        ],
        "变速箱": [
            "CVT无级变速(模拟8挡)"
        ],
        "长*宽*高(mm)": [
            "4761*1826*1439"
        ],
        "车身结构": [
            "4门5座三厢车",
            "三厢车"
        ],
        "最高车速(km/h)": [
            "230"
        ],
        "官方0-100km/h加速(s)": [
            "8.2"
        ],
        "整车质保": [
            "三年或10万公里"
        ],
        "长度(mm)": [
            "4761"
        ],
        "宽度(mm)": [
            "1826"
        ],
        "高度(mm)": [
            "1439"
        ],
        "轴距(mm)": [
            "2869"
        ],
        "前轮距(mm)": [
            "-"
        ],
        "后轮距(mm)": [
            "-"
        ],
        "满载最小离地间隙(mm)": [
            "118"
        ],
        "车门数(个)": [
            "4"
        ],
        "座位数(个)": [
            "5"
        ],
        "油箱容积(L)": [
            "65"
        ],
        "后备厢容积(L)": [
            "480"
        ],
        "整备质量(kg)": [
            "1600"
        ],
        "发动机型号": [
            "-"
        ],
        "排量(mL)": [
            "1984"
        ],
        "排量(L)": [
            "2.0"
        ],
        "进气形式": [
            "涡轮增压"
        ],
        "气缸排列形式": [
            "L"
        ],
        "气缸数(个)": [
            "4"
        ],
        "每缸气门数(个)": [
            "4"
        ],
        "配气机构": [
            "DOHC"
        ],
        "最大马力(Ps)": [
            "180"
        ],
        "最大功率转速(rpm)": [
            "4000-6000"
        ],
        "最大扭矩转速(rpm)": [
            "1500-3800"
        ],
        "发动机特有技术": [
            "AVS"
        ],
        "燃料形式": [
            "汽油"
        ],
        "燃油标号": [
            "95号"
        ],
        "供油方式": [
            "混合喷射"
        ],
        "缸盖材料": [
            "铝合金"
        ],
        "缸体材料": [
            "铸铁"
        ],
        "挡位个数": [
            "8"
        ],
        "变速箱类型": [
            "无级变速箱(CVT)"
        ],
        "简称": [
            "CVT无级变速(模拟8挡)"
        ],
        "驱动方式": [
            "前置前驱"
        ],
        "前悬架类型": [
            "五连杆独立悬架"
        ],
        "后悬架类型": [
            "梯形连杆式独立悬架"
        ],
        "助力类型": [
            "电动助力"
        ],
        "车体结构": [
            "承载式"
        ],
        "前制动器类型": [
            "通风盘式"
        ],
        "后制动器类型": [
            "盘式"
        ],
        "驻车制动类型": [
            "电子驻车"
        ],
        "前轮胎规格": [
            "225/50 R17"
        ],
        "后轮胎规格": [
            "225/50 R17"
        ],
        "备胎规格": [
            "非全尺寸"
        ],
        "主/副驾驶座安全气囊": [
            "主● / 副●"
        ],
        "前/后排侧气囊": [
            "前● / 后●"
        ],
        "前/后排头部气囊(气帘)": [
            "前● / 后●"
        ],
        "ABS防抱死": [
            "●"
        ],
        "制动力分配(EBD/CBC等)": [
            "●"
        ],
        "刹车辅助(EBA/BAS/BA等)": [
            "●"
        ],
        "牵引力控制(ASR/TCS/TRC等)": [
            "●"
        ],
        "车身稳定控制(ESC/ESP/DSC等)": [
            "●"
        ],
        "胎压监测功能": [
            "●"
        ],
        "安全带未系提醒": [
            "●"
        ],
        "ISOFIX儿童座椅接口": [
            "●"
        ],
        "车道偏离预警系统": [
            "○"
        ],
        "主动刹车/主动安全系统": [
            "-"
        ],
        "疲劳驾驶提示": [
            "-"
        ],
        "驾驶模式切换": [
            "-"
        ],
        "发动机启停技术": [
            "●"
        ],
        "自动驻车": [
            "●"
        ],
        "上坡辅助": [
            "●"
        ],
        "陡坡缓降": [
            "-"
        ],
        "可变转向比": [
            "○"
        ],
        "前/后驻车雷达": [
            "前● / 后●"
        ],
        "驾驶辅助影像": [
            "○倒车影像"
        ],
        "巡航系统": [
            "●定速巡航○自适应巡航"
        ],
        "卫星导航系统": [
            "○"
        ],
        "导航路况信息显示": [
            "-"
        ],
        "并线辅助": [
            "○"
        ],
        "车道保持辅助系统": [
            "-"
        ],
        "轮圈材质": [
            "●铝合金"
        ],
        "电动后备厢": [
            "-"
        ],
        "发动机电子防盗": [
            "●"
        ],
        "车内中控锁": [
            "●"
        ],
        "钥匙类型": [
            "●遥控钥匙"
        ],
        "无钥匙启动系统": [
            "○"
        ],
        "无钥匙进入功能": [
            "○"
        ],
        "近光灯光源": [
            "●氙气"
        ],
        "远光灯光源": [
            "●氙气"
        ],
        "LED日间行车灯": [
            "●"
        ],
        "自适应远近光": [
            "-"
        ],
        "自动头灯": [
            "●"
        ],
        "转向头灯": [
            "○"
        ],
        "车前雾灯": [
            "●"
        ],
        "大灯高度可调": [
            "●"
        ],
        "大灯清洗装置": [
            "●"
        ],
        "大灯延时关闭": [
            "-"
        ],
        "天窗类型": [
            "●电动天窗"
        ],
        "前/后电动车窗": [
            "前● / 后●"
        ],
        "车窗一键升降功能": [
            "-"
        ],
        "车窗防夹手功能": [
            "●"
        ],
        "后风挡遮阳帘": [
            "○"
        ],
        "后排侧窗遮阳帘": [
            "○"
        ],
        "车内化妆镜": [
            "●"
        ],
        "后雨刷": [
            "-"
        ],
        "感应雨刷功能": [
            "●"
        ],
        "外后视镜功能": [
            "●电动调节\n●电动折叠\n○后视镜记忆\n●后视镜加热\n○自动防眩目"
        ],
        "中控彩色屏幕": [
            "●"
        ],
        "中控屏幕尺寸": [
            "-"
        ],
        "蓝牙/车载电话": [
            "○"
        ],
        "手机互联/映射": [
            "-"
        ],
        "语音识别控制系统": [
            "-"
        ],
        "车载电视": [
            "○"
        ],
        "车载CD/DVD": [
            "●多碟CD系统○多碟DVD系统"
        ],
        "车联网": [
            "-"
        ],
        "OTA升级": [
            "-"
        ],
        "方向盘材质": [
            "●皮质"
        ],
        "方向盘位置调节": [
            "●手动上下+前后调节"
        ],
        "多功能方向盘": [
            "●"
        ],
        "方向盘换挡": [
            "○"
        ],
        "方向盘加热": [
            "-"
        ],
        "方向盘记忆": [
            "-"
        ],
        "行车电脑显示屏幕": [
            "●"
        ],
        "全液晶仪表盘": [
            "-"
        ],
        "液晶仪表尺寸": [
            "-"
        ],
        "内后视镜功能": [
            "○自动防眩目"
        ],
        "多媒体/充电接口": [
            "●AUX"
        ],
        "USB/Type-C接口数量": [
            "-"
        ],
        "手机无线充电功能": [
            "-"
        ],
        "座椅材质": [
            "●真皮"
        ],
        "主座椅调节方式": [
            "●前后调节\n●靠背调节\n●高低调节(2向)\n●高低调节(4向)"
        ],
        "副座椅调节方式": [
            "●前后调节\n●靠背调节"
        ],
        "主/副驾驶座电动调节": [
            "主● / 副●"
        ],
        "前排座椅功能": [
            "●加热○通风"
        ],
        "电动座椅记忆功能": [
            "○"
        ],
        "副驾驶位后排可调节按钮": [
            "-"
        ],
        "第二排座椅调节": [
            "-"
        ],
        "第二排座椅功能": [
            "○加热"
        ],
        "后排座椅放倒形式": [
            "●比例放倒"
        ],
        "前/后中央扶手": [
            "前● / 后●"
        ],
        "后排杯架": [
            "●"
        ],
        "扬声器品牌名称": [
            "○Bang & Olufsen"
        ],
        "扬声器数量": [
            "●10-11喇叭○≥12喇叭"
        ],
        "车内环境氛围灯": [
            "-"
        ],
        "空调温度控制方式": [
            "●自动空调"
        ],
        "后排独立空调": [
            "-"
        ],
        "后座出风口": [
            "●"
        ],
        "温度分区控制": [
            "●"
        ],
        "车载空气净化器": [
            "-"
        ],
        "车内PM2.5过滤装置": [
            "-"
        ]
    },
    "info": {
        "infoid": 52553696,
        "carname": "奥迪A4L 2015款 35 TFSI 自动舒适型",
        "brandid": 33,
        "brandname": "奥迪",
        "seriesid": 692,
        "seriesname": "奥迪A4L",
        "specid": 19488,
        "cid": 440100,
        "cname": "广州",
        "pid": 440000,
        "displacement": "2",
        "gearbox": "自动",
        "mileage": 10.5,
        "price": 6.18,
        "remark": "这辆奥迪A4L 2015款 35 TFSI 自动舒适型，首次上牌时间是2014年10月，表显里程为10.5万公里。它的亮点配置包括ISOFIX儿童座椅接口、自动驻车、定速巡航、防紫外线玻璃和后排出风口。外观漆面完美，内饰整洁，车机功能正常，空调给力，还有豪华真皮座椅，座椅柔软舒适。发动机变速箱运转良好，底盘紧凑，车况极佳，无渗油漏油。性价比极高，保值神器，支持第三方检测。这辆车非常值得您的关注，快来我们店里看看吧！",
        "vincode": "LFV3A28K4E3062639",
        "userid": 7318603,
        "dealerid": 622766,
        "transfercount": 1,
        "firstregshortdate": "2014-10-01",
        "firstregdate": "2014-10",
        "firstregyear": "2014年",
        "firstregstr": "10年3个月",
        "environmental": "国IV(国V)",
        "isloan": 0,
        "downpayment": 0,
        "haswarranty": 0,
        "isbrandcar": 0,
        "iscontainfe": 0,
        "isnewcar": 0,
        "istop": 0,
        "islatestcar": 0,
        "ad_carleveid": "4",
        "videourl": "usedcar://scheme.che168.com/smallvideo?param=%7B%22url%22%3A%22https%3A%2F%2F2sc.autohome.com.cn%2Fvideo%2Fm.html%3Finfoid%3D52553696%26pvareaid%3D107991%22%7D",
        "fromtype": 70,
        "publicdate": "2天前",
        "linktype": 3,
        "particularactivityurl": "",
        "countyname": "从化",
        "imageurl": "https://2sc2.autoimg.cn/escimg/auto/g32/M0B/24/92/400x300_c42_autohomecar__ChxkPmcbHbCADBblAAkV2CANbmI848.jpg.webp",
        "spid": 0,
        "livestatus": 0,
        "liveurl": "",
        "hqcluesurl": "usedcar://scheme.che168.com/web?param=%7B%22url%22%3A%22https%3A%2F%2Fm.che168.com%2Fhqclues%2Findex.html%3Finfoid%3D52553696%26pvareaid%3D109812%22%7D",
        "displacecount": 110245203,
        "servicetimes": 20385475,
        "followcount": 1539,
        "isreport": 0,
        "imreply": 0,
        "cxctype": 0,
        "risk": {
            "btntitle": "",
            "url": "usedcar://scheme.che168.com/web?param=%7B%22url%22%3A%22https%3A%2F%2Factivitym.che168.com%2F2022%2F2022sellcount3%2Findex%3Fvincode%3DLFV%2A%2A%2A%2A%2A%2A%2A%2A%2A%2A2639%26infoid%3D52553696%26pvareaid%3D110265%26source%3D223%26sourcename%3Dmainapp%26tabindex%3D0%22%7D"
        },
        "examine": "2024-10",
        "insurance": "2024-10",
        "colorname": "白色",
        "carusename": "家用",
        "iscpl": 0,
        "isextwarranty": 0,
        "sellcarcontent": "立享免费上门检测",
        "particularactivity": 0,
        "activityimgurl": "",
        "activityfloatimgurl": "",
        "activityurl": "",
        "popuplable": "",
        "springid": "",
        "isfanxian": 0,
        "topactivityurl": "",
        "activityofferimg": "",
        "activitycontent": "",
        "suboffertype": 0,
        "activitybodytopimgurl": "",
        "issuperfriday": 0,
        "cpcinfo": {
            "adid": 0,
            "platform": 0,
            "cpctype": 0,
            "position": 0,
            "encryptinfo": ""
        },
        "iscpd": 1,
        "template_config": {
            "imquestiontabs": [
                {
                    "title": "砍砍价格"
                },
                {
                    "title": "了解车况"
                },
                {
                    "title": "车辆报告"
                },
                {
                    "title": "咨询专家"
                },
                {
                    "title": "在线沟通"
                }
            ]
        },
        "testreporturl": "",
        "liveconnectmobile": "",
        "detselfhelpacttags": [
            {
                "acttype": 100,
                "reduceprice": 500,
                "reducecontent": "优惠直降",
                "title": "直降",
                "subtitle": "直降500元，到店后可享",
                "color": "#FF6600"
            }
        ],
        "isev": 0,
        "fuelname": "汽油",
        "batterypower": 0,
        "lifemileage": 0,
        "quickcharge": 0,
        "slowcharge": 0,
        "guidanceprice": 33.29,
        "isreporttag": 0,
        "discounttags": [
            {
                "title": "直降500元，到店后可享",
                "color": "#FF6600"
            }
        ],
        "drivingmode": "前置前驱",
        "levelname": "中型车",
        "flowmode": "涡轮增压",
        "setcount": 5,
        "question_config": [
            {
                "icon": "http://x.autoimg.cn/2sc/2022/2022-12/icon_carinfo_qa_1.png",
                "title": "此车可以异地上牌吗？",
                "more": "咨询专家",
                "linkurl": ""
            }
        ],
        "isttpcity": 1,
        "isybyq": 0,
        "is4sby": 0,
        "isjpck": 0,
        "issp": 1,
        "isyzf": 0,
        "offer_abtest": 1,
        "offer_abtest_b": 1,
        "ispopup": 1,
        "imuserid": "esc_b_|7318521",
        "userno": "",
        "vd_abtest": 0,
        "re_abtest": 0,
        "tem_isallfinsh": 1,
        "isshowvideoliving": 0,
        "mobilestatus": 0,
        "stage_tag": [],
        "cartype": 186,
        "act_tips_img_url": "",
        "acttipsjumpurl": "",
        "infoidext": "aCuXvMpAtIotlc1Tf276tQ%3D%3D",
        "dealeridext": "pimrHdLIw4c%3D",
        "accelerate": "8.2",
        "drivingnum": "-",
        "batterytype": "-",
        "batterybrand": "-",
        "ev100power": "0kWh",
        "evwarranty": "-",
        "piclist": [
            "https://2sc2.autoimg.cn/escimg/auto/g32/M0B/24/92/1024x768_c42_autohomecar__ChxkPmcbHbCADBblAAkV2CANbmI848.jpg.webp",
            "https://2sc2.autoimg.cn/escimg/auto/g32/M09/24/92/1024x768_c42_autohomecar__ChxkPmcbHbGATEpmAAgOyHBmc2o393.jpg.webp",
            "https://2sc2.autoimg.cn/escimg/auto/g32/M03/24/92/1024x768_c42_autohomecar__ChxkPmcbHbGAMxOmAAp-hwjJKzs929.jpg.webp",
        ],
        "taxi_voucher_status": 0,
        "is_work_time": 1
    }
}
    :param info_id:
    :return:
    """
    car_data = {
        "params": {},
        "info": {}
    }
    req = get("https://apiuscdt.che168.com/api/v1/car/getparamtypeitems", DELAY,
              params={"_appid": "2sc.m", "infoid": info_id})
    if req.status_code == 200 and req.json()['message'] == "成功":
        for cat in req.json()['result']:
            for desc in cat['data']:
                desc_name = desc['name']
                if (cat['title'] == "基本参数" and desc_name == "车身结构") or desc['content'] == '-':
                    continue
                car_data['params'][desc_name] = car_data['params'].get(desc_name, [])
                car_data['params'][desc_name].append(desc['content'])
    else:
        logger.critical(str(req.__dict__))
        logger.critical('failed get car data params')
        raise ConnectionError
    req = get("https://apiuscdt.che168.com/apic/v2/car/getcarinfo", DELAY,
              params={"_appid": "2sc.m", "infoid": info_id})
    if req.status_code == 200 and req.json()['message'] == "成功":
        car_data['info'] = req.json()['result']
    else:
        logger.critical(str(req.__dict__))
        logger.critical('failed get car data info')
        raise ConnectionError
    return car_data

def parse():
    brands_data = get_brands_data()
    for brand, brand_ch in brands_en_ch.items():
        if brand_ch not in brands_data:
            logger.warning(f"{brand} / {brand_ch} not found")
    parsed_brands = pandas.read_excel(DATA_FILES['che168'], sheet_name='cars')['mark'].unique() if os.path.exists(DATA_FILES['che168']) else []
    for i, (brand, brand_info) in enumerate(brands_data.items()):
        if brand_info['name_en'] in parsed_brands:
            logger.info(f"{brand_info['name_en']} already parsed")
            continue
        data = {
            "cars": [],
            "images": []
        }
        series_data = get_series_data(brand_info['bid'])
        for ii, (series, series_info) in enumerate(series_data.items()):
            cars_list = get_cars_list(brand_info['bid'], series_info['sid'])
            for iii, car in enumerate(cars_list):
                logger.info(f"Brand {brand_info['name_en']} {i+1}/{len(brands_data)} "
                            f"Series {series} {ii+1}/{len(series_data)} "
                            f"Car {iii+1}/{len(cars_list)}")

                reg_year = int(car["firstregyear"].strip("年")) if car["firstregyear"] != '未上牌' else 'unlicensed'
                if reg_year != 'unlicensed' and reg_year < MIN_REG_YEAR:
                    logger.debug(f'Skip <{MIN_REG_YEAR}y data id: {car['infoid']}')
                    continue

                car_data = get_car_data(car['infoid'])

                fuel = car_data['info']["fuelname"]
                if not fuel:
                    logger.warning(f'Get empty car data / id: {car['infoid']}')
                    continue

                if reg_year == 'unlicensed':
                    reg_year = int(car_data['params']['上市时间'][0].split('.')[0])
                    if reg_year < MIN_REG_YEAR:
                        logger.debug(f'Skip <{MIN_REG_YEAR}y data id: {car['infoid']}')
                        continue
                    elif reg_year > 2030:
                        logger.critical(f"wrong reg year: {reg_year} id: {car['infoid']}")
                        raise ValueError

                if check := any(attr in car_data['params'] and len(car_data['params'][attr]) != 1 for attr in ["最大马力(Ps)", "电动机(Ps)", "系统综合功率(Ps)", "排量(L)", "车身结构"]):
                    logger.critical(f"!=1 {check} id: {car['infoid']}")
                    raise ValueError

                car_params = {
                    "id": car['infoid'],
                    "name": car['carname'],
                    "mark": brand_info['name_en'],
                    "model": series_info['name'],
                    "price": int(float(car['price']) * 10000),
                    "date": reg_year,
                    "mileage": int(float(car["mileage"]) * 10000),
                    "color": car_data['info']['colorname'],
                    "wd": car_data['info']['drivingmode'],
                    "volume": float(car_data['params']["排量(L)"][0]) if "排量(L)" in car_data['params'] else numpy.nan,
                    "engine_power": int(car_data['params']["最大马力(Ps)"][0]) if "最大马力(Ps)" in car_data['params'] else numpy.nan,
                    "electric_power": int(car_data['params']["电动机(Ps)"][0]) if "电动机(Ps)" in car_data['params'] else numpy.nan,
                    "comprehensive_power": int(car_data['params']["系统综合功率(Ps)"][0]) if "系统综合功率(Ps)" in car_data['params'] else numpy.nan,
                    "fuel": fuel,
                    "fuelcons": numpy.nan,
                    "trans": car_data['info']["gearbox"],
                    "bdwk": car_data['params']["车身结构"][0] if "车身结构" in car_data['params'] else numpy.nan,
                    "about": car_data['info']['remark'],
                    "url": f"https://www.che168.com/dealer/{car_data['info']['dealerid']}/{car['infoid']}.html"
                }
                car_images = [{
                    'car_id': car['infoid'],
                    'img': pic
                } for pic in car_data['info']["piclist"]]
                data['cars'].append(car_params)
                data['images'].extend(car_images)
        dump_parsed_data(data, DATA_FILES["che168"])

def main():
    logger.info(f"Parsing")
    parse()
    logger.info(f"Translating")
    translator = myTranslator()
    translator.translate_excel("che168", 'cars', TRANSLATE_CATS)
    translator.translate_excel("che168", 'images', {})
    logger.info(f"to sql")
    to_sql("che168")
    os.rename(DATA_FILES['che168'], f"{DATA_FILES['che168'].replace(".xlsx", '')}_{int(time.time())}.xlsx")
    os.remove(DATA_FILES['translated_che168'])
    logger.info('DONE')

if __name__ == "__main__":
    while True:
        start_datetime = datetime.datetime.now()
        main()
        while start_datetime + datetime.timedelta(days=PARSING_DELAY_DAYS) > datetime.datetime.now():
            time_to_wait = start_datetime + datetime.timedelta(days=PARSING_DELAY_DAYS) - datetime.datetime.now()
            logger.info(f"Sleepin for {time_to_wait.days} days & {time_to_wait.seconds // 60 // 60 % 24} hours")
            time.sleep(min(60 * 60, time_to_wait.seconds))
