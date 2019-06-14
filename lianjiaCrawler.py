import requests
import threading
from lxml import etree
from pymongo import MongoClient


# 处理URL并得到HTML文本
def get_page(c, base_url):
    if c == 1:
        url = base_url
    else:
        url = base_url + "pg" + str(c)
    print(url)
    try:
        response = requests.get(url=url, headers=headers)
        if response.status_code == 200:
            status = parse_page(c, response.text)
            if status == 'break':
                return 'break'
    except requests.ConnectionError as e:
        print('Error', e.args)


def parse_page(c, html):  # 解析提取信息
    tree = etree.HTML(html)
    per_page_items = tree.xpath('//ul[@class ="sellListContent"]/li[@class="clear LOGCLICKDATA"]')
    count = tree.xpath('//div[@class ="resultDes clear"]//h2//span//text()')
    print(count)
    print('count', count[0].strip())
    if (c-1) * 30 > int(count[0].strip()):
        return "break"

    length = len(per_page_items)
    if length > 0:
        for i in range(1, length + 1):
            # 筛数据
            title = tree.xpath(
                '//ul[@class ="sellListContent"]/li[@class="clear LOGCLICKDATA"]'
                '[{}]//div[@class="title"]//a'.format(i))
            totalprice = tree.xpath(
                '//ul[@class ="sellListContent"]/li[@class="clear LOGCLICKDATA"]'
                '[{}]//div[@class="priceInfo"]//div[@class="totalPrice"]//span'.format(
                    i))
            unitprice = tree.xpath(
                '//ul[@class ="sellListContent"]/li[@class="clear LOGCLICKDATA"]'
                '[{}]//div[@class="priceInfo"]//div[@class="unitPrice"]//span'.format(
                    i))
            address = tree.xpath(
                '//ul[@class ="sellListContent"]/li[@class="clear LOGCLICKDATA"]'
                '[{}]//div[@class="address"]//div[@class="houseInfo"]//text()'.format(
                    i))
            flood = tree.xpath(
                '//ul[@class ="sellListContent"]/li[@class="clear LOGCLICKDATA"]'
                '[{}]//div[@class="flood"]//div[@class="positionInfo"]//text()'.format(
                    i))

            ershoufang_dict = {}
            ershoufang_dict['title'] = title[0].text
            ershoufang_dict['address'] = address[0]  # 例：佳兆业可园四期
            ershoufang_dict['info'] = address[1]  # 例： | 3室2厅 | 70.36平米 | 东北 | 精装 | 有电梯
            ershoufang_dict['positoninfo'] = flood[0]  # 例： 中楼层(共18层)2006年建板楼  -
            ershoufang_dict['position'] = flood[1]  # 例： 布吉关
            ershoufang_dict['totalprice'] = totalprice[0].text
            ershoufang_dict['unitprice'] = unitprice[0].text
            save_to_mongo(ershoufang_dict, i, c)  # c：当前页, i:当前节点


def save_to_mongo(obj, index, c):  # 保存提取的数据到数据库
    print("第{}页正在存取第{}条数据".format(c, index))
    collection.update({'title': obj['title']}, {'$setOnInsert': obj}, upsert=True)


def get_district_kv(url):
    district_list = []
    district_response = requests.get(url=url, headers=headers)
    if district_response.status_code == 200:
        district_tree = etree.HTML(district_response.text)
        district_items_path = district_tree.xpath('//div[@data-role="ershoufang"]/div/a/@href')
        district_items_name = district_tree.xpath('//div[@data-role="ershoufang"]/div/a/text()')
        district_length = len(district_items_path)
        print('城区数量为：', district_length)
        print('城区包含如下：', district_items_name)
        if district_length > 0:
            for i in range(0, district_length):
                urban_dict = {}
                urban_dict['key'] = district_items_path[i].split('/')[2]
                urban_dict['value'] = district_items_name[i]
                district_list.append(urban_dict)

    return district_list


def get_street_kv(district):
    street_list = []
    district_url = base_url + district['key'] + '/'
    response = requests.get(url=district_url, headers=headers)
    if response.status_code == 200:
        tree = etree.HTML(response.text)
        street_items_path = tree.xpath('//div[@data-role="ershoufang"]/div[2]/a/@href')
        street_items_name = tree.xpath('//div[@data-role="ershoufang"]/div[2]/a/text()')
        street_length = len(street_items_path)
        print(district['value'], '含街道数: ', street_length)
        print('具体如下：', street_items_name)
        if street_length > 0:
            for j in range(0, street_length):
                street_dict = {}
                street_dict['key'] = street_items_path[j].split('/')[2]
                street_dict['value'] = street_items_name[j]
                street_list.append(street_dict)

    return street_list

# 等分列表,方便后续多线程


def div_list(ls, num):
    ls_len = len(ls)
    if num <= 0 or 0 == ls_len:
        return []

    elif num == ls_len:
        return [[i] for i in ls]

    elif num > ls_len:
        return ls

    else:
        j = ls_len // num  # 26 / 5 ,j=5
        ls_return = []
        # 步长j,次数num-1
        for i in range(0, (num - 1) * j, j):
            ls_return.append(ls[i:i + j])
        # 算上末尾的j+k
        ls_return.append(ls[(num - 1) * j:])
        return ls_return


def my_thread(slice_list):
    for p in slice_list:
        # url路径形如: /,pg2/,pg3/ ...
        for thread_idx in range(1, 101):
            status = get_page(thread_idx, base_url + p + '/')
            if status == 'break':
                break


def init(i, slice_list):
    print(slice_list)
    t1 = threading.Thread(target=my_thread, name=i, args=(slice_list,))
    t1.start()


if __name__ == '__main__':

    value = input("请输入想查询的城市名称: 例：上海  ")
    key = input("请输入想查询的城市名称各汉字拼音首字母(小写): 例：sh  ")

    client = MongoClient(host='localhost', port=27017)  # 创建MongoDB的连接对象
    db = client.lianjia  # 指定要使用的数据库

    headers = {
        'Host': key+'.lianjia.com',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                      'Chrome/71.0.3578.98 Safari/537.36',
    }  # 请求头伪装

    # 以下针对二手房

    base_url = "https://"+key+".lianjia.com/ershoufang/"
    collection = db[value+"二手房"]  # 指定要操作的集合（集合类似与关系数据库中的表）
    print('从以下网址获取城区名：', base_url)

    # 以下获取某城市区
    current_city_district_list = get_district_kv(base_url)
    for idx, district in enumerate(current_city_district_list):
        # 以下获取街道
        street_list = get_street_kv(district)
        district['children'] = street_list

    # print(current_city_district_list)

    # 以下得到该城市所有划分街道
    street_key_list = []

    for idx, district in enumerate(current_city_district_list):
        for street in district['children']:
            street_key_list.append(street['key'])
    print('该城市街道数：', len(street_key_list))

    num = input("请输入线程数: (建议线程数为街道数公因数) ")
    after_segmentation_list = div_list(street_key_list, int(num))
    for x in range(len(after_segmentation_list)):
        init(x, after_segmentation_list[x])

