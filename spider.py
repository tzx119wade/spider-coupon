#-*-coding:utf-8-*-2
from bs4 import BeautifulSoup
import requests
from datetime import datetime
import re
from pymongo import MongoClient
import random
import time

from apscheduler.schedulers.background import BackgroundScheduler

# class spider
class CouponSpider(object):
	def __init__(self):
		super(CouponSpider, self).__init__()
		# 以当前时间为参数 设置request_url
		date_now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
		list_date = date_now.split(' ')
		string_date = list_date[0]
		string_h = list_date[1].split(':')[0]
		string_m = list_date[1].split(':')[1]
		string_s = list_date[1].split(':')[2]

		self.request_url = 'http://m.huim.com/ajax/GetIndex?id={}+{}%3A{}%3A{}&tags='.format(string_date,string_h,string_m,string_s)

	def start_spider(self):
		# 首先建立数据库连接
		host = '127.0.0.1'
		port = 27017
		client = MongoClient(host,port)
		coupon_db = client['coupon_db']
		coupon = coupon_db['coupon']

		# 获取当前时间参数
		date_now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
		date_now = date_now.split(' ')[0]

		# 请求
		try:
			resp = requests.get(self.request_url)
		except :
			print ('请求失败')
			time.sleep(2)
			resp = requests.get(self.request_url)
			soup = BeautifulSoup(resp.text, 'lxml')
		else:
			soup = BeautifulSoup(resp.text,'lxml')

		all_tag_a = soup.find_all('a')

		# 验证data-url是否还有pid参数
		for a in all_tag_a:
			if 'pid' in a['data-url']:
				# 验证activityId 是否重复
				matchObj = re.search(r'activityId=(\w+)&pid',a['data-url'])
				activityId = matchObj.group(1)
				has_activityId = coupon.find_one({ 'activityId':activityId })
				# 如果没有重复则存储这条信息
				if has_activityId == None:
					# 处理pid
					coupon_url = self.handle_pid(a['data-url'])
					# 券后价
					cut_price = a['data-price']
					if re.search(r'\.',cut_price):
						cut_price = float(cut_price)
					else:
						cut_price = int(cut_price)
					# 商品名
					name = a['data-title']
					# 获取优惠券价格
					tag_p = a.find('p',class_='goods_coupon')
					coupon_price = tag_p.find_all('span')[1].text
					# 商品图片
					img = ''
					if a.find('img').has_attr('data-original'):
						img = a.find('img')['data-original']
					else:
						img = a.find('img')['src']
					# 领取人数
					tag_p = a.find('p',class_='goods_num')
					num_receive = tag_p.find_all('span')[1].text
					matchobj = re.match(r'\d+',num_receive)
					num_receive = matchobj.group()
					# 存储
					coupon_item = {
						'name':name,
						'activityId':activityId,
						'coupon_url':coupon_url,
						'cut_price':cut_price,
						'coupon_price':coupon_price,
						'img':img,
						'num_receive':int(num_receive),
						'created_date':datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
					}
					coupon.insert_one(coupon_item)

	def handle_pid(self,url):
		pattern = re.compile(r'mm.+&i')
		replace_string = 'mm_40786416_22002092_73348373&i'
		result = re.sub(pattern, replace_string, url)
		return result;


def coupon_spider_job():
	spider = CouponSpider()
	spider.start_spider()
	print ('== 爬取完成 {}=='.format(datetime.now().strftime('%Y-%m-%d %H:%M:%S')))

scheduler = BackgroundScheduler()
scheduler.add_job(coupon_spider_job, 'interval', minutes=5)
scheduler.start()


