# -*- coding:utf-8 -*-
import hashlib
import time
import json
import requests
from urllib import request

class SjsService(object):
    """ 神箭手官方RESTful爬虫接口
        http://docs.shenjian.io/develop/platform/restful/crawler.html
        python实现
    """
    
    def __init__(self,user_key,user_mi_key,spider_id):
        self.user_key = user_key
        self.user_mi_key = user_mi_key
        self.spider_id = spider_id
        self.sess = requests.Session()
        self.headers = {
            'Host': 'www.shenjianshou.cn',
            'Content-Type': 'application/x-www-form-urlencoded'
        }
        self.source_id = '' if int(spider_id) == 0 else \
                            self.get_spider_status()['data']['source_id']
        
    def _request_url(self,url,postdata={}):
        """传入url带上时间戳和签名调用神箭手api"""
        timestamp = str(int(time.time()))
        tmp_sign = self.user_key + timestamp + self.user_mi_key
        sign = hashlib.md5(tmp_sign.encode('utf-8')).hexdigest()
        url = url.format(timestamp, sign)
        for i in range(10): # 最多循环十次
            time.sleep(1)
            if postdata:
                r=self.sess.post(url, headers=self.headers, data=postdata)
            else:
                r=self.sess.get(url)
            resp = json.loads(r.text)
                
            # 执行query_data返回状态码key为‘code’
            if int(resp.get('error_code', resp.get('code',-1))) == 0:
                return resp
            time.sleep(1)
    
    def modify_spider(self,postdata):
        """修改爬虫设置"""
        url = 'http://www.shenjianshou.cn/rest/crawler/config?\
                user_key=%s&timestamp={}&sign={}&\
                crawler_id=%s' % (self.user_key, self.spider_id)
        return self._request_url(url, postdata)
    
    def start_spider(self,node_num=1,postdata={}):
        """
        定时启动爬虫
        可设置node_num节点数
        可设置postdata参数定时启动爬虫
        """
        url = 'http://www.shenjianshou.cn/rest/crawler/start?\
                user_key=%s&timestamp={}&sign={}&crawler_id=%s&\
                node=%s' % (self.user_key, self.spider_id, node_num)
        return self._request_url(url, postdata)
        
    def stop_spider(self):
        """停止爬虫"""
        url = 'http://www.shenjianshou.cn/rest/crawler/stop?\
                user_key=%s&timestamp={}&sign={}&\
                crawler_id=%s' % (self.user_key, self.spider_id)
        return self._request_url(url)
    
    def get_spider_status(self):
        """获取爬虫状态"""
        url = 'http://www.shenjianshou.cn/rest/crawler/status?\
                user_key=%s&timestamp={}&sign={}&\
                crawler_id=%s' % (self.user_key, self.spider_id)
        return self._request_url(url)
        
    def delete_spider(self):
        """删除爬虫"""
        url = 'http://www.shenjianshou.cn/rest/crawler/delete?\
                user_key=%s&timestamp={}&sign={}&\
                crawler_id=%s' % (self.user_key, self.spider_id)
        return self._request_url(url)
    
    def pause_spider(self):
        """暂停爬虫"""
        url = 'http://www.shenjianshou.cn/rest/crawler/pause?\
                user_key=%s&timestamp={}&sign={}&\
                crawler_id=%s' % (self.user_key, self.spider_id)
        return self._request_url(url)
        
    def resume_spider(self):
        """继续爬虫"""
        url = 'http://www.shenjianshou.cn/rest/crawler/resume?\
                user_key=%s&timestamp={}&sign={}&\
                crawler_id=%s' % (self.user_key, self.spider_id)
        return self._request_url(url)
        
    def change_node(self,node_delta=0):
        """修改爬虫节点"""
        url = 'http://www.shenjianshou.cn/rest/crawler/changeNode?\
                user_key=%s&timestamp={}&sign={}&crawler_id=%s&\
                node_delta=%s' % (self.user_key,self.spider_id,node_delta)
        return self._request_url(url)
        
    def get_spider_speed(self):
        """获取爬虫速率"""
        url = 'http://www.shenjianshou.cn/rest/crawler/speed?\
                user_key=%s&timestamp={}&sign={}&\
                crawler_id=%s' % (self.user_key, self.spider_id)
        return self._request_url(url)
    
    def query_data(self,query='source{}'):
        """获取查询数据，返回字典格式"""
        url = 'http://graphql.shenjian.io/?user_key=%s&\
                timestamp={}&sign={}&source_id=%s&\
                query=%s' % (self.user_key,self.source_id,request.quote(query))
        return self._request_url(url)

if __name__ == '__main__':
    """测试"""
    user_key = 'xxx'
    user_mi_key = 'xxx'
    spider_id = '819772'
    timing_postdata ="""[{"label":"","name":"crawlType","type":"select","defaultvalue":"按店铺","display":"false","options":[]},{"label":"","name":"crawlKeywordType","type":"select","defaultvalue":"输入关键字","display":"false","options":[]},{"label":"要爬取的淘宝店铺链接","name":"shopUrls","type":"array","defaultvalue":"[\"https:\\/\\/shop130702226.taobao.com\\/?spm=a230r.7195193.1997079397.2.fBQv9i\"]","display":"false","options":[]},{"label":"","name":"crawlStoresCountAndProductPV","type":"checkbox","defaultvalue":"true","display":"false","options":[]}]"""
    sjs_api = SjsService(user_key,user_mi_key,spider_id)
    
    print(sjs_api.modify_spider(timing_postdata))




















