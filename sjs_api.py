# -*- coding:utf-8 -*-
import os
import re
import time
import requests
import pandas as pd
from pandas import DataFrame
from itertools import cycle
from sjs_service import SjsService
import json

class SjsApi(object):
    """ 处理系统与神箭手API之间的参数传递
        拓展sjs_api
    """
    
    def __init__(self,user_key=None,user_mi_key=None,spider_id=0):
        self.sjs = SjsService(user_key,user_mi_key,spider_id)
        self.timestamp = str(int(time.time()))
        self.sess = requests.Session()
        
    def modify_crawler(self,formdata):
        """ 修改爬虫设置
            formdata -- 自定义爬虫的参数，字符串
        """
        postdata = {
            item['name']:item['defaultvalue'] 
                if item['type'] not in ['tags','array']
                else str(re.split(',|\n',item['defaultvalue'].strip())).replace('\'','"')
            for item in json.loads(formdata)
        }   #处理参数
        return self.sjs.modify_spider(postdata)
        
    def start_crawler(self):
        return self.sjs.start_spider()
        
    def pause_crawler(self):
        return self.sjs.pause_spider()
    
    def resume_crawler(self):
        return self.sjs.resume_spider()
        
    def stop_crawler(self):
        return self.sjs.stop_spider()
    
    def get_crawler_status(self):
        """ 0	已停止
            1	正在运行
            2	爬虫刚初始化完成, 即爬虫刚创建, 还未改过配置
            3	未启动
            4	出现异常
            5	正在停止
            6	正在启动
            7	已删除
            13	定时休息中
            14	正在暂停
            15	已暂停
            16	正在从暂停中恢复
        """
        return self.sjs.get_spider_status()['data']['crawler_status']
        
    def get_data_count(self):
        return self.sjs.query_data('source{count}')['result']['count']
    
    def delete_crawler(self):
        return self.sjs.delete_spider()
    
    def change_node(self,node_delta=0):
        """修改爬虫节点
        
        node_delta -- 修改数，int，正数增加，负数减少，0不变
        """
        return self.sjs.change_node(node_delta)
        
    def get_left_node(self):
        """ 执行修改节点函数返回left_node数据
            执行这个函数实际上节点计数已经减少了一个
        """
        return self.sjs.change_node(1)['data']['left_node']
        
    def emptied_data_and_start_crawler_by_all_node(self, \
                        fromdata,sjs_loginname,sjs_loginpwd):
        """ 运行全部节点启动爬虫
            访问过快有限制，适当暂停1s   
            fromdata -- 自定义爬虫的参数，字符串
        """
        self.emptied_data(sjs_loginname,sjs_loginpwd)#清空数据
        self.modify_crawler(fromdata)
        time.sleep(1)
        self.start_crawler()
        time.sleep(1)
        left_node = self.get_left_node()
        time.sleep(1)
        self.change_node(left_node)
        time.sleep(1)

    def export_data(self,filename,cols={},split_cols=['params']):
        """导出全部数据"""
        dir_path = '.\\temp'
        filename = os.path.join(dir_path, filename)
        try: os.remove(filename)    #尝试删除文件
        except: pass
        query_str = 'source%s{data{},page_info{has_next_page,end_cursor}}'
        df = DataFrame()
        # 防止神箭手导出时出错，限定按500条/次循环查询
        for i,lmt in enumerate(cycle([500])):  #遍历序列的（序号，值）
            if i == 0:  #第一次没有限定__id
                query = query_str % ('(limit:%s)' % lmt)
            else:   #第二次开始需要限定__id,即查询后的end_cursor字段
                query = query_str % (
                            '(__id:{lt:%s},limit:%s)' % (end_cursor, lmt))
            data = self.sjs.query_data(query)
            data_info = data['result']['data']
            
            if data['code'] == 0 and data_info:
                df = pd.concat([df, DataFrame(data_info)])
                end_cursor = data['result']['page_info']['end_cursor']
            else:
                break   #查询失败是即退出循环 
        self._deal_excel(df,filename,cols,split_cols)
    
    def emptied_data(self,user,password):
        """ 清空爬虫数据
            user，password -- 神箭手的账号密码，str
        """
        fromdata = {'name_login':user, 'password':password}
        self.sess.post('http://www.shenjianshou.cn/index.php?r=sign/inAjax', data=fromdata)
        self.sess.post('http://www.shenjianshou.cn/index.php?r=source/dataClear&app_id={}'.format(self.sjs.spider_id))
    
    def export_data(self,filename,cols,db,Tasklist,taskid):
        """导出全部数据并清空"""
        cols = json.loads(cols)
        self.export_data(filename,cols)
        task = Tasklist.query.get_or_404(taskid)
        task.status = '已停止'
        task.endtime = int(time.time())
        task.filename = filename
        db.session.add(task)
        db.session.commit()
    
    def _deal_excel(self,dataframe,filename,cols,split_cols):
        """ 按需分割成多列，替换中文表头，去掉excel中非空值低于10%的列
            dataframe -- pandas的DataFrame对象
            filename -- 保存excel的文件名
            cols -- 列名字典，对应的中文
            split_cols -- 需要分列数据的列名
        """
        df = dataframe
        df = dataframe
        for col in split_cols:
            for i,item in df[col].items():
                try:
                    for params in eval(str(item)):
                        name = params['label']
                        value = params.get('value')
                        df.ix[i,name] = value if value else ';'.join(
                                            [i['desc'] for i in params['values']])
                except: pass
            else: del(df[col])
        if cols:
            df.columns = [cols.get(x,x) for x in df.columns]
        for idx in df.columns:
            if len(df[idx].dropna())/len(df[idx]) < 0.1:
                del(df[idx])
        writer = pd.ExcelWriter(filename,engine='xlsxwriter',options={'strings_to_urls':False})
        df.to_excel(writer, index=False)
        writer.close()
    
if __name__ == '__main__':
    """测试"""
    user_key = 'xxx'
    user_mi_key = 'xxx'
    spider_id = '521648'
    sjs_api = SjsApi(user_key,user_mi_key,spider_id)
    sjs_api.export_data('2018.xlsx')



