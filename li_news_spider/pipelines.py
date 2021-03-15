# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface

import pymysql,time,requests,hashlib
from ftplib import FTP
from io import BytesIO




class LiNewsSpiderPipeline:
    def open_spider(self, spider):
        #mysql------------------------------------------
        self.conn = pymysql.Connect(
            host='154.212.112.247',
            port=13006,
            # 数据库名：
            db='test',
            user="root",
            passwd='itfkgsbxf3nyw6s1',
            charset='utf8')
        self.cur = self.conn.cursor()
        #ftp---------------------------------------------
        self.ftp = FTP()
        self.ftp.connect('154.86.175.226', 21)
        self.ftp.login(user='img', passwd='W2BpLPnyXbdmWCNd')
        self.ftp.encoding = 'utf-8'
        self.headers={'User-Agent':'Mozilla/5.0 (Linux; Android 7.0; SM-G930V Build/NRD90M) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.125 Mobile Safari/537.36',
             'Accept':'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
             'Accept-Language': 'zh-CN,zh;q=0.9',}
        sess = requests.Session()
        sess.keep_alive = False  # 关闭多余连接
    def process_item(self, item, spider):
        find_ex="select id,PageUrl from Data_Content_665 where title= %s "
        self.cur.execute(find_ex, (item["title"]))
        try:
            if not self.cur.fetchone():
                img_path=self.img_parse(item['img_src'],item['keyword'])
                news="insert into Data_Content_665(title,content,author,time,keywords,description,tag,PageUrl,thumbid,category,create_time,be_from) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                self.cur.execute(news,(item['title'],img_path+'\n'+'&nbsp;'*8+item['content'],item['author'],item['release_time'],item['keyword'],item['description'],item['keyword'],item['url'],img_path,item['category'],time.time(),item['be_from']))
                #self.conn.commit()
                return item
        except Exception as e:
            print(e)
    def img_parse(self,img_src,keyword):
        if img_src == '':
            return '<img src="https://img.ksyoume.cn/img_upload/95e3325e00471c3a0705d42e406d69a3.jpg" alt="" >'
        else:
            time_name=time.strftime("%Y-%m-%d", time.localtime(time.time())).replace('-', '')
            try:self.ftp.mkd(time_name)
            except Exception:pass
            try:
                img_data=requests.get(img_src,timeout=30,headers=self.headers).content
                base_txt_name = img_src + str(time.time())
                m = hashlib.md5()
                m.update(base_txt_name.encode('utf-8'))
                base_txtname_md5 = m.hexdigest() + '.jpg'
                ftp_path=time_name+'/'+base_txtname_md5
                self.ftp.storbinary(cmd="STOR %s" % ftp_path,fp=BytesIO(img_data))
                return '<img src="https://img.ksyoume.cn/img_upload/'+ftp_path+'" alt="{}"'.format(keyword)+' />'
            except Exception as e:
                print(e)
                return '<img src="https://img.ksyoume.cn/img_upload/95e3325e00471c3a0705d42e406d69a3.jpg" alt="" >'

    def close_spider(self, spider):
        #self.conn.commit()
        self.conn.close()