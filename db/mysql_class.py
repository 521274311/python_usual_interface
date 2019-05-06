import pymysql
import datetime
import time
class MysqlTools:
    '''
    MYSQL 工具类
    所有操作均以事务形式完成，增删改查默认方式下不开启自动提交事务
    '''
    # 基本数据库配置
    __HOST = 'xxx.xxx.xxx.xxx'  # 配置host
    __PORT = 3306  # 配置port
    __USERNAME = 'xxxx'  # 配置username
    __PASSWORD = 'xxxx'  # 配置password
    __DB = 'xxxx'  # 配置数据库

    # 其他服务配置
    conn = ''
    __RE_BACK = 3   # 失败重试次数
    __RE_BACK_TIME = 10 # 失败重试时间间隔
    __IS_AUTO_COMMIT = False # 是否开启自动提交事务（True开启，False不开启）
    __IS_DEBUG = False # 是否开启输出sql信息模式（True开启，False不开启）

    def __init__(self):
        self.conn = pymysql.connect(host=self.__HOST,port=int(self.__PORT),user=self.__USERNAME,passwd=self.__PASSWORD,db=self.__DB)

    def insert_json(self,table_name,data):
        '''
        字典类型入库，将key作为字段，value作为值插入数据库
        :param table_name: 表名
        :param data: 字典类型数据
        :return: int类型，:1 成功，0 失败
        '''
        re_back = 0
        while re_back < self.__RE_BACK:
            try:
                insert_sql = 'insert into '+table_name+'('
                value_sql = ' values('
                begin = True
                for key in data.keys():
                    if begin == True:
                        begin = False
                    else:
                        insert_sql += ','
                        value_sql += ','

                    insert_sql += '`'+key+'`'

                    if type(data[key]) == bool:
                        data[key] = 1 if data[key] == True else 0
                    if type(data[key]) == str or type(data[key]) == list \
                            or type(data[key]) == set or type(data[key]) == dict:
                        value_sql += "'"+str(data[key]).replace("'","\\'") + "'"
                    else:
                        value_sql += str(data[key] if data[key] != None else "'None'")

                sql = insert_sql + ')' + value_sql +')'

                self.execute(sql)
                if self.__IS_AUTO_COMMIT:
                    self.conn.commit()
                return 1
            except:
                re_back += 1
                if self.__IS_DEBUG:
                    print('insert_json '+str(re_back)+' fail,error info:'+str(e))
                time.sleep(self.__RE_BACK_TIME)
        return 0

    def query_json(self,sql):
        '''
        查询mysql数据以列表类型返回，列表中的每个元素为字典类型，其中字典的key为字段，value为该字段某行对应的值
        :param sql: 查询sql语句
        :return: 列表类型 或 整数0，列表类型查询成功，0 查询失败
        '''
        re_back = 0
        while re_back < self.__RE_BACK:
            try:
                cursor = self.conn.cursor()

                if self.__IS_DEBUG:
                    print(sql)

                cursor.execute(sql)
                fields = cursor.description
                data = cursor.fetchall()
                res = []
                for dt in data:
                    i = 0
                    rs = {}
                    for field in fields:
                        rs[field[0]] = ((dt[i] if type(dt[i])!= datetime.datetime
                            else str(dt[i])) if type(dt[i]) != datetime.date else str(dt[i]))
                        i += 1
                    res.append(rs)

                if self.__IS_AUTO_COMMIT:
                    self.conn.commit()

                return res
            except Exception as e:
                re_back += 1
                if self.__IS_DEBUG:
                    print('query_json '+str(re_back)+' fail,error info:'+str(e))
                time.sleep(self.__RE_BACK_TIME)
        return 0

    def execute(self,sql):
        '''
        sql 增删改处理方法
        '''
        re_back = 0
        while re_back < self.__RE_BACK:
            try:

                if self.__IS_DEBUG:
                    print(sql)
                cursor = self.conn.cursor()
                res = cursor.execute(sql)

                if self.__IS_AUTO_COMMIT:
                    self.conn.commit()
                return res
            except:
                re_back += 1
                if self.__IS_DEBUG:
                    print('execute '+str(re_back)+' fail,error info:'+str(e))
                time.sleep(self.__RE_BACK_TIME)
        return 0

    def commit(self):
        '''
        提交事务
        '''
        re_back = 0
        while re_back < self.__RE_BACK:
            try:
                self.conn.commit()
                return 1
            except:
                re_back += 1
                if self.__IS_DEBUG:
                    print('commit '+str(re_back)+' fail,error info:'+str(e))
                time.sleep(self.__RE_BACK_TIME)
        return 0

    def query(self,sql):
        '''
        普通sql查询，返回二维元组
        '''
        re_back = 0
        while re_back < self.__RE_BACK:
            try:
                cursor = self.conn.cursor()

                if self.__IS_DEBUG:
                    print(sql)

                cursor.execute(sql)

                if self.__IS_AUTO_COMMIT:
                    self.conn.commit()

                list = cursor.fetchall()
                return list
            except:
                re_back += 1
                if self.__IS_DEBUG:
                    print('query '+str(re_back)+' fail,error info:'+str(e))
                time.sleep(self.__RE_BACK_TIME)
        return 0

if __name__ == '__main__':
    #demo
    mysql = MysqlTools()
    data = {
        'key1' : 'value1',
        'key2' : 2,
    }
    result = mysql.insert_json('table_name',data)

