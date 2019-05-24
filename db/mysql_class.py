import pymysql
import datetime
import time


def check_is_error(fn):
    '''
    检测程序是否出错，并重试
    :param fn:
    :return:
    '''

    def run_check(self,*args, **kwargs):
        re_back = 0
        while re_back < self.conf['RE_BACK']:
            try:
                ans = fn(self,*args, **kwargs)
                if self.conf['IS_AUTO_COMMIT']:
                    self.conn.commit()
                return ans
            except Exception as e:
                re_back += 1
                if self.conf['IS_DEBUG']:
                    print('The program  run failure,count '+str(re_back)+' times,error info:' + str(e))
                time.sleep(self.conf['RE_BACK_TIME'])

        if self.conf['IS_DEBUG']:
            print('The program run failure end..Will return zero.')
        return 0
    return run_check

class MysqlTools:
    '''
    MYSQL 工具类
    所有操作均以事务形式完成，增删改查默认方式下不开启自动提交事务
    （PS：建议使用自动提交事务模式，在未使用自动提交事务模式下，每次查询完也需要提交事务）
    '''
    # 基本数据库配置
    __HOST = '10.2.16.102'  # 配置Mysql host
    __PORT = 33306  # 配置Mysql port
    __USERNAME = 'jinqunlong'  # 配置Mysql username
    __PASSWORD = 'jinqunlong123$%^'  # 配置Mysql password
    __DB = 'monitor_db'  # 配置Mysql 连接数据库

    # 其他服务配置
    conn = ''
    __CONFIG = {
        'RE_BACK' : 3, # 失败重试次数
        'RE_BACK_TIME' : 5,  # 失败重试时间间隔（秒）
        'IS_AUTO_COMMIT' : True,  # 是否开启自动提交事务（True开启，False不开启）
        'IS_DEBUG' : True,  # 是否开启调试模式（输出错误信息，输出sql）
        'BATCH_INSERT_COUNT' : 200, # 批量插入数据时，单条语句的数据的最大条数
    }

    @property
    def conf(self):
        return self.__CONFIG

    @conf.setter
    def conf(self, dicti):
        for key in dicti:
            self.__CONFIG[key] = dicti[key]

    @check_is_error
    def __connect(self):
        self.conn = pymysql.connect(host=self.__HOST,port=int(self.__PORT),user=self.__USERNAME,passwd=self.__PASSWORD,db=self.__DB)


    def __init_config(self,para):
        # 初始化配置
        for key in para.keys():
            self.__CONFIG[key.upper()] = para[key]

    def __init__(self,host=None, port=None, username=None, password=None, db=None, **kwargs):
        '''
        初始化Mysql工具类
        :param host: 数据库域名或IP
        :param port: 数据库端口
        :param username: 数据库用户名
        :param password: 数据库密码
        :param db: 选择的数据库
        :param re_back: 失败重试次数
        :param re_back_time: 失败重试间隔时间
        :param is_auto_commit: 自动提交事务（默认）
        :param is_debug: 是否开启调试模式
        :param batch_insert_count: 批量插入时每个insert的value最大长度
        '''
        if host is not None:
            self.__HOST = host
        if port is not None:
            self.__PORT = port
        if username is not None:
            self.__USERNAME = username
        if password is not  None:
            self.__PASSWORD = password
        if db is not None:
            self.__DB = db

        self.__connect()
        self.__init_config(kwargs)

    def __insert_json_value(self,data, value_sql):
        value_sql += '('
        begin = True
        for key in data.keys():
            if begin == True:
                begin = False
            else:
                value_sql += ','
            if type(data[key]) == bool:
                data[key] = 1 if data[key] == True else 0
            if type(data[key]) == str or type(data[key]) == list \
                    or type(data[key]) == set or type(data[key]) == dict:
                value_sql += "'" + str(data[key]).replace("'", "\\'") + "'"
            else:
                value_sql += str(data[key] if data[key] != None else "'None'")
        value_sql += ')'

        return value_sql
    @check_is_error
    def insert_jsons(self,table_name='', data=None):
        # 单次插入个数
        lens = self.__CONFIG['BATCH_INSERT_COUNT']
        current_index = 0
        no_end = True
        while no_end:
            insert_sql = 'insert into ' + table_name + '('
            value_sql = ' values '
            value_sql += '('
            begin = True
            for key in data[current_index].keys():
                if begin == True:
                    begin = False
                else:
                    insert_sql += ','
                    value_sql += ','
                insert_sql += '`' + key + '`'
                if type(data[current_index][key]) == bool:
                    data[current_index][key] = 1 if data[current_index][key] == True else 0
                if type(data[current_index][key]) == str or type(data[current_index][key]) == list \
                        or type(data[current_index][key]) == set or type(data[current_index][key]) == dict:
                    value_sql += "'" + str(data[current_index][key]).replace("'", "\\'") + "'"
                else:
                    value_sql += str(data[current_index][key] if data[current_index][key] != None else "'None'")
            value_sql += '),'
            current_index += 1

            while True:
                if current_index >= len(data):
                    no_end = False
                    break
                value_sql = self.__insert_json_value(data[current_index],value_sql)
                current_index += 1
                value_sql += ','
                if (current_index + 1) % lens == 0:
                    break

            insert_sql +=  ')'
            sql = insert_sql + value_sql[0:-1]
            if self.__CONFIG['IS_DEBUG']:
                print(sql)
            cursor = self.conn.cursor()
            cursor.execute(sql)
            cursor.close()
        return 1

    @check_is_error
    def insert_json(self,table_name='',data=None):
        '''
        字典类型入库，将key作为字段，value作为值插入数据库
        :param table_name: 表名
        :param data: 字典类型数据
        :return: int类型，:1 成功，0 失败
        '''

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

        if self.__CONFIG['IS_DEBUG']:
            print(sql)

        cursor = self.conn.cursor()
        cursor.execute(sql)
        cursor.close()
        return 1

    @check_is_error
    def query_json(self,sql):
        '''
        查询mysql数据以列表类型返回，列表中的每个元素为字典类型，其中字典的key为字段，value为该字段某行对应的值
        :param sql: 查询sql语句
        :return: 列表类型 或 整数0，列表类型查询成功，0 查询失败
        '''

        if self.__CONFIG['IS_DEBUG']:
            print(sql)

        cursor = self.conn.cursor()
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
        cursor.close()
        return res

    @check_is_error
    def execute(self,sql):
        '''
        sql 增删改处理方法
        '''
        if self.__CONFIG['IS_DEBUG']:
            print(sql)

        cursor = self.conn.cursor()
        res = cursor.execute(sql)

        return res

    def commit(self):
        '''
        提交事务
        '''
        self.conn.commit()
        return 1

    @check_is_error
    def query(self,sql):
        '''
        普通sql查询，返回二维元组
        '''
        cursor = self.conn.cursor()
        cursor.execute(sql)
        list = cursor.fetchall()
        return list

    def set_db(self,db):
        '''
        修改选择的数据库
        :param db:
        :return:
        '''
        self.__DB = db
        try:
            self.conn.close()
        except:
            pass
        self.__connect()
        return True

    def close(self):
        self.conn.close()
        return True

    def __str__(self):
        pass

    def set_config(self,**kwargs):
        self.__init_config(kwargs)

    def get_config(self):
        return self.__CONFIG

if __name__ == '__main__':
    #demo
    mysql = MysqlTools(host='127.0.0.1', port=3306, username='root', password='1224qunlong', db='test')
    print(mysql.insert_json('test_1',{'name' : 'hhh', 'sex' : 'male'}))
    print(mysql.insert_jsons('test_1',[{'name' : "hhh",'sex' : 'female'},{'name' : 'eee','sex' : 'male'}]))



