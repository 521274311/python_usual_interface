import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.header import Header
from email.mime.image import MIMEImage
import time
class EmailTools:
    '''
    发送邮件工具类
    '''
    # 基本邮件信息配置
    #文本邮件配置
    __EMAIL_HOST = 'smtp.qq.com' # 邮件服务商域名
    __EMAIL_PORT = 25 # 邮件服务商端口
    __SEND_EMAIL = 'xxxxxxxxxxxx' # 发件人邮箱
    __SEND_EMAIL_PASS = 'xxxxxxxxxxxx' # 发件人密钥
    __RECEIVE_EMAIL = ['xxxxx','xxxxxx'] # 收件人邮箱
    __SEND_USER = 'xxxxxxxxx' # 发件人名称
    __TYPE = 'plain'  # 可填入字段 plain,html。
    __ENCODING = 'utf-8'  # 配置邮件编码

    # 带附件邮件配置
    __ENCLOSURE = [] # 附件位置列表，不为空时发送附件。例如:__ENCLOSURE = ['c:/test.txt']

    # html页面图片配置
    __PAGE_PICTURE = [] # html 模式页面图片本地位置列表，例如:__PAGE_PICTURE = ['c:/test1.png']
    __PAGE_PICTURE_ID = [] # html 模式页面图片页面ID，与__PAGE_PICTURE一一对应，例如:__PAGE_PICTURE_ID = ['<image1>']，之后在html内容里写上，<img src="cid:image1">即可

    # 其他服务配置
    __IS_DEBUG = True # 配置是否显示发送成功信息
    __RE_BACK = 3  # 失败重试次数
    __RE_BACK_TIME = 10  # 失败重试时间间隔

    def __init__(self):
        self.__smtp_obj = smtplib.SMTP()
        self.__smtp_obj.connect(self.__EMAIL_HOST, self.__EMAIL_PORT)
        self.__smtp_obj.login(self.__SEND_EMAIL, self.__SEND_EMAIL_PASS)

    def __get_text_message(self,content='this is a test'):
        '''
        构造MIMEText对象
        :param subject: 标题
        :param content: 内容
        :return: MIMEText
        '''
        message = MIMEText(content,self.__TYPE,self.__ENCODING)
        return message

    def __set_message(self,subject,message):
        '''
        设置发送邮件的发送人，收件人
        :param message:
        :return:
        '''
        message['Subject'] = subject

        message['From'] = Header(self.__SEND_USER, self.__ENCODING)
        for recive_email in self.__RECIVE_EMAIL:
            message['To'] = Header(recive_email, self.__ENCODING)
        return message

    def __add_enclosure(self,message):
        '''
        添加所有附件
        :return:
        '''
        if type(self.__ENCLOSURE) != list:
            self.__ENCLOSURE = [self.__ENCLOSURE]

        for enclosure in self.__ENCLOSURE:
            with open(enclosure,'rb') as f:
                att = MIMEText(f.read(),'plain',self.__ENCODING)
                att['Content-Type'] = 'application/octet-stream'
                att["Content-Disposition"] = 'attachment; filename="' + enclosure.replace('\\','/').split('/')[-1]+'"'
                message.attach(att)
        return message

    def __add_page_picture(self,message):
        '''
        添加页面图片
        :param message:
        :return:
        '''
        i = 0
        for page_picture in self.__PAGE_PICTURE:
            with open(page_picture,'rb') as f:
                img = MIMEImage(f.read())
                img.add_header('Content-ID', self.__PAGE_PICTURE_ID[i])
                message.attach(img)
            i += 1
        return message

    def send_email(self,title='title',message=''):
        '''
        发送邮件
        :param title:
        :param message:
        :return:
        '''
        re_back = 0
        if type(self.__RECIVE_EMAIL) != list:
            self.__RECIVE_EMAIL = [self.__RECIVE_EMAIL]
        message_text_obj = self.__get_text_message(content=message)

        mime_message = MIMEMultipart('related')
        mime_message.attach(message_text_obj)
        if len(self.__ENCLOSURE) > 0:
            mime_message = self.__add_enclosure(mime_message)

        if len(self.__PAGE_PICTURE) > 0:
            mime_message = self.__add_page_picture(mime_message)

        send_message = self.__set_message(title, mime_message)
        while re_back < self.__RE_BACK:
            try:
                self.__smtp_obj.sendmail(self.__SEND_EMAIL,self.__RECIVE_EMAIL,send_message.as_string())
                if self.__IS_DEBUG:
                    print('all email is finished.')
                return 1
            except Exception as e:
                re_back += 1
                if self.__IS_DEBUG:
                    print('send error '+str(re_back)+' times,error info:'+ str(e))
                time.sleep(self.__RE_BACK_TIME)
        return 0

    def set_receive_email(self,receive_email):
        '''
        设置收件人
        :param receive_email: 收件人邮件，支持list,tuple类型
        :return:
        '''
        if type(receive_email) not in (list,tuple):
            receive_email = [receive_email]
        self.__RECEIVE_EMAIL = receive_email


if __name__ == '__main__':
    #demo
    help(EmailTools)
    #
    email = EmailTools()
    email.send_email('title','this is content test')