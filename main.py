import functools
import json
import os
import os.path
import gzip
import datetime
from filecmp import cmp

import itertools

import botocore


class Tester(object):
    """ it is to convert exported S3 data to mongodb table"""
    version = 0.1

    def __init__(self):
        self.S3DataFolderPath = 'C:\\dc_exercise\\0e7d5638c77e46deba4896551220433c'


    # def read_gz_file(self, path):
    #     if os.path.exists(path):
    #         with gzip.open(path, 'r') as pf:
    #             for line in pf:
    #                 yield line
    #     else:
    #         print('the path [{}] is not exist!'.format(path))
    #
    # def ExtractAWSAnalyticsInfo(self):
    #
    #     for YearFolder in os.listdir(self.S3DataFolderPath):
    #
    #         YearsPath = os.path.join(self.S3DataFolderPath, YearFolder)
    #         for MonthFolder in os.listdir(YearsPath):
    #
    #             MonthsPath = os.path.join(YearsPath, MonthFolder)
    #             for DayFolder in os.listdir(MonthsPath):
    #
    #                 TimeStamp = "-".join((YearFolder, MonthFolder, DayFolder))
    #
    #                 DaysPath = os.path.join(MonthsPath, DayFolder)
    #                 for HourFolder in os.listdir(DaysPath):
    #                     HoursPath = os.path.join(DaysPath, HourFolder)
    #
    #                     for S3File in os.listdir(HoursPath):
    #                         Content = self.read_gz_file(os.path.join(HoursPath, S3File))
    #
    #                         if getattr(Content, '__iter__', None):
    #                             for line in Content:
    #                                 __dict__ = json.loads(line)
    #                                 cont = json.dumps(self, default=lambda o: __dict__,
    #                                            sort_keys=True, indent=4)
    #                                 print(cont)
    #                                 print("============")
    #
    #
    #                                 # if "2018-02-05" in __dict__:
    #                                 #     print(line)



    def read_gz_file(self, path):
        if os.path.exists(path):
            with gzip.open(path, 'r') as pf:
                for line in pf:
                    yield line
        else:
            print('the path [{}] is not exist!'.format(path))

    def ExtractAWSAnalyticsInfo(self):

        counter = 0

        for YearFolder in os.listdir(self.S3DataFolderPath):

            YearsPath = os.path.join(self.S3DataFolderPath, YearFolder)
            for MonthFolder in os.listdir(YearsPath):

                MonthsPath = os.path.join(YearsPath, MonthFolder)
                for DayFolder in os.listdir(MonthsPath):

                    TimeStamp = "-".join((YearFolder, MonthFolder, DayFolder))

                    DaysPath = os.path.join(MonthsPath, DayFolder)
                    for HourFolder in os.listdir(DaysPath):
                        HoursPath = os.path.join(DaysPath, HourFolder)

                        for S3File in os.listdir(HoursPath):
                            Content = self.read_gz_file(os.path.join(HoursPath, S3File))
                            if getattr(Content, '__iter__', None):
                                for line in Content:
                                    __dict__ = json.loads(line)
                                    cont = json.dumps(self, default=lambda o: __dict__,
                                                               sort_keys=True, indent=4)

                                    if("2018-01-31" in cont):
                                        print("========")
                                        print(__dict__['event_type'])
                                        print(__dict__["attributes"]["SerialNumber"])
                                        counter = counter + 1

        print("counter: ", counter)

def redirect_io():
    import sys
    sys.stdout = open('out/output.txt', 'w')
    print('test')

def ToUtcTime():
    import time

    time1 = datetime.datetime.now()
    print(time1)

    t = 1429417200.0
    print(datetime.datetime.fromtimestamp(t))

    # 1 Python的timestamp是一个浮点数。如果有小数位，小数位表示毫秒数。
    # 2 某些编程语言（如Java和JavaScript）的timestamp使用整数表示毫秒数，
    #    这种情况下只需要把timestamp除以1000就得到Python的浮点表示方法。
    time2 = datetime.datetime.fromtimestamp(1510012132000 / 1000)
    print(time2)
    time3 = datetime.datetime.utcfromtimestamp(1510012132000 / 1000)
    print(time3)
    print(type(time3))

    time4 = datetime.date.fromtimestamp(1510012132000 / 1000)
    print(time4)

    a = 1234
    b = a / 100
    print(b)

    print("====================")
    time4 = datetime.datetime.fromtimestamp(1517875220309 / 1000)
    print(time4)
    time5 = datetime.datetime.utcfromtimestamp(1517875220309 / 1000)
    print(time5)

def OrderedDictTest():
    from collections import OrderedDict
    a = '{"fields": { "name": "%s", "city": "%s", "status": "%s", "country": "%s" }}'
    b = json.loads(a, object_pairs_hook=OrderedDict)
    print(json.dumps(b))

def OrderedDictTest2():
    from collections import OrderedDict
    StatInfo = {}
    StatInfo["1INF Model #"] = "INF Model #"
    StatInfo["2 PC Model"] = "PC Model"
    StatInfo["3 OS Version"] = "OS Version"
    StatInfo["4 MP Software Version"] = "MP Software Version"
    StatInfo["5 Image Version"] = "Image Version"
    StatInfo['6 Id'] = 'Id'
    StatInfo['7 ClientId'] = 'ClientId'

    print(json.dumps(StatInfo))

def compareVersion(version1, version2):
    #usage: sorted(res, key=functools.cmp_to_key(compareVersion))
    versions1 = [int(v) for v in version1.split(".")]
    versions2 = [int(v) for v in version2.split(".")]
    for i in range(max(len(versions1), len(versions2))):
        v1 = versions1[i] if i < len(versions1) else 0
        v2 = versions2[i] if i < len(versions2) else 0
        if v1 > v2:
            return 1
        elif v1 < v2:
            return -1
    return 0

def compareVersion2(version1, version2):
    v1, v2 = (map(int, v.split('.')) for v in (version1, version2))
    d = len(v2) - len(v1)
    return cmp(v1 + [0]*d, v2 + [0]*-d)

def compareVersio3(version1, version2):
    splits = (map(int, v.split('.')) for v in (version1, version2))
    return cmp(*zip(*itertools.zip_longest(*splits, fillvalue=0)))

def compareVersio4(version1, version2):
    from distutils.version import LooseVersion, StrictVersion
    if LooseVersion(version1) < LooseVersion(version2):
        return -1
    elif LooseVersion(version1) > LooseVersion(version2):
        return 1

    return 0

def compareVersio5(version1, version2):

    len1 = len(version1)
    len2 = len(version2)

    pos = 0
    while(pos < min(len1, len2)) :
        if version1[pos] == version2[pos]:
            pos += 1
            continue
        elif version1[pos] == '.':
            return -1
        elif version2[pos] == '.':
            return 1
        elif version1[pos] < version2[pos]:
            return -1
        else:
            return 1

        #: ++pos wrong: python dont support
        pos += 1

    if pos < len1:
        return 1
    elif pos < len2:
        return -1

    return 0

def boto_test():
    import boto3

    #no need to create credential file before
    session = boto3.Session(
        aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY'),
        region_name = 'us-east-1'
    )

    s3 = session.resource('s3')

    for bucket in s3.buckets.all():
        print(bucket.name)

    # Upload a new file
    data = open('C:\\Users\\fqyya\\Desktop\\qq.jpg', 'rb')
    s3.Bucket('scutbucket').put_object(Key='test.jpg', Body=data)


    #download unexistent object
    BUCKET_NAME = 'scutbucket'  # replace with your bucket name
    KEY = 'my_image_in_s3.jpg'  # replace with your object key

    try:
        s3.Bucket(BUCKET_NAME).download_file(KEY, 'my_local_image.jpg')
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            print("The object does not exist.")
        else:
            raise

    # download existent object
    KEY = 'test.jpg'  # replace with your object key

    try:
        s3.Bucket(BUCKET_NAME).download_file(KEY, 'C:\\Users\\fqyya\\Desktop\\testlog\\local_obj.jpg')
        print("download successfully")
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            print("The object does not exist.")
        else:
            raise

def getParentPath():
    S3DataFolderPath = "C:\\aws_workspace\\awsma\\events"
    print("path", S3DataFolderPath)
    ParentPath = os.path.abspath(os.path.join(S3DataFolderPath, os.path.pardir))
    print("parent", ParentPath)

def process_retest1():
    #不能跨平台： 由于Windows没有fork调用，上面的代码在Windows上无法运行。
    import os

    print('Process (%s) start...' % os.getpid())

    from sys import platform
    if platform == "linux" or platform == "linux2":
        pid = os.fork()
        if pid == 0:
            print('I am child process (%s) and my parent is %s.' % (os.getpid(), os.getppid()))
        else:
            print('I (%s) just created a child process (%s).' % (os.getpid(), pid))
    else:
        print("current os: " + platform)


from multiprocessing import Process
import os

# 子进程要执行的代码
def run_proc(name):
    print('Run child process %s (%s)...' % (name, os.getpid()))

def process_retest2():
    #能跨平台： multiprocessing模块就是跨平台版本的多进程模块
    print('Parent process %s.' % os.getpid())

    p = Process(target=run_proc, args=('arg1',))
    print('Process will start.')

    p.start()
    p.join()
    print('Process end.')


from multiprocessing import Pool
import os, time, random

def long_time_task(name):
    print ('Run task %s (%s)...' % (name, os.getpid()))
    start = time.time()
    time.sleep(random.random() * 3)
    end = time.time()
    print ('Task %s runs %0.2f seconds.' % (name, (end - start)))

def process_pool_retest():
    print('Parent process %s.' % os.getpid())
    MyPool = Pool(12)
    for i in range(12):
        MyPool.apply_async(long_time_task, args=(i,))
    print('Waiting for all subprocesses done...')

    MyPool.close()
    MyPool.join()
    print('All subprocesses done.')

from multiprocessing import Queue

# 写数据进程执行的代码:
def write(q):
    for value in ['A', 'B', 'C']:
        print ('Put %s to queue...' % value)
        q.put(value)
        time.sleep(random.random())

# 读数据进程执行的代码:
def read(q):
    while True:
        value = q.get(True)
        print ('Get %s from queue.' % value)

def ipc_retest():
    # 父进程创建Queue，并传给各个子进程：
    q = Queue()
    WriterProcess = Process(target=write, args=(q,))
    ReaderProcess = Process(target=read, args=(q,))

    # 启动子进程，写入:
    WriterProcess.start()
    # 启动子进程，读取:
    ReaderProcess.start()
    # 等待Writer结束:
    WriterProcess.join()
    time.sleep(2)
    # ReaderProcess进程里是死循环，无法等待其结束，只能强行终止:
    ReaderProcess.terminate()

import time, threading
# 新线程执行的代码:
def loop(*SCUT_Args):
    print('thread %s is running...' % threading.current_thread().name)
    print("args : ", SCUT_Args)
    n = 0
    while n < 5:
        n = n + 1
        print('thread %s >>> %s' % (threading.current_thread().name, n))
        time.sleep(1)
    print('thread %s ended.' % threading.current_thread().name)


def thread_retest1():
    print('thread %s is running...' % threading.current_thread().name)
    t = threading.Thread(target=loop, name='SCUTThread', args =('C13',"A4"))
    t.start()
    t.join()
    print('thread %s ended.' % threading.current_thread().name)


# # 假定这是你的银行存款:
# balance = 0
#
# def change_it(n):
#     # 先存后取，结果应该为0:
#     global balance
#     balance = balance + n
#     balance = balance - n

def run_thread(n):
    for i in range(100000):
        change_it(n)


def WithOutLock_Retest():
    # 当t1、t2交替执行时，只要循环次数足够多，balance的结果就不一定是0了。
    t1 = threading.Thread(target=run_thread, args=(5,))
    t2 = threading.Thread(target=run_thread, args=(8,))
    t1.start()
    t2.start()
    t1.join()
    t2.join()
    print(balance)

# 假定这是你的银行存款:
balance = 0
lock = threading.Lock()

def change_it(n):
    # 先存后取，结果应该为0:
    global balance
    balance = balance + n
    balance = balance - n

def run_thread2(n):
    for i in range(100000):
        # 先要获取锁:
        lock.acquire()
        try:
            # 放心地改吧:
            change_it(n)
        finally:
            # 改完了一定要释放锁:
            lock.release()

def Lock_Retest():
    # 当t1、t2交替执行时，不管循环次数为多少，balance的结果总是0了。
    t1 = threading.Thread(target=run_thread2, args=(5,))
    t2 = threading.Thread(target=run_thread2, args=(8,))
    t1.start()
    t2.start()
    t1.join()
    t2.join()
    print(balance)


def loop():
    x = 0
    while True:
        x = x ^ 1

import threading, multiprocessing
def thread_cpu_percentage_test():
    # 启动与CPU核心数量相同的N个线程，在4核CPU上可以监控到CPU占用率仅有25%左右，也就是仅使用了一核。
    for i in range(multiprocessing.cpu_count()):
        t = threading.Thread(target=loop)
        t.start()


# 创建全局ThreadLocal对象:
SCUT = threading.local()

def process_student():
    # 获取当前线程关联的StudentName:
    std = SCUT.StudentName
    print('Hello, %s (in %s)' % (std, threading.current_thread().name))

def process_thread(name):
    # 绑定ThreadLocal的StudentName:
    SCUT.StudentName = name
    process_student()

def ThreadLocal_Retest():
    t1 = threading.Thread(target=process_thread, name='Thread-A', args=('Alice',))
    t2 = threading.Thread(target=process_thread, name='Thread-B', args=('Bob',))
    t1.start()
    t2.start()
    t1.join()
    t2.join()

def main():
    # list1 = ['2.4.07.494', '2.4.06.6']
    # list2 = ['2.4.06.6', '2.4.06.7']
    #
    # print("=====================  list1 : ", list1)
    # list1.sort(key=functools.cmp_to_key(compareVersio5))
    # print("===================== list1 : ", list1)
    #
    # print("=====================  list2 : ", list2)
    # list2.sort(key=functools.cmp_to_key(compareVersio5))
    # print("===================== list2 : ", list2)

    # OrderedDictTest2()
    # OrderedDictTest()
    # ToUtcTime()
    # redirect_io()
    # IfcTest = Tester()
    # IfcTest.ExtractAWSAnalyticsInfo()
    # getParentPath()


    # boto_test()

    # process_retest1()
    #process_retest2()
    # process_pool_retest()
    # ipc_retest()

    # thread_retest1()

    # WithOutLock_Retest()
    # Lock_Retest()

    # thread_cpu_percentage_test()

    ThreadLocal_Retest()

if __name__ == '__main__':
    main()
