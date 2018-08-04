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

def main():
    list1 = ['2.4.07.494', '2.4.06.6']
    list2 = ['2.4.06.6', '2.4.06.7']

    print("=====================  list1 : ", list1)
    list1.sort(key=functools.cmp_to_key(compareVersio5))
    print("===================== list1 : ", list1)

    print("=====================  list2 : ", list2)
    list2.sort(key=functools.cmp_to_key(compareVersio5))
    print("===================== list2 : ", list2)

    # OrderedDictTest2()
    # OrderedDictTest()
    # ToUtcTime()
    # redirect_io()
    # IfcTest = Tester()
    # IfcTest.ExtractAWSAnalyticsInfo()
    # getParentPath()


    boto_test()

if __name__ == '__main__':
    main()
