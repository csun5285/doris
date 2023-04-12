import os
import fdb
import base64
import importlib
import sys

# ------------ 输入参数 ------------
# get_key, 从meta_service log中的复制出来的key
get_key = '0110696e7374616e636500011072656772657373696f6e5f696e7374616e63652d64782d313231390001'
# proto_directory selectdb代码库的中proto存放的目录
proto_directory = '/mnt/disk2/dengxin/selectdb-core/gensrc/proto'
# type, get_key对应的pb类型
type = 'InstanceInfoPB'
# ------------ 输入参数结束 ------------

proto_files = []
sys.path.append(proto_directory)


def get_proto_module():
    for root, dirs, files in os.walk(proto_directory):
        for file in files:
            if file.endswith('.proto'):
                proto_files.append(os.path.join(root, file))

def load_proto_module_and_get_message(value):
    for proto_file in proto_files:
        module_name = os.path.splitext(os.path.basename(proto_file))[0] + '_pb2'
        module = importlib.import_module(module_name)
        for message_name in dir(module):
            if message_name == type:
                message_class = getattr(module, message_name)
                try:
                    message = message_class.FromString(value)
                    return message
                except:
                    pass

    return None


def get(key0):
    fdb.api_version(710)
    db = fdb.open()
    key = base64.b16decode(key0.upper())
    print("k =", key)
    value = db[key]
    return value


def range_get(start_key, end_key):
    fdb.api_version(710)
    db = fdb.open

    range = fdb.Range(start_key, end_key)
    values = []
    for key, value in db.get_range(range):
        values.append(value)
    return values


if __name__ == '__main__':
    value = get(get_key)
    print("v =", value)
    get_proto_module()
    value_proto = load_proto_module_and_get_message(value)
    print("-------- Output --------")
    print(value_proto)