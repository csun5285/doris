# 使用指南
工具支持两种模式warehouse和s3，通过``mode``参数指定，默认使用warehouse模式。
## warehouse模式
* 该模式自动从ms中获取指定warehouse的s3配置，若只存在单个s3配置，直接使用该配置；若存在多个配置，则需选择。
* 若待恢复文件只有一个可恢复版本，直接恢复该版本；若存在多个版本，则需选择。
* 参数ms_address(ip::port)，ms_token，warehouse_id和恢复文件路径recovery_file。
```
// 使用例子：

// 多S3配置+多可恢复版本
./recovery_tool --warehouse_id=test --ms_address=127.0.0.1:5000 --ms_token=greedisgood9999 --recovery_file=data/220410/02000000000000011648650642e1526e9806573db40ac1ad_0.dat --list_versions_nums=2
------ Find s3 conf: ------
ID: 0
ak: xxx
sk: xxx
endpoint: cos.ap-beijing.myqcloud.com
region: ap-beijing
bucket: doris-build-1308700295
prefix: test
provider: COS

ID: 1
ak: xxx
sk: xxx
endpoint: cos.ap-beijing.myqcloud.com
region: ap-beijing
bucket: gavin-tmp-bj-1308700295
prefix: test
provider: COS

----------------------------
Multiple s3 info, select the ID of s3 conf: 1
------ Find recoverable version: ------
ID: 0
Key: test/data/220410/02000000000000011648650642e1526e9806573db40ac1ad_0.dat
version_id: MTg0NDUwNjE1MDY3MDM1NTgzMDU
last_modify_time: 2023-04-27T03:43:25.000Z

ID: 1
Key: test/data/220410/02000000000000011648650642e1526e9806573db40ac1ad_0.dat
version_id: MTg0NDUwNjE1NjMzNDk5NDg5ODU
last_modify_time: 2023-04-26T11:59:19.000Z

---------------------------------------
Multiple recoverable version, select the ID of recoverable version: 0
Successfully recovery object

// 自动选择S3配置+可恢复版本
./recovery_tool --warehouse_id=ALBJGSEP --ms_address=xxxx:5000 --ms_token=xxxx  --recovery_file=data/11163/02000000000000011f45893ab3bb8d646652d3cc087a64ba_0.dat
------ Find s3 conf: ------
ID: 0
ak: xxx
sk: xxx
endpoint: oss-cn-beijing-internal.aliyuncs.com
region: cn-beijing
bucket: xxx
prefix: ALBJGSEP
provider: OSS

----------------------------
only one s3 conf, automatically select that one
------ Find recoverable version: ------
ID: 0
Key: ALBJGSEP/data/11163/02000000000000011f45893ab3bb8d646652d3cc087a64ba_0.dat
version_id: CAEQpQEYgYCAn9vF4b0YIiAyMTVkNzI1ODU5YTc0ZjQyYTI3ZDIzZjIxNWU2YTZkNQ--
last_modify_time: 2023-04-25T12:28:19Z

---------------------------------------
only one recoverable version, automatically select that one
Successfully recovery object


```

## s3模式
* 该模式通过直接配置s3信息来恢复文件，参数有ak、sk、bucket、region、endpoint、provider和obejct_key

```
// 使用例子：
./recovery_tool --mode=s3 --ak=xxx --sk=xxx --endpoint=cos.ap-hongkong.myqcloud.com --region=ap-hongkong --bucket=xxx --provider=COS --object_key=test/data/220410/02000000000000011648650642e1526e9806573db40ac1ad_0.dat --list_versions_nums=1
use s3 info :
ak: xxx
sk: xxx
endpoint: cos.ap-hongkong.myqcloud.com
region: ap-hongkong
bucket: xxx
prefix: 
provider: COS

------ Find recoverable version: ------
ID: 0
Key: test/data/220410/02000000000000011648650642e1526e9806573db40ac1ad_0.dat
version_id: MTg0NDUwNjE1MDY2OTE5MDAwNTA
last_modify_time: 2023-04-27T03:43:37.000Z

---------------------------------------
only one recoverable version, automatically select that one
Successfully recovery object
```
## 其他说明
* ``list_versions_nums``，该参数用于指定list最近对象版本的数量。如某对象最近有一个delete版本和一个可恢复版本，该参数设置为2，则能list出一个可恢复的版本并返回。一次最大支持1000，超过1000请设置-1.