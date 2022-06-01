# kafka-util

#### 介绍
拓展原生 kafka 脚本功能，如：删除指定时间戳之前的指定topic所有数据


#### 使用说明
生成删除策略
```shell
./kafka-strategy-maker.sh --bootstrap-server super158:9092 --topic DAS-54 --timestamp 1653793077000 --output /tmp
```
![image-20220601151130354](https://blog-oss-1.oss-cn-hangzhou.aliyuncs.com/2022-06-01/image-20220601151130354.png)
执行删除策略
```shell
./kafka-delete-records.sh --bootstrap-server super158:9092 --offset-json-file /tmp/delete-strategy.json
```
![image-20220601151453821](https://blog-oss-1.oss-cn-hangzhou.aliyuncs.com/2022-06-01/image-20220601151453821.png)

#### 参与贡献

1.  Fork 本仓库
2.  新建 Feat_xxx 分支
3.  提交代码
4.  新建 Pull Request
