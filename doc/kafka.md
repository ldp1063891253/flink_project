下载解压kafka
获取下载地址（点开具体版本）：
http://kafka.apache.org/downloads

下载Binary 二进制版本而不是源码

```shell
cd /usr/local/soft
wget https://mirror.bit.edu.cn/apache/kafka/2.5.0/kafka_2.13-2.5.0.tgz
tar -xzvf kafka_2.13-2.5.0.tgz
cd kafka_2.13-2.5.0
```

启动zookeeper
kafka需要依赖ZK，安装包中已经自带了一个ZK，也可以改成指定已运行的ZK。
如果改成指定的ZK需要修改修改 kafka 安装目录下的 config/server.properties 文件中的 zookeeper.connect 。这里使用自带的ZK。

后台启动ZK：

```shell
nohup ./bin/zookeeper-server-start.sh config/zookeeper.properties >> zookeeper.nohup &
```

检查zookeeper是否启动成功：

```shell
ps -ef|grep zookeeper
```

启动kafka
修改相关配置

```shell
vim config/server.properties
```

Broker ID启动以后就不能改了

```shell
broker.id=1
```

取消注释，改成本机IP：

```shell
listeners=PLAINTEXT://192.168.44.160:9092
```



num.partitions后面增加2行。
发送到不存在topic自动创建。允许永久删除topic。

```shell
num.partitions=1
auto.create.topics.enable=true
delete.topic.enable=true
```

后台启动kafka（kafka安装目录下）：

```shell
 /opt/ldp/kafka_2.13-2.5.0/bin/kafka-server-start.sh  /opt/ldp/kafka_2.13-2.5.0/config/server.properties & 
```

日志在logs目录下

创建Topic
创建一个名为gptest的topic，只有一个副本，一个分区：

```shell
 /opt/ldp/kafka_2.13-2.5.0/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic gptest
```

查看已经创建的 topic：

```shell
 /opt/ldp/kafka_2.13-2.5.0/bin/kafka-topics.sh -list -zookeeper localhost:2181
```

启动Producer
打开一个窗口，在kafka解压目录下：

```shell
 /opt/ldp/kafka_2.13-2.5.0/bin/kafka-console-producer.sh --broker-list localhost:9092 --topic gptest
```

启动Consumer
在一个新的远程窗口中：

```shell
/opt/ldp/kafka_2.13-2.5.0/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic gptest --from-beginning
```

删除kafka全部数据步骤：
1、停止每台机器上的kafka；
2、删除kafka存储目录（server.properties文件log.dirs配置，默认为“/tmp/kafka-logs”）全部topic的数据目录；
3、删除zookeeper上与kafka相关的znode节点；除了/zookeeper
4、重启kafka。