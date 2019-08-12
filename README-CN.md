# RapidQueue 进程内的低延迟的持久化组件
* 用于需要高性能低延迟并且需要持久化的应用场景
* 用于内存数据的持久化场景
* 用于多写少读的场景
## 安装
> JDK 8 及以上   

Maven:
```
<dependency>
  <groupId>io.github.rapid-queue</groupId>
  <artifactId>rapid-queue-core</artifactId>
  <version>0.0.2</version>
</dependency>
```

## 集成架构说明
这个组件主要满足 低延迟（几微妙）并且需要持续化存储便于故障恢复的系统中。  
满足这样的需求，最简单有效的办法就是把需要的数据放到内存中运行，这样可以免去数据库事务等较耗时间的操作。但是在如果系统崩溃时候，尽可能的希望恢复之前的状态。
一般需要内存持久化的方式有两种：快照方式（全量同步）。日志方式（增量同步）。这个组件主要满足使用日志增量同步的方式。  

这里介绍一个较为通用的设计方式：

```

输入源
-----------
进程内，内存存储
-----------
进程内，持久化队列
-----------
数据库

====================

启动初始化场景：
1、数据库读取RapidQueue中未处理的日志消息
2、内存存储发出一致性读消息
3、RapidQueue(非持久化消息)
4、数据库收到后读取数据返回数据
5、内存存储
6、启动

====================

运行场景：
1、输入
2、业务处理从内存读取数据并生成处理日志
3、RapidQueue(持久化消息)
4、日志合并生成SQL，提交到数据库 | 同时 其他消费者处理消息

====================

内存数据库混合存储使用场景：(防止内存使用过大)
1、输入
2、业务处理从内存读取数据 (未找到需要的数据)，发出一致性读操作指令消息
3、RapidQueue(非持久化消息)
4、等待数据完成在读取指令之前的所有操作，读取到数据并返回数据

```

# 组件使用说明：