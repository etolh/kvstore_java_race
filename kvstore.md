### 题目思路
>>>性能评测
随机写入：16个线程并发随机写入，每个线程使用Set各写400万次随机数据（key 8B、value 4KB）
顺序读取：16个线程并发按照写入顺序读取，每个线程各使用Get读取400万次随机数据
随机读取：16个线程并发读取，每个线程随机读取400万次数据，读取范围覆盖全部写入数据
1. 共2个维度测试性能，每一个维度测试结束后会保留DB数据，关闭实例，重启进程，清空PageCache，下一个维度开始后重新打开新的实例
      这里是2个维度还是3个维度？如果是2个维度，哪两个阶段是在一起的？
      随机写入和顺序读取是一个阶段（即写入完成后立即读取），随机读取是另一个阶段（经过Close和Init后再读取）
2次测评：
1	创建实例，启动init，再使用set写入400w条数据，写入完成后再按写入的顺序使用get读取数据，最后调用close关闭。
中间：关闭实例，重启进程，清空PageCache。
2	创建新的实例，启动init，再使用get随机获取400万次数据，范围覆盖全部写入的数据。

>>>正确性测试流程：
一次：先写入一定数量的数据set，kill -9
再重启程序，先启动init初始化，再按顺序获取刚才写入的所有key(get)，若一致，则认为正确。
判定正确后，重新调用set写入；重复
10.203.99.163


### 思考
key: 8B value:4KB
8B*64000000/1024/1024
4KB*64000000/1024/1024
key:8B=64bit，2^64绝对够6400万，但是可能存在重复
一个value 4KB，对应12位

一个key-file: 12M 
12*64=768M

### 基础版
1.	key和value分离
value单独存储到多个value文件中（1.data）中，每个value文件大小固定256MB，一共有64个文件。
hash(key) --> file.no
一个线程创建400万4KB的value
400w*4KB~15.25GB

2.	set:首先根据key获取文件编号即得到value文件，将value值顺序插入到value文件尾部，返回插入的offset（插入的位置），使用key_map缓存key和offset的映射，并将（key,offset）也追加到kv_store.key文件的末尾。
offset: 256M/4KB=2^16 只需要16bit=2B
因此，一个(key,offset)只需要10B大小，
400w*10B = 38.14M

3.	open:首先初始化所有的value文件，缓存其FileChannel对象。然后，读取kv_store.key文件，将文件中(key,offset)映射一一存储到key_map中。

4.	get: 首先根据key哈希得到指定文件channel，再直接从key_map中得到value的offset，即可读取value文件中的value值。

全局变量：
1.	key_map: 存储key和offset的映射
2.	DataFiles[]: 存储所有value文件的channel数组

注意问题：
1．重复key更新问题
在set时，首先根据key判断offset是否存在，若存在，表明此key已经写入过，因此只要在此offset处更新value。

2. key分布不均匀，导致一些value文件，超出value范围
添加一个temp文件，当该key存入对应的value文件时，若此时文件已经满了，将此value存到temp文件中，并设置offset=原value文件结尾offset+temp文件offset。


涉及知识
1.	Java NIO FileChannel
2.	FastThreadLocal
3.	HHPC HashMap

解决问题
1.	maven clean install问题：无法连接到阿里云的maven服务器
59.110.144.164 maven.aliyun.com
https://blog.csdn.net/qiushisoftware/article/details/89609447
https://yq.aliyun.com/articles/621196

2.	注意问题

1.数据不要写在dir下，写在dir父级目录
2.如果get key为空则需要进行val.setValue(null);
3.EngineKVStoreRace不是单例

3.	偏移量单位问题 √
4.	文件创建问题
5.	缓存问题—正确性问题
6.	多线程问题：
不同线程不同文件夹
7.	异常打印与日志问题

日志分析
1.	Key分布均匀
Key以递增数字方式显示，类似 “107374182497”~ “107374182597”

实现功能方式
1.	寻找优化点
2.	学习相关知识点
3.	测试
4.	应用
5.	评估

### 思路与思考
1.	出错了，先思考寻找问题所在，再进行学习测试，验证通过后再提交。
不要做无用之功。

分析
时间开销：主要花费时间在init读取keyFile，构建keyOff索引。
set和get都只是直接获取索引存储或获取数据，时间开销不大。

初始版本：
1.	维护HashMap，在读取keyFile文件上、索引结构上、文件分区、压缩等上并没有进行很大改进
应当在其上面进行优化

### 版本2：
读取keyFile使用MMAP，多线程
1.	MMAP

其他想法：
1	读取keyFile时加速：多线程、MMAP、对keyFile进行分片
2	索引结构变化（本质读取keyFile）:
目前keyFile只是按插入顺序存储在keyFile文件中（固定大小），是否可以根据key将记录（key, off）排序，从而可以直接根据key获取到off
	数据结构：树
3	整体结构：
目前是key,value分离，key和value的offset一起存储。

急需实现事项
1.	思路：polar博客
2.	本地单线程测试案例（正确性测评）、多线程测试
3.	相关文档md.git整理
4.	统一TestCase在项目中



Caused by: java.lang.IndexOutOfBoundsException



IllegalArgumentException

// 解决读取分区错误问题51 // og.info("set: partition No:{} key:{} off:{}   partitionOff{}", parNo, key, offset, partitionOff);


commit 3386cfd6ab03ba2fbc6689c2d7f5ea6eed4ea4b4 (HEAD -> feature/partition)
Author: etolh <huliang12@126.com>
Date:   Mon Jul 29 23:41:06 2019 +0800

    新架构 16KB落盘


7.31 代实现：
1.	架构整理+git提交整理
2.	16KB落盘
3.	direct io尝试

### 整理架构 0802
    一个分区，一个KeyFile,一个ValueFile和一个MemoryMap
1. DB: 记录分区号parNo,分区偏移量offset 
    1. init(dir, file_size):提取所有分区中的key文件构建索引映射
    2. set(key,val): 根据parNo和offset操作对应分区文件: 将(key,parNo,offset)写入到keyFile,将value写入到valueFile
    3. get(key,val): 直接根据索引映射获取parNo和offset: 读取指定分区中valueFile指定偏移量的value值
    4. close(): 关闭所有分区中KeyFile和ValueFile通道

2. Partition: 分区架构
    1. 根据插入顺序一个一个放入到分区文件中，插满换一个分区

3. Common: 存储通用信息 
    1. Constant: 存储常量信息
    
4. index: 分区Key读写（分区架构1）
    1. init(dir, thread_no, parNo): 读取分区KeyFile文件，构建通道
    2. load(): 读取KeyFile中数据，根据架构情况构建Key-Map映射
        1. KeyFile存储(key, parNo, offset): 按插入顺序构建分区,每分区4000(其他)kv值, 因此key需要映射parNo, map:key->(parNo, offset) (当前架构)
        2. KeyFile存储(key, parNo)：架构如上，读取key由于和value一一对应，因此key偏移量offset即是分区偏移量，节约KeyFile容量，map:key->(parNo, offset)
        3. KeyFile存储key: 新架构:对key进行hash构建分区（选择合适的哈希函数，使得连续的key平均分到各个分区），直接根据key哈希获取parNo, map:key->(offset)
        
        Note: Map单独设置为一类，可以使用HPPC中的HashMap、ArrayMap(有序，二分查找)，更多的Map索引结构：树
        
    3. int read(key): 直接根据map获取key对应的偏移量
    4. write(key): 根据KeyFile下一个插入的偏移量（全局变量），插入到KeyFile中，并缓存map。
        架构1: 需要存储key、parNo、offset
        架构2: 只需要存储key
    5.  close: 关闭分区KeyFile资源
    
    3. Map: 存储Key和(parNo,offset)映射，设置接口，不同实现类(架构1)
        1. insert(key, (parNo,offset))
        2. long get(key)
        
5. data(valueFile): 分区valueFile读写
    1. init(dir, thread_no, parNo): 读取分区ValueFile文件，构建通道
    2. read(offset): 直接读取偏移量位置的数据
    3. write(value, offset): 将value写入到指定offset位置
    4. close(): 关闭分区KeyFile资源
    
    
### 0803
1. 分区测试：经过测试，无法很好地根据key进行分区，只能使用全局Map:映射所有key和parNo的关系
在DB创建Map对象，传递到KeyData中获取keyFile数据，输入Map内容。