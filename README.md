# 自定义函数

## 使用方法

1. 找到适合的 release 版本 jar （或者自行编译）
2. 拷贝到 hive 部署的一台服务器 或者 上传到 hdfs
3. 注册函数
    ```
    CREATE [temporary] FUNCTION [db_name.]function_name AS class_name [USING JAR|FILE|ARCHIVE 'file_uri' [, JAR|FILE|ARCHIVE 'file_uri'] ];
    ```
    如下：
    ```
    CREATE FUNCTION default.mdecode AS 'tech.jiafan.udf.MDecode' USING JAR 'hdfs:///user/hive/udf/hudf-0.0.1.jar';
    ```
    注册了一个 mdecode 函数在 default 下
4. 使用
    ```
    select user_type, default.mdecode(user_type,1, '个人会员', 2, '单位会员', 3, '特约会员', '其他会员') user_type_name from user_table
    ```

    在多hive gateway 若遇到找不到函数情况，在 spark sql 或者 hive cli 执行 下面语句

    ```
    reload function ;
    ```




## UDF

### 1. **mdecode** 函数

> 作用： 简化case when 类似映射语句

> 参数说明: (待解码字段,可能值1,可能值1对应返回,可能值2,可能值2对应返回,...,可能值n,可能值n对应返回,其他未枚举到值的补充返回)

> 参数说明: 参数列表可变长度

> 返回：字符串

### 2. **ipToNum** 函数

> 作用： ipv4 转化为数字,便于 ip地址段处理

> 参数说明: (ip地址字段, 【可选：转化失败默认值】)

> 参数说明: 默认值需要传入数字

> 返回：长整形 *Long*

### 3. **numToIp** 函数

> 作用： 将数字转化为 ip地址

> 参数：列表，可变长度

> 参数说明: (ip数字字段, 【可选：转化失败默认值】)

> 返回：字符串

### 4. **dateToSign** 函数

> 作用： 日期转为星座

> 参数：日期Date

> 参数说明: 日期，一般为出生日期

> 返回：字符串

## UDAF

## UDTF

### 1. **dateRangeExplode** 函数

> 作用： 根据起止日期，纵向展开， 配合 lateral view 优化拉链表 关联条件必须写在 where子句中的，过程中产生笛卡尔积的性能问题

> 参数：(start_date , end_date)

> 参数说明: 参数可为 Date 、Timestamp、String（yyyy-MM-dd） 类型

> 返回：生成表
