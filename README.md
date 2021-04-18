# dba-toolkit
DBA工具集

## monitor_ddl_progress.sh
用于监控MySQL Online DDL的进度，简单，直接。

#### 使用方法（Usage）
只需输入表的当前目录，及表名，如：
```
# sh monitor_ddl_progress.sh /dbdata/mysql/3306/data/sbtest/ sbtest1
Altering sbtest.sbtest1 ...
Altering sbtest.sbtest1:  16% 00:01:08 remain
Altering sbtest.sbtest1:  28% 00:01:03 remain
Altering sbtest.sbtest1:  38% 00:01:01 remain
Altering sbtest.sbtest1:  48% 00:00:47 remain
Altering sbtest.sbtest1:  59% 00:00:39 remain
Altering sbtest.sbtest1:  68% 00:00:33 remain
Altering sbtest.sbtest1:  78% 00:00:23 remain
Altering sbtest.sbtest1:  87% 00:00:12 remain
Altering sbtest.sbtest1:  98% 00:00:01 remain
Successfully altered sbtest.sbtest1
```


