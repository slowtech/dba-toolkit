# MySQLTools

## monitor_ddl_progress.sh
用于监控MySQL Online DDL的进度，简单，直接。虽然在MySQL 5.7中，performance_schema中新增了instrument实现该功能，但因为将DDL分成了7个阶段，每次只能看到当前阶段的进度，无法预测下个阶段的进度，所以，从实践角度来说，意义不是太大。

###使用方法（Usage）
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
