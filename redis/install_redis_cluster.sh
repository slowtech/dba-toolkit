#!/usr/bin/bash
pkill redis-server
sleep 3
rm -rf /opt/redis/
mkdir -p /opt/redis/data/
mkdir -p /opt/redis/conf/
mkdir -p /opt/redis/log
cd /opt/redis/conf/
cat > redis_6379.conf  << EOF
port 6379
daemonize yes
pidfile "/opt/redis/data/redis_6379.pid"
loglevel notice
logfile "/opt/redis/log/redis_6379.log"
dbfilename "dump_6379.rdb"
dir "/opt/redis/data"
appendonly yes
appendfilename "appendonly_6379.aof"
cluster-enabled yes
cluster-config-file /opt/redis/conf/nodes-6379.conf
cluster-node-timeout 15000
EOF

cp redis_6379.conf redis_6380.conf
cp redis_6379.conf redis_6381.conf
cp redis_6379.conf redis_6382.conf
cp redis_6379.conf redis_6383.conf
cp redis_6379.conf redis_6384.conf

sed -i 's/6379/6380/g' redis_6380.conf 
sed -i 's/6379/6381/g' redis_6381.conf 
sed -i 's/6379/6382/g' redis_6382.conf 
sed -i 's/6379/6383/g' redis_6383.conf 
sed -i 's/6379/6384/g' redis_6384.conf 

cd /opt/redis/conf
redis-server redis_6379.conf
redis-server redis_6380.conf
redis-server redis_6381.conf
redis-server redis_6382.conf
redis-server redis_6383.conf
redis-server redis_6384.conf

redis-cli -p 6379 cluster meet 192.168.244.10 6380
redis-cli -p 6379 cluster meet 192.168.244.10 6381
redis-cli -p 6379 cluster meet 192.168.244.10 6382
redis-cli -p 6379 cluster meet 192.168.244.10 6383
redis-cli -p 6379 cluster meet 192.168.244.10 6384

sleep 3
echo "cluster replicate `redis-cli -p 6379 cluster nodes | grep 6379 | awk '{print $1}'`" | redis-cli -p 6382 -x
echo "cluster replicate `redis-cli -p 6379 cluster nodes | grep 6380 | awk '{print $1}'`" | redis-cli -p 6383 -x
echo "cluster replicate `redis-cli -p 6379 cluster nodes | grep 6381 | awk '{print $1}'`" | redis-cli -p 6384 -x

redis-cli -p 6379 cluster addslots {0..5461}

redis-cli -p 6380 cluster addslots {5462..10922}

redis-cli -p 6381 cluster addslots {10923..16383}
sleep 5

redis-cli -p 6379 cluster nodes

