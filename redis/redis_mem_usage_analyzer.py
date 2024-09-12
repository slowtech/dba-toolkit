import redis
import time
import sys
import datetime
import argparse

def get_connection(host, port, password, tls=False):
    kwargs = {
        'host': host,
        'port': port,
        'password': password,
        'socket_timeout': 5,
    }
    if tls:
        kwargs['ssl'] = True
    return redis.Redis(**kwargs)

def bytes_to_human(n):
    """Convert bytes to a human-readable format."""
    if n == 0:
        return '0B'
    sign = '-' if n < 0 else ''
    n = abs(n)
    units = ['B', 'K', 'M', 'G', 'T', 'P']
    unit = units[0]
    for u in units[1:]:
        if n < 1024:
            break
        n /= 1024
        unit = u
    return f"{sign}{n:.2f}".rstrip('0').rstrip('.') + unit

def calculate_total_mem_hashtable(memory_stats):
    total_hashtable_main = 0
    total_hashtable_expires = 0
    total_hashtable_slot_to_keys = 0
    
    # 遍历字典中的每个 db
    for db_key, db_stats in memory_stats.items():
        # 只处理以 'db.' 开头的键
        if db_key.startswith('db.'):
            # 更新总和
            total_hashtable_main += db_stats.get('overhead.hashtable.main',0)
            total_hashtable_expires += db_stats.get('overhead.hashtable.expires',0)
            total_hashtable_slot_to_keys += db_stats.get('overhead.hashtable.slot-to-keys',0)
    
    return total_hashtable_main + total_hashtable_expires + total_hashtable_slot_to_keys

def calculate_total_keys(info):
    total_keys = 0
    for key, value in info.items():
        if key.startswith('db'):
            total_keys += value.get('keys',0)
    return total_keys

def calculate_total_mem_overhead(info, keys_to_sum):
    return sum(info.get(key, 0) for key in keys_to_sum if key != 'overhead_total')

def print_diff(old_info, old_memory_stats, new_info, new_memory_stats, interval):
    """计算并打印两个内存统计信息的差值，按组输出。"""
    groups = {
        'Summary': ['used_memory', 'used_memory_dataset', 'used_memory_overhead'],
        'Overhead': ['overhead_total','mem_clients_normal', 'mem_clients_slaves', 'mem_replication_backlog', 'mem_aof_buffer','used_memory_startup', 'mem_cluster_links','used_memory_scripts','mem_hashtable'],
        'Evict & Fragmentation': ['maxmemory', 'mem_not_counted_for_evict', 'mem_counted_for_evict', 'maxmemory_policy', 'used_memory_peak','used_memory_rss','mem_fragmentation_bytes'],
        'Others': ['keys', 'instantaneous_ops_per_sec','lazyfree_pending_objects'],
    }
    now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    header = "{:<30} {:<20} {:<20} {:<20}".format(f'Metric({now})', 'Old Value', f'New Value(+{interval}s)', 'Change per second')
    print(header)
    print("="*90)

    old_info["mem_hashtable"]= calculate_total_mem_hashtable(old_memory_stats)
    new_info["mem_hashtable"]= calculate_total_mem_hashtable(new_memory_stats)
    old_info["overhead_total"] = calculate_total_mem_overhead(old_info,groups['Overhead'])
    new_info["overhead_total"] = calculate_total_mem_overhead(new_info,groups['Overhead'])
    old_info["keys"] =calculate_total_keys(old_info)
    new_info["keys"] =calculate_total_keys(new_info)
    group_num = len(groups) # 之所以定义 group_num，主要是为了循环结束时不用打换行符
    i=0
    for group_name, keys in groups.items():
        i=i+1
        if group_name != 'Overhead':
            print(f"{group_name}")
            print("-"*45)
        for key in keys:
            if key not in old_info and key !='mem_counted_for_evict':
                continue
            old_value = old_info.get(key,0)
            new_value = new_info.get(key,0)
            if key == 'mem_counted_for_evict':
                old_value = old_info.get('used_memory', 0) - old_info.get('mem_not_counted_for_evict', 0)
                new_value = new_info.get('used_memory', 0) - new_info.get('mem_not_counted_for_evict', 0)
            if key in ["maxmemory_policy", "instantaneous_ops_per_sec"]:
                diff = ""
            else:
                diff = (new_value - old_value)/interval
            if any(x in key for x in ['ratio', 'percentage']) or key in ["maxmemory_policy","instantaneous_ops_per_sec","keys", "lazyfree_pending_objects"]:
                # These are non-byte metrics, no conversion to MB needed
                old_value_display = old_value
                new_value_display = new_value
                diff_display = diff
            else:
                # Convert bytes-based metrics to MB
                old_value_display = bytes_to_human(old_value)
                new_value_display = bytes_to_human(new_value)
                diff_display = bytes_to_human(diff)
            if key == "overhead_total":
                key = "Overhead(Total)"
                print(f"{key:<30} {old_value_display:<20} {new_value_display:<20} {diff_display:<20}")
                print("-"*45)
            else:
                print(f"{key:<30} {old_value_display:<20} {new_value_display:<20} {diff_display:<20}")
        if i != group_num:
            print()

def get_redis_info(r):
    pipeline = r.pipeline()
    pipeline.info()
    pipeline.memory_stats()
    results = pipeline.execute()
    return results[0], results[1]

def print_client_list(r):
    client_list = r.client_list()
    sorted_list = sorted(client_list, key=lambda x: int(x['tot-mem']), reverse=True)
    header = f"{'ID':<5} {'Address':<18} {'Name':<5} {'Age':<6} {'Command':<15} {'User':<8} {'Qbuf':<10} {'Omem':<10} {'Total Memory':<15}"
    print(header)
    print('-' * len(header))
    for client in sorted_list:
        line = (f"{client.get('id'):<5} "
                f"{client.get('addr'):<18} "
                f"{client.get('name'):<5} "
                f"{client.get('age'):<6} "
                f"{client.get('cmd'):<15} "
                f"{client.get('user'):<8} "
                f"{bytes_to_human(int(client.get('qbuf'))):<10} "
                f"{bytes_to_human(int(client.get('omem'))):<10} "
                f"{bytes_to_human(int(client.get('tot-mem'))):<15}")
        print(line) 

def main():
    parser = argparse.ArgumentParser(description='Monitor Redis memory usage and statistics.')
    parser.add_argument('-host', '--hostname', type=str, default='127.0.0.1', help='Server hostname (default: 127.0.0.1)')
    parser.add_argument('-p', '--port', type=int, default=6379, help='Server port (default: 6379)')
    parser.add_argument('-a', '--password', type=str, help='Password for Redis Auth')
    parser.add_argument('--tls', action='store_true', help='Enable TLS for Redis connection')
    parser.add_argument('-i', '--interval', type=int, default=3, help='Refresh interval in seconds (default: 3)')
    parser.add_argument('-c', '--client', action='store_true', help='Show client list info')
    args = parser.parse_args()
    print(args)
    try:
        r = get_connection(args.hostname, args.port, args.password, args.tls)
    except Exception as e:
        print(f"Failed to connect to Redis: {e}")
        return

    if args.client:
        print_client_list(r)
        return 

    old_info, old_memory_stats = get_redis_info(r)
    while True:
        time.sleep(args.interval)
        new_info, new_memory_stats = get_redis_info(r)
        print_diff(old_info, old_memory_stats, new_info, new_memory_stats, args.interval)
        old_info, old_memory_stats = new_info, new_memory_stats

if __name__ == "__main__":
    main()
