import secrets
import hashlib

def compute_sha1_hash(data):
    # 创建 SHA-1 摘要对象
    sha1 = hashlib.sha1()

    # 更新摘要对象的内容
    sha1.update(data)

    # 获取摘要的二进制表示
    digest = sha1.digest()

    return digest

password = "123456".encode('utf-8')
hash_stage1 = compute_sha1_hash(password)
hash_stage2 = compute_sha1_hash(hash_stage1)
print("hash_stage1: ", hash_stage1)
print("hash_stage2: ", hash_stage2)
print("authentication_string: *%s"%hash_stage2.hex().upper())


def generate_user_salt(buffer_len):
    # 生成随机字节序列
    random_bytes = secrets.token_bytes(buffer_len)

    # 将字节序列转换为合法的 UTF-8 字符串
    salt = random_bytes.decode('utf-8', errors='ignore')

    # 处理特殊字符，确保生成的字符串不包含 '\0' 和 '$'
    salt = salt.replace('\0', '\1').replace('$', '\2')

    return salt.encode('utf-8')

buffer_len = 20
generated_salt = generate_user_salt(buffer_len)
print("salt: %s"%generated_salt)

def scramble_411(password, seed):
    # 计算 password 的 SHA-1 哈希值
    password_hash_stage1 = hashlib.sha1(password).digest()

    # 计算 password_hash_stage1 的 SHA-1 哈希值
    password_hash_stage2 = hashlib.sha1(password_hash_stage1).digest()
    # 更新 seed 和 password_hash_stage2，然后计算哈希值
    md = hashlib.sha1()
    md.update(seed)
    md.update(password_hash_stage2)
    to_be_xored = md.digest()

    # 将 to_be_xored 中的每个字节与 password_hash_stage1 中对应的字节进行异或操作
    reply = bytes(x ^ y for x, y in zip(to_be_xored, password_hash_stage1))
    return reply

client_reply = scramble_411(password, generated_salt)
print("client reply: ",client_reply)

def compute_sha1_hash_multi(buf1, buf2):
    # 创建 SHA-1 哈希对象
    sha1_context = hashlib.sha1()

    # 更新哈希对象，将 buf1 和 buf2 的内容添加到计算中
    sha1_context.update(buf1)
    sha1_context.update(buf2)

    # 获取最终的 SHA-1 哈希值
    digest = sha1_context.digest()
    return digest

def my_crypt(s1, s2):
    # 使用 zip 函数将 s1 和 s2 中对应位置的元素一一匹配
    # 使用异或运算符 ^ 对每一对元素执行按位异或操作
    result = bytes(a ^ b for a, b in zip(s1, s2))
    
    return result

def check_scramble_sha1(client_reply, generated_salt, hash_stage2):
    buf=compute_sha1_hash_multi(generated_salt, hash_stage2)
    buf=my_crypt(buf, client_reply)
    hash_stage2_reassured=compute_sha1_hash(buf)
    print("hash_stage2_reassured: %s"%hash_stage2_reassured)
    if hash_stage2 == hash_stage2_reassured:
        print("passed")
    else:
        print("failed")
check_scramble_sha1(client_reply, generated_salt, hash_stage2)
