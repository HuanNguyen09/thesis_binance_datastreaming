# import redis

# # Connect to the Redis server running on localhost:6379
# r = redis.Redis(host='localhost', port=6379, decode_responses=True)

# # Ví dụ về sử dụng các Redis database
# r.select(2)  # Chọn database 0
# r.set('key1', 'value1')

# r.select(1)  # Chọn database 1
# r.set('key1', 'value2')


# import redis

# # Kết nối tới máy chủ Redis
# r = redis.Redis(host='localhost', port=6379, decode_responses=True)

# # Chọn stream bạn muốn đọc dữ liệu
# stream_key = 'my-key'
 
# r.select(0)
# # Đọc toàn bộ dữ liệu từ stream
# data = r.xrange(stream_key)

# # In ra dữ liệu đã đọc
# for entry in data:
#     print(entry)

import redis

def print_all_redis_data():
    # Kết nối tới Redis
    redis_host = "localhost"
    redis_port = 6379
    redis_client = redis.StrictRedis(host=redis_host, port=redis_port)

    # Lấy danh sách tất cả các key trong Redis
    keys = redis_client.keys("*")

    # In ra dữ liệu của từng key
    for key in keys:
        try:
            # Lấy giá trị của key và kiểu dữ liệu của nó
            key_type = redis_client.type(key)
            value = None

            # Kiểm tra kiểu dữ liệu và lấy giá trị tương ứng
            if key_type == b'string':
                value = redis_client.get(key)
            elif key_type == b'list':
                value = redis_client.lrange(key, 0, -1)
            elif key_type == b'set':
                value = redis_client.smembers(key)
            elif key_type == b'hash':
                value = redis_client.hgetall(key)
            elif key_type == b'zset':
                value = redis_client.zrange(key, 0, -1, withscores=True)
            else:
                value = "Unknown data type"

            print(f"Key: {key.decode()}, Type: {key_type.decode()}, Value: {value}")

        except Exception as e:
            print(f"Error processing key '{key.decode()}': {e}")

if __name__ == "__main__":
    print_all_redis_data()
