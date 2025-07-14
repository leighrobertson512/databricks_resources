# Databricks notebook source
import random
import string
import json

def random_string(length):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))

def gen_deeply_nested_large_json():
    payload_size = 1024 * 1024 * 5  # 5MB
    num_strings = payload_size // 1000
    large_list = [random_string(1000) for _ in range(num_strings)]
    data = {
        "user": {
            "id": random.randint(1, 1_000_000_000),
            "profile": {
                "name": {"first": random_string(10), "last": random_string(10)},
                "contacts": [
                    {"type": "email", "value": f"{random_string(10)}@example.com"},
                    {"type": "phone", "value": f"+1{random.randint(1000000000,9999999999)}"}
                ],
                "address": {
                    "street": random_string(20),
                    "city": random_string(10),
                    "zip": random_string(5),
                    "geo": {"lat": random.uniform(-90, 90), "lon": random.uniform(-180, 180)}
                }
            },
            "settings": {
                "theme": random.choice(["dark", "light"]),
                "notifications": {"email": True, "sms": False, "push": True}
            }
        },
        "activity": [
            {
                "timestamp": "2025-07-08T12:00:00Z",
                "actions": [
                    {"type": "login", "success": True},
                    {"type": "view", "item": {"id": random.randint(1, 1000000), "type": "product", "meta": {"category": "books", "tags": ["fiction", "bestseller"]}}}
                ]
            }
        ],
        "metadata": {
            "ingest_ts": "2025-07-08T12:00:00Z",
            "source": "synthetic"
        },
        "large_payload": {
            "deep": {
                "level1": {
                    "level2": {
                        "level3": {
                            "data": large_list
                        }
                    }
                }
            }
        }
    }
    return json.dumps(data)


# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC large_nested_rdd = sc.parallelize([x for x in range(100000)], 100).map(lambda x: gen_deeply_nested_large_json())
# MAGIC spark.read.json(large_nested_rdd).write.format("json").mode("overwrite").save('/tmp/syudb/nested_spill_data_100K_5MB')
