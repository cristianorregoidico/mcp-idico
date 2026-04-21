import os
import redis.asyncio as aioredis

def create_redis_client():
    return aioredis.from_url(
        os.environ["AZURE_REDIS_URL"],
        ssl_cert_reqs=None,
        decode_responses=True,
    )
