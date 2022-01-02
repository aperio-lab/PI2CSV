import logging
import asyncio

logger = logging.getLogger(__name__)


def retry_async(retry):
    def decorator(func):
        async def wrapper(*args, **kwargs):
            while True:
                try:
                    return await func(*args, **kwargs)
                except Exception as e:
                    logger.exception(f'Error while running main task, sleeping {retry} before retry')
                    await asyncio.sleep(retry)

        return wrapper

    return decorator


def safe_async(func):
    async def wrapper(*args, **kwargs):
        try:
            return await func(*args, **kwargs)
        except Exception as e:
            logger.exception('Safe async decorator except error')

    return wrapper


def safe(func):
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            logger.exception(f'Safe decorator except error')

    return wrapper
