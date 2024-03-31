import asyncio
from typing import AsyncIterator, TypeVar

T = TypeVar('T')


async def with_lock(agen: AsyncIterator[T], lock: asyncio.Lock) -> AsyncIterator[T]:
    """
    https://stackoverflow.com/questions/72204244/python-asynchronous-generator-is-already-running
    :param agen:
    :param lock:
    :return:
    """
    await lock.acquire()
    try:
        async for item in agen:
            lock.release()
            yield item
            await lock.acquire()
        return
    except StopIteration:
        return
    finally:
        lock.release()
