import asyncio


def int2bytes(x):
    return bytes.fromhex(f'{x:x}')


def bytes2int(x):
    return int(x.hex(), 16)


def run(*args):
    loop = asyncio.get_event_loop()
    loop.run_until_complete(asyncio.gather(*args))
