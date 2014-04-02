#! python3
# coding=utf-8

import asyncio

@asyncio.coroutine
def greet(n):
    for i in range(n):
        print('hello')
        yield from asyncio.sleep(2)
    print('bye')

loop = asyncio.get_event_loop()
coobj = greet(6)
loop.run_until_complete(coobj)
