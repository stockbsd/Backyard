#! python3
# coding=utf-8

import asyncio

def print_call(loop):
    print('hello')
    loop.call_later(2, print_call, loop)

loop = asyncio.get_event_loop()
loop.call_soon(print_call, loop)
loop.run_forever()
