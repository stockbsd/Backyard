#! python3
# encoding=utf-8

import asyncio

@asyncio.coroutine
def fact(name, number):
    ret = 1
    for i in range(2, number+1):
        print('Task %s: computing factorial %d ...'%(name, number))
        yield from asyncio.sleep(2)
        ret *= i
    print('Task %s: fact(%d)=%d'%(name, number, ret))

tasks = [asyncio.Task(fact(chr(63+i), i)) for i in range(2,9)]

loop = asyncio.get_event_loop()
loop.run_until_complete(asyncio.wait(tasks))
loop.close()
