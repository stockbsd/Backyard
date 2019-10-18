import asyncio
import aiohttp
import threading
import logging
from queue import Queue, Empty

# threaded asyncio
def loop_in_thread(async_entry, *args, **kwargs):
    log = logging.getLogger('LoopThread')
    log.info('loop begin...')
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(async_entry(*args, **kwargs))
    loop.close()
    log.info('loop end...')

class AsyncDlder():
    def __init__(self):
        self.q = Queue()
        self.threads = []
        self.log = logging.getLogger('AsyncWorker')
    
    def start(self, num_threads, num_coros):
        for _ in range(num_threads):
            t = threading.Thread(target=loop_in_thread, 
                    args=(self.sched_downloaders, num_coros))
            self.threads.append(t)
            t.start()
    
    def join(self):
        for t in self.threads:
            t.join()
    
    def add_endsignal(self):
        self.q.put((None, None))
    
    def add_url(self, url, dest):
        self.q.put((url, dest))

    async def inf_downloader(self, session):
        while True:
            try:
                item, dest = self.q.get(False)
                if item is None:
                    self.log.info('Queue Finished')
                    self.add_endsignal()
                    break
                else:
                    now = time.time()
                    async with session.get(item) as resp:
                        if resp.status == 200:
                            data = await resp.read()
                            with open(dest, 'wb') as f:
                                f.write(data)
                            self.log.info(f'{resp.url} ==> {dest}, use {time.time()-now:.2f}s')
            except Empty:
                self.log.info('Waiting for Queue')
                await asyncio.sleep(0.1)
            except Exception as e:
                self.log.info(f'processing error: {e}')

    # async entry point
    async def sched_downloaders(self, num_coros):
        loop = asyncio.get_running_loop()
        async with aiohttp.ClientSession(loop=loop) as session:
            tasks = [loop.create_task(self.inf_downloader(session)) for _ in range(num_coros)]
            await asyncio.gather(*tasks, loop=loop)


if "__main__" == __name__:
    import time, sys

    logging.basicConfig(
        level=logging.INFO,
        format='%(threadName)10s %(name)12s: %(message)s',
        stream=sys.stderr,
    )

    t0 = time.time()
    
    dld = AsyncDlder()
    for i in range(30):
        dld.add_url(f"http://httpbin.org/delay/1?a={i}", '/dev/null')
    dld.add_endsignal()

    log = logging.getLogger('')
    log.info('start worker threads')

    dld.start(2, 5)
    dld.join()

    log.info('all workers exit')

    print(f'{time.time()-t0} seconds')
