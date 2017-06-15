import asyncio

from optparse import OptionParser

async def io():
    await asyncio.sleep(0)

async def heavy_coroutine(idx, loop, start):
    await io()
    print("%d heavy_coroutine time %f" % (idx, loop.time() - start))

async def light_coroutine(idx, loop, start):
    await io()
    print("%d light_coroutine time %f" % (idx, loop.time() - start))

async def main(loop):
    start = loop.time()
    tasks = [
        heavy_coroutine(n, loop, start) for n in range(0, 10)
    ]
    tasks += [
        light_coroutine(1, loop, start),
        light_coroutine(2, loop, start)
    ]
    await asyncio.gather(*tasks)

async def qmain(loop):
    start = loop.time()
    tasks = [
        heavy_coroutine(n, loop, start) for n in range(0, 10)
    ]
    tasks += [
        loop.spawn(light_coroutine(1, loop, start), partition='light'),
        loop.spawn(light_coroutine(2, loop, start), partition='light'),
        loop.spawn(light_coroutine(3, loop, start), partition='light')
    ]
    await asyncio.gather(*tasks)



if __name__ == '__main__':
    parser = OptionParser()
    parser.add_option("-l", "--loop", dest="loop",
                      help="Use loop/qloop", default="asyncio")

    (options, args) = parser.parse_args()
    if options.loop == "asyncio":
        print("Using asyncio loop")
        loop = asyncio.get_event_loop()
        loop.run_until_complete(main(loop))
    elif options.loop == "qloop":
        print("Using qloop")
        from qloop import EventLoopPolicy
        loop = EventLoopPolicy().get_event_loop()
        loop.run_until_complete(qmain(loop))
    else:
        raise ValueError(options.loop)
