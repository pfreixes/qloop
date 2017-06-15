"""
Loop based on the default loop that implements a fair callback
scheduling based on queues. Each queue stands for a partition which
are isolated between them. User can decide to spawn a coroutine and its
future asynhronous resources to a specific partition using the new loop
method called `spawn`, perhaps:

    >>> async def foo():
    >>>     await asyncio.sleep(0):
    >>>
    >>> async def main(loop):
    >>>    loop.spawn(foo())
    >>>
    >>> loop = asyncio.get_event_loop()
    >>> loop.run_until_complete(main(loop))

Future tasks and IO file descriptors created by spawned coroutines will be
allocated at the same partition, sharing the same reactor queue.

The different queues belonging to each partition are iterated in a round robin way,
meanwhile each queue is handled as a regular FIFO queue. If, and only if, a queue
runs out of callabcks the IO process is executed again, processing only those events
related to the file descriptors that belong to that specific partion that owns that queue.
The same for the scheduled calls.
"""
import collections
import heapq
import logging

from asyncio import Task
from asyncio import Future
from asyncio import Handle
from asyncio import SelectorEventLoop
from asyncio import DefaultEventLoopPolicy
from asyncio.log import logger
from asyncio.base_events import _format_handle
from asyncio.base_events import _MIN_SCHEDULED_TIMER_HANDLES
from asyncio.base_events import _MIN_CANCELLED_TIMER_HANDLES_FRACTION 
from asyncio import events
from asyncio.events import BaseDefaultEventLoopPolicy


__all__ = ("Loop", "EventLoopPolicy")


_ROOT_PARTITION = object()


def _find_partition(loop):
    # find the partition by following the next strategies
    # 1 - Callback is called inside of a scope task
    # 2 - If not, use the ROOT_PARTITION
    try:
        return Task.current_task(loop=loop).partition
    except AttributeError:
        # out of a task scope
        return _ROOT_PARTITION

class _Partition:
    def __init__(self):
        self.tasks = set()
        self.readers = set()
        self.writers = set()
        self.handles = collections.deque()
        self.scheduled = []


class Loop(SelectorEventLoop):

    def __init__(self, selector=None):

        self._partitions = {
            _ROOT_PARTITION: _Partition()
        }
        self._partitions_to_process = set((_ROOT_PARTITION,))
        self._task_factory = self._inherit_queue
        super().__init__(selector)

    def spawn(self, coro, partition=None):
        """Place and run a coro to a specific and isolated partition, if partition is not given a new one
        will be created.

        Return a task object bound to the coro.
        """
        task = Task(coro, loop=self)
        partition = partition if partition else task
        task.partition = partition

        if task._source_traceback:
            del task._source_traceback[-1]

        try:
            self._partitions[partition].tasks.add(task)
        except KeyError:
            self._partitions[partition] = _Partition()
            self._partitions[partition].tasks.add(task)
            self._partitions_to_process.add(partition)

        return task

    def _inherit_queue(self, coro):
        """Create a new task inheriting the partition
        assigned to the current task.

        If there is no current task, or the curren task
        does not have any queue will be assinged to the
        root one.

        Return a task object
        """
        task = Task(coro, loop=self)
        task.partition = _find_partition(self)
        if task._source_traceback:
            del task._source_traceback[-1]

        self._partitions[task.partition].add_task(task)
        task.add_done_callback(self._partitions[task.partition].remove_task, task)
        return task

    def _call_soon(self, callback, args):

        # find the partition by following the next strategies
        # 1 - The callback is a method realated to a task
        # 2 - If not, usual strategy
        try:
            if isinstance(callback.__self__, Task):
                partition = callback.__self__.partition
            else:
                partition = _find_partition(self)
        except AttributeError:
            partition = _find_partition(self)

        handle = events.Handle(callback, args, self)
        if handle._source_traceback:
            del handle._source_traceback[-1]
        self._partitions[partition].handles.append(handle)
        return handle

    def _add_reader(self, fd, callback, *args):
        super()._add_reader(fd, callback, *args)
        partition = _find_partition(self)
        self._partitions[partition].readers.add(fd)

    def _remove_reader(self, fd):
        super()._remove_reader(fd)
        partition = _find_partition(self)
        self._partitions[partition].readers.remove(fd)

    def _add_writer(self, fd, callback, *args):
        super()._add_reader(fd, callback, *args)
        partition = _find_partition(self)
        self._partitions[partition].writers.add(fd)

    def _remove_writer(self, fd):
        super()._remove_writer(fd)
        partition = _find_partition(self)
        self._partitions[partition].writers.remove(fd)

    def _process_events(self, event_list):
        for key, mask in event_list:
            fileobj, (reader, writer) = key.fileobj, key.data
            for partition in self._partitions_to_process:
                if fileobj in self._partitions[partion].readers or\
                    fileobj in self._partitions[partion].writers:
                    if mask & selectors.EVENT_READ and reader is not None:
                        if reader._cancelled:
                            self._remove_reader(fileobj)
                        else:
                            self._partitions[partition].handles.append(reader)
                    if mask & selectors.EVENT_WRITE and writer is not None:
                        if writer._cancelled:
                            self._remove_writer(fileobj)
                        else:
                            self._partitions[partition].handles.append(writer)
                    break

    def call_at(self, when, callback, *args):
        """Like call_later(), but uses an absolute time.

        Absolute time corresponds to the event loop's time() method.
        """
        self._check_closed()
        if self._debug:
            self._check_thread()
            self._check_callback(callback, 'call_at')
        timer = events.TimerHandle(when, callback, args, self)
        if timer._source_traceback:
            del timer._source_traceback[-1]

        partition = _find_partition(self)
        heapq.heappush(self._partitions[partition].scheduled, timer)
        timer._scheduled = True
        return timer

    def _run_once(self):
        """Run one full iteration of the event loop.

        This calls all currently ready callbacks, polls for I/O,
        schedules the resulting callbacks, and finally schedules
        'call_later' callbacks.

        Basically a copy of the original one, but running ready
        callbacks applying a round robin strategy between the differnet
        partitions. Once a queue, if it had at least one callback, runs out
        of callbacks the IO loop is requested again for its IO and time handles.
        """
        sched_count = sum(
            [len(self._partitions[partition].scheduled) for partition in self._partitions_to_process])

        if (sched_count > _MIN_SCHEDULED_TIMER_HANDLES and
             self._timer_cancelled_count / sched_count > _MIN_CANCELLED_TIMER_HANDLES_FRACTION):

            for partition in self._partitions_to_process:
                # Remove delayed calls that were cancelled if their number
                # is too high
                new_scheduled = []
                for handle in self._partitions[partition].scheduled:
                    if handle._cancelled:
                        handle._scheduled = False
                    else:
                        new_scheduled.append(handle)

                heapq.heapify(new_scheduled)
                self._partitions[partition].scheduled = new_scheduled
                self._timer_cancelled_count = 0
        else:
            for partition in self._partitions_to_process:
                # Remove delayed calls that were cancelled from head of queue.
                while self._partitions[partition].scheduled and self._partitions[partition].scheduled[0]._cancelled:
                    self._timer_cancelled_count -= 1
                    handle = heapq.heappop(self._partitions[partition].scheduled)
                    handle._scheduled = False

        timeout = None
        any_handles = any([bool(self._partitions[partition].handles) for partition in self._partitions])
        any_scheduled = any([bool(self._partitions[partition].scheduled) for partition in self._partitions_to_process])
        if any_handles or self._stopping:
            timeout = 0
        elif any_scheduled:
            # Compute the desired timeout.
            when = min([self._partitions[partition].scheduled[0]._when for partition in self._partitions_to_process])
            timeout = max(0, when - self.time())

        if self._debug and timeout != 0:
            t0 = self.time()
            event_list = self._selector.select(timeout)
            dt = self.time() - t0
            if dt >= 1.0:
                level = logging.INFO
            else:
                level = logging.DEBUG
            nevent = len(event_list)
            if timeout is None:
                logger.log(level, 'poll took %.3f ms: %s events',
                           dt * 1e3, nevent)
            elif nevent:
                logger.log(level,
                           'poll %.3f ms took %.3f ms: %s events',
                           timeout * 1e3, dt * 1e3, nevent)
            elif dt >= 1.0:
                logger.log(level,
                           'poll %.3f ms took %.3f ms: timeout',
                           timeout * 1e3, dt * 1e3)
        else:
            event_list = self._selector.select(timeout)
        self._process_events(event_list)

        # Handle 'later' callbacks that are ready.
        end_time = self.time() + self._clock_resolution
        for partition in self._partitions_to_process:
            while self._partitions[partition].scheduled:
                handle = self._partitions[partition].scheduled[0]
                if handle._when >= end_time:
                    break
                handle = heapq.heappop(self._partitions[partition].scheduled)
                handle._scheduled = False
                self._partitions[partition].handles.append(handle)

        partitions = [partition for partition in self._partitions if self._partitions[partition].handles]
        ntodo = max([len(self._partitions[partition].handles) for partition in self._partitions])
        cnt = 0
        partitions_to_process = set()
        handles_executed_per_partition = {partition:0 for partition in self._partitions}
        while not partitions_to_process and cnt < ntodo:
            for partition in partitions:
                try:
                    handle = self._partitions[partition].handles.popleft()
                except IndexError:
                    if handles_executed_per_partition[partition] > 0:
                        partitions_to_process.add(partition)
                    continue
                else:
                    handles_executed_per_partition[partition] += 1

                if handle._cancelled:
                    continue

                if self._debug:
                    try:
                        self._current_handle = handle
                        t0 = self.time()
                        handle._run()
                        dt = self.time() - t0
                        if dt >= self.slow_callback_duration:
                            logger.warning('Executing %s took %.3f seconds',
                                           _format_handle(handle), dt)
                    finally:
                        self._current_handle = None
                else:
                    handle._run()

            cnt += 1

        if partitions_to_process:
            self._partitions_to_process = partitions_to_process
        else:
            # keep with the same ones, we didnt run the queues.
            # FIXME : it can create starvation
            pass

        handle = None  # Needed to break cycles when an exception occurs.


class EventLoopPolicy(BaseDefaultEventLoopPolicy):
    _loop_factory = Loop
