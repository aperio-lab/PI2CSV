import asyncio

# Stream Merge helper class
# It allows to manage a queue of streams, adding new streams to an existing queue,
# all without interrupting an open stream.
#
# Example:
# self.iterators_queue = [self.retrieve_stream_set_updates(), self.retrieve_stream_set_updates()]
# self.merged_iterators = StreamMerge(self._logger, *self.iterators_queue)
# try:
#   async for events in self.merged_iterators:
#       yield events
# except CancelledError as err:
    # handle exception
# except StopAsyncIteration:
#   # handle exception
#
# add new iterator task to an existing queue:
# self.merged_iterators.append_iter(self.retrieve_stream_set_updates())
#

class StreamMerge:
    def __init__(self, logger, *iterables):
        self._iterables = list(iterables)
        self._wakeup = asyncio.Event()
        self._logger = logger

    def _add_iters(self, next_futs, on_done):
        for it in self._iterables:
            it = it.__aiter__()
            nfut = asyncio.ensure_future(it.__anext__())
            nfut.add_done_callback(on_done)
            next_futs[nfut] = it
        del self._iterables[:]
        return next_futs

    async def __aiter__(self):
        done = {}
        next_futs = {}

        def on_done(nfut):
            done[nfut] = next_futs.pop(nfut)
            self._wakeup.set()

        self._add_iters(next_futs, on_done)
        try:
            while next_futs:
                await self._wakeup.wait()
                self._wakeup.clear()
                for nfut, it in done.items():
                    try:
                        ret = nfut.result()
                    except StopAsyncIteration as e:
                        self._logger.debug(f'StreamMerge: StopAsyncIteration exception with error: {e}')
                        continue
                    except Exception as e:
                        self._logger.debug(f'StreamMerge: Some exception with error: {e}')
                        continue
                    self._iterables.append(it)
                    yield ret
                done.clear()
                if self._iterables:
                    self._add_iters(next_futs, on_done)
        finally:
            # if the generator exits with an exception, or if the caller stops
            # iterating, make sure our callbacks are removed
            self._logger.debug(f'StreamMerge: caller stopped iterating')

            for nfut in next_futs:
                nfut.remove_done_callback(on_done)
            raise StopAsyncIteration

    def append_iter(self, new_iter):
        self._iterables.append(new_iter)
        self._wakeup.set()

