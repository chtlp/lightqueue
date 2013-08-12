# import redis
from multiprocessing import Process
# from queue import Queue
from worker import Worker


class SequentialDispatcher(object):
    # Only one worker; jobs are pulled off the queue one at a time and
    # processed sequentially.

    def __init__(self, host, port, db, queue_name, time_out):
        self.host = host
        self.port = port
        self.db = db
        self.queue_name = queue_name
        self.time_out = time_out

    def dispatch(self):
        # Create a single worker
        Worker(self.host, self.port, self.db, self.queue_name,
               self.time_out).work()


class ParallelDispatcher(object):
    # Use multiprocessing to launch worker processes

    def __init__(self, workers, host, port, db, queue_name, time_out):
        self.workers = workers
        self.host = host
        self.port = port
        self.db = db
        self.queue_name = queue_name
        self.time_out = time_out

    def dispatch(self):
        # Create a new process for each worker
        for x in range(self.workers):
            Process(target=Worker(self.host, self.port, self.db,
                    self.queue_name, self.time_out).work).start()
