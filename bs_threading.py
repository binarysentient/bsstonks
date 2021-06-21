from queue import Queue
from threading import Thread, Lock
import time

class GenericWorker(Thread):

    def __init__(self, queue, generic_function):
        Thread.__init__(self)
        self.queue = queue
        self.generic_function = generic_function

    def run(self):
        while True:
            data_dict = self.queue.get()
            try:
                self.generic_function(data_dict)
            finally:
                self.queue.task_done()
                

def bs_threadify(worker_data_list, worker_func, num_threads=8):
    queue = Queue()
    # Create 8 worker threads
    for x in range(num_threads):
        worker = GenericWorker(queue, worker_func)
        # Setting daemon to True will let the main thread exit even though the workers are blocking
        worker.daemon = True
        worker.start()
    # Put the tasks into the queue as a tuple
    for worker_data in worker_data_list:
        queue.put(worker_data)
    # Causes the main thread to wait for the queue to finish processing all the tasks
    queue.join()
    return queue

# make sure to do sleep yourself
def bs_make_throttle_ready_func(min_interval_second=1.0/3.0):
    last_accessed_time = time.time()
    lock = Lock()
    def ready_function():
        readystate = False
        nonlocal last_accessed_time
        
        with lock:
            if time.time() - last_accessed_time > min_interval_second:
                last_accessed_time = time.time()
                readystate = True
        return readystate
    return ready_function


