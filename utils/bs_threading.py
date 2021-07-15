import queue
import queue
import threading
import multiprocessing
from multiprocessing import Process, Queue, JoinableQueue
from threading import Thread
import time


class GenericWorker(Thread):

    def __init__(self, q, generic_function, should_terminate_func=None, auto_terminate=True, auto_terminate_duration=30):
        Thread.__init__(self)
        
        self.q = q
        self.generic_function = generic_function
        self.should_terminate_func = should_terminate_func
        self.auto_terminate = auto_terminate
        self.auto_terminate_duration = auto_terminate_duration
        
    def run(self):
        while True:
            try:
                data_dict = self.q.get(timeout=self.auto_terminate_duration)
            except queue.Empty:
                if self.auto_terminate:
                    break
                data_dict = None
                
            if data_dict is not None:
                try:
                    self.generic_function(data_dict)
                finally:
                    self.q.task_done()
                if self.should_terminate_func is not None:
                    if self.should_terminate_func():
#                         print("TERMINATING THE THREAD")
                        break

def bs_multiprocessify(worker_data_list, worker_func, num_processes=8, auto_terminate_duration=10):
    
    for worker_data in worker_data_list:
        q.put(worker_data)
        
    def worker_loop_func(q):
        while True:
            print("inside worker loop")
            try:
                data_dict = q.get(timeout=self.auto_terminate_duration)
            except queue.Empty:
                data_dict=None
                break
                     
        if data_dict is not None:
            try:
                worker_func(data_dict)
            finally:
                q.task_done()
            
    all_processes = []
    for x in range(num_threads):
        p = Process(target=worker_func, args=(q,))
        p.start()
        all_processes.append(p)
        
    
        
    for p in all_processes:
        p.join()
#     q.join()
    
def bs_threadify(worker_data_list, worker_func, num_threads=8, should_terminate_func=None, auto_terminate=True, auto_terminate_duration=30):
    q = queue.Queue()
    # Create 8 worker threads
    all_workers = []
    for x in range(num_threads):
        worker = GenericWorker(q, worker_func, should_terminate_func=should_terminate_func, auto_terminate=auto_terminate, auto_terminate_duration=auto_terminate_duration)
        # Setting daemon to True will let the main thread exit even though the workers are blocking
        worker.daemon = True
        worker.start()
        all_workers.append(all_workers)
    # Put the tasks into the queue as a tuple
    for worker_data in worker_data_list:
        q.put(worker_data)
    # Causes the main thread to wait for the queue to finish processing all the tasks
    q.join()
    return queue

# make sure to do sleep yourself
def bs_make_throttle_ready_func(min_interval_second=1.0/3.0):
    last_accessed_time = time.time()
    lock = Lock()
    def ready_function():
        readystate = False
        nonlocal last_accessed_time

        # blocking messes things up, dont' block here, python functions are not called concurrently
        # so this is a shared function and multiple threads will wait to call this
        # if you block here, then it's pointless to have multiple threads
#         if blocking == False:
#             with lock:
#                 if time.time() - last_accessed_time > min_interval_second:
#                     last_accessed_time = time.time()
#                     readystate = True
#         else:

        while not readystate:
            with lock:
                if time.time() - last_accessed_time > min_interval_second:
                    last_accessed_time = time.time()
                    readystate = True
            if readystate == True:
                return readystate
            time.sleep(0.015)
                
        return readystate
    return ready_function




def main():
    print("tet")


if __name__ == "__main__":
    main()