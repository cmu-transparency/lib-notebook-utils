import threading as th
import copy

# http://nbviewer.jupyter.org/gist/minrk/4563193
import sys
import threading
import time
from contextlib import contextmanager

import util.misc as misc

# we need a lock, so that other threads don't snatch control
# while we have set a temporary parent
stdout_lock = threading.Lock()

@contextmanager
def set_stdout_parent(parent):
    """a context manager for setting a particular parent for sys.stdout
    
    the parent determines the destination cell of output
    """
    save_parent = sys.stdout.parent_header
    with stdout_lock:
        sys.stdout.parent_header = parent
        try:
            yield
        finally:
            # the flush is important, because that's when the parent_header actually has its effect
            sys.stdout.flush()
            sys.stdout.parent_header = save_parent

class Tasker(th.Thread):
    def __init__(self, filename, input_maker, evaluator):
        th.Thread.__init__(self)

        self.filename = filename

        self.tasks = list(input_maker())
        self.evaluator = evaluator
        
        self.completed_lock = th.Lock()
        self.completed_all = misc.load_or_new(filename, dict())
        self._update_num_completed()
 
        self.num_tasks = len(self.tasks)
        
        self.running = False

    def reset(self):
        if (os.path.exists(self.filename)): os.remove(self.filename)
        self.completed = dict()

    def _update_num_completed(self):
        self.completed = self.get_completed_of_tasks()
        self.num_completed_all = len(self.completed_all.keys())
        self.num_completed = len(self.completed.keys())
        
    def get_completed_of_tasks(self):
        ret = {}
        for task in self.tasks:
            if task in self.completed_all:
                ret[task] = self.completed_all[task]
        return ret

    def get_completed_all(self):
        temp = None
        with self.completed_lock:
            temp = copy.copy(self.completed_all)
        return temp

    def get_completed(self):
        temp = None
        with self.completed_lock:
            temp = copy.copy(self.completed)
        return temp
    
    def stop(self):
        self.running = False

    def __str__(self):
        return "[{self.num_completed}/{self.num_tasks} {self.num_completed_all}]".format(**locals())
        
    def run(self):
        self.running = True
        thread_parent = sys.stdout.parent_header
        with set_stdout_parent(thread_parent):
            for task in self.tasks:
                if task in self.completed:
                    #misc.printme("*")
                    continue
                misc.printme("{self} running: {task} ".format(**locals()))
                result = self.evaluator(task)
                with self.completed_lock:
                    self.completed_all[task] = result
                    self._update_num_completed()
                misc.printme(" done\n")
                misc.save(self.filename, self.completed_all)
                if not self.running: return