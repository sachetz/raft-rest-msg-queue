import random
from threading import Timer

class ResettableTimer:
    def __init__(self, function, interval_lb, interval_ub, node_id, timer_type):
        self.interval = (interval_lb, interval_ub)
        self.function = function
        self.node_id = node_id
        self.timer_type = timer_type
        self.timer = Timer(self._interval(), self.function)

    def _interval(self):
        interval = random.randint(*self.interval) / 1000
        print(f"Setting {self.timer_type} timer of node {self.node_id} to {interval} generated from {self.interval}")
        return interval

    def run(self):
        self.timer.start()

    def reset(self):
        self.timer.cancel()
        self.timer = Timer(self._interval(), self.function)
        self.timer.start()
    
    def stop(self):
        self.timer.cancel()
