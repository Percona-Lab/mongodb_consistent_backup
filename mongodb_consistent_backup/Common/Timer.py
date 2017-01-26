from time import time


class Timer:
    def __init__(self):
        self.count  = 0
        self.rounds = {}

    def start(self):
        self.count += 1
        self.rounds[self.count] = { 'start': time(), 'started': True }

    def stop(self):
        if self.rounds[self.count] and self.rounds[self.count]['started']:
            self.rounds[self.count]['started'] = False
            self.rounds[self.count]['end']     = time()

    def duration(self):
        if 'start' in self.rounds[self.count]:
            if 'end' in self.rounds[self.count]:
                end = self.rounds[self.count]['end']
            else:
                end = time()
            return end - self.rounds[self.count]['start']
        return -1
