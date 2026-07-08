class TimeTravel:
    def __init__(self):
        self._now = 0.0

    def monotonic(self):
        return self._now

    def sleep(self, seconds):
        self._now += seconds
