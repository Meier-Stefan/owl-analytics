import sys


class _Tee:
    def __init__(self, f):
        self.f = f

    def write(self, text):
        sys.__stdout__.write(text)
        return self.f.write(text)

    def flush(self):
        sys.__stdout__.flush()
        self.f.flush()
