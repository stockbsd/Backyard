#! python3
# coding=utf-8

class Movie():
    def __init__(self, time, title, score, url):
        self._score = 0

        self.time = time
        self.title = title
        self.score = score
        self.url = url

    @property
    def score(self):
        return self._score

    @score.setter
    def score(self, value):
        if value < 0:
            raise ValueError('Noops')
        self._score = value

m = Movie('Sat', 'Sting', 8.0, 'http://imdb.com/kasd')
print(m.score)

try:
    m.score = -7
except:
    print('Wrong')

## descriptor
from weakref import WeakKeyDictionary 
class NonSense():
    def __init__(self, defv):
        self.defv = defv
        self.data = WeakKeyDictionary()
    def __get__(self, ins, owner):
        print('get', ins, owner)
        return self.defv if ins is None else self.data.get(ins, self.defv)
    def __set__(self,ins,value):
        print('set', ins, value)
        self.data[ins] = value

class Destest():
    n = NonSense('Zero')

    def __init__(self, s):
        self.n = s

d = Destest('Jack')
e = Destest('King')
e. n = 36
print(d.n, e.n, Destest.n)
print(d.__dict__, Destest.__dict__)
