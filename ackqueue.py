#coding:utf-8
from __future__ import unicode_literals
import heapq
import itertools


DISCARDED = 'DEL'

def discard(queue):
    return 1

class Queue(object):
    def __init__(self, maxsize=0, full_policy=discard):
        self.size = 0
        self.truesize = 0
        self.maxsize = maxsize
        self.full_policy = full_policy
        self.heaps = {}
        self.lookup = {}
        self.counter = itertools.count()
    
    def full(self):
        return self.maxsize > 0 and self.size >= self.maxsize
    
    def push(self, item, priority=1, category='default'):
        if self.full() and self.full_policy(self):
            return
        if item in self.lookup:
            raise ValueError('Duplicate item values are not allowed')
        
        q = self.heaps.get(category, [])
        self.heaps[category] = q
        count = next(self.counter)
        entry = [priority, count, category, item]
        self.lookup[item] = entry
        heapq.heappush(q, entry)
        self.size += 1
        self.truesize += 1
    
    def exists(self, item):
        return item in self.lookup
    
    def discard(self, item):
        entry = self.lookup.pop(item)
        entry[-1] = DISCARDED
        self.size -= 1
        
    def compact(self):
        for cat, q in self.heaps.items():
            newq = [ x for x in q if x[-1] is not DISCARDED ]
            heapq.heapify(newq)
            self.heaps[cat] = newq
    
    def purge(self):
        self.size = 0
        self.truesize = 0
        self.heaps = {}
        self.lookup = {}
    
    def pop(self, count=None, categories=None, ratios=None):
        if ratios and not count:
            raise ValueError('Must specify a count threshold when using ratios')
        items = []
        threshold = count or 1
        heaps = [ y for x, y in self.heaps.items()
                     if not categories or x in categories ]
        value = 0
        while value < threshold:
            entry = self._pop(heaps)
            if not entry:
                break
            item = entry[-1]
            cat = entry[-2]
            self.truesize -= 1
            if item is not DISCARDED:
                del self.lookup[item]
                self.size -= 1
                items.append(item)
                if categories and ratios:
                    value += ratios[categories.index(cat)]
                else:
                    value += 1 
        
        if not count:
            return (items and items[0]) or None
        return items
    
    def _pop(self, heaps):
        smallest = [ x[0] for x in heaps if x ]
        if not smallest:
            return None
        entry = min(smallest)
        return heapq.heappop(self.heaps[entry[-2]])
        
    def __len__(self):
        return self.size
    
    def __nonzero__(self):
        return self.size > 0
    
    def __bool__(self):
        return self.__nonzero__()
    
        
