import random
import string
from cron_exceptions import *

UUID_NCHAR = 6

class heap:
    def __init__(self, key):
        self.heap_items = []
        self.heap_nitems = 0
        self.heap_current_min = None
        self.uuid = self.generate_uuid()
        self.key = key
    
    def generate_uuid(self):
        return ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(UUID_NCHAR))

    def _search_heap(self, item, index=0):
        if index >= self.heap_nitems:
            return None, None
        if self.heap_items[index][self.key] == item[self.key]:
            return self.heap_items[index], index
        lchild_index = (index << 1) + 1
        rchild_index = lchild_index + 1
        if lchild_index < self.heap_nitems and \
                self.heap_items[lchild_index][self.key] <= item[self.key]:
            sitem, sindex = self._search_heap(item, lchild_index)
            if sitem != None:
                assert sindex != None, "Found None index while searching in heap " + self.uuid
                return sitem, sindex
        if rchild_index < self.heap_nitems and \
                self.heap_items[rchild_index][self.key] <= item[self.key]:
            return self._search_heap(item, rchild_index)
        return None, None

    def _heapify_down(self, parent_index):
        while True:
            lchild_index = (parent_index << 1) + 1
            rchild_index = lchild_index + 1
            if lchild_index >= self.heap_nitems:
                break
            min_index = lchild_index if rchild_index >= self.heap_nitems or \
                            self.heap_items[lchild_index][self.key] < \
                            self.heap_items[rchild_index][self.key] \
                            else rchild_index
            if self.heap_items[min_index][self.key] < self.heap_items[parent_index][self.key]:
                self.heap_items[parent_index], self.heap_items[min_index] = \
                        self.heap_items[min_index], self.heap_items[parent_index]
                parent_index = min_index
            else:
                break
        self.heap_current_min = self.peek_min()[self.key] if self.heap_nitems > 0 else None

    def search_heap(self, item):
        if not self.heap_items:
            return None, None
        return self._search_heap(item)

    def insert_heap(self, new):
        if not self.heap_items:
            self.heap_items.append(new)
            self.heap_nitems = 1
            self.heap_current_min = self.heap_items[0][self.key]
            return
        item, _index = self.search_heap(new)
        if item !=  None:
            print("Item already exists")
            return
        parent_index = self.heap_nitems
        self.heap_items.append(new)
        self.heap_nitems += 1
        while True:
            current_index = parent_index
            parent_index = (current_index >> 1) - 1 if current_index % 2 == 0 else current_index >> 1
            if parent_index < 0 or self.heap_items[parent_index][self.key] < \
                    self.heap_items[current_index][self.key]:
                break
            self.heap_items[parent_index], self.heap_items[current_index] = \
                self.heap_items[current_index], self.heap_items[parent_index]
        self.heap_current_min = self.peek_min()[self.key]

    def peek_min(self):
        if self.heap_nitems == 0:
            return None
        return self.heap_items[0]

    def remove_min(self):
        if not self.heap_items:
            assert self.heap_nitems == 0, "number of items in heap are inconsistent"
            raise HeapEmpty("The heap with uuid " + self.uuid + " is empty")
        if self.heap_nitems in [1, 2]:
            self.heap_nitems -= 1
            return self.heap_items.pop(0)
        min_item = self.heap_items[0]
        self.heap_items[0] = self.heap_items.pop()
        self.heap_nitems -= 1
        self._heapify_down(0)
        return min_item
    
    # When we remove a particular item from heap, it's not returned to
    # the caller, since the caller already knows its value.
    def remove(self, item):
        if not self.heap_items:
            assert False, "Attepmt to remove from empty heap with uuid " + self.uuid
        if self.heap_nitems == 1:
            assert item[self.key] == self.heap_items[0][self.key], "Key mismatch at the root of heap"
            self.heap_items.pop()
            self.heap_nitems = 0
            return
        # If the item matches with root, then call self.remove_min()
        # to remove and heapify the tree.
        if item[self.key] == self.heap_items[0][self.key]:
            self.remove_min()
            return
        last_item = self.heap_items.pop()
        self.heap_nitems -= 1
        # If the last item in heap becomes a match, then simply remove it
        # from heap and return.
        if last_item[self.key] == item[self.key]:
            return
        check_item, index = self.search_heap(item)
        assert check_item != None, "Expected heap item with key " + str(self.key) + " not found"
        self.heap_items[index] = last_item
        pindex = (index >> 1) - 1 if index % 2 == 0 else index >> 1
        if self.heap_items[pindex][self.key] > self.heap_items[index][self.key]:
            while True:
                pindex = (index >> 1) - 1 if index % 2 == 0 else index >> 1
                if pindex < 0 or self.heap_items[pindex][self.key] < \
                            self.heap_items[index][self.key]:
                        break
                self.heap_items[pindex], self.heap_items[index] = \
                    self.heap_items[index], self.heap_items[pindex]
                index = pindex
        else:
            self._heapify_down(index)