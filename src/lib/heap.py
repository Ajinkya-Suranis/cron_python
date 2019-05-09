class heap:
    def __init__(self, key):
        self.heap_items = []
        self.heap_nitems = 0
        self.heap_current_min = None
        self.key = key

    def _search_heap(self, item, index=0):
        if index >= self.heap_nitems:
            return False
        if self.heap_items[index][self.key] == item[self.key]:
            return True
        lchild_index = (index << 1) + 1
        rchild_index = lchild_index + 1
        if lchild_index < self.heap_nitems and \
                self.heap_items[lchild_index][self.key] <= item[self.key]:
            if self._search_heap(item, lchild_index) == True:
                return True
        if rchild_index < self.heap_nitems and \
                self.heap_items[rchild_index][self.key] <= item[self.key]:
            return self._search_heap(item, rchild_index)
        return False

    def search_heap(self, item):
        if not self.heap_items:
            return False
        return self._search_heap(item)

    def insert_heap(self, new):
        if not self.heap_items:
            self.heap_items.append(new)
            self.heap_nitems = 1
            self.heap_current_min = self.heap_items[0][self.key]
            return
        found = self.search_heap(new)
        if found:
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
            return None
        if self.heap_nitems in [1, 2]:
            self.heap_nitems -= 1
            return self.heap_items.pop(0)
        min_item = self.heap_items[0]
        self.heap_items[0] = self.heap_items.pop()
        self.heap_nitems -= 1
        parent_index = 0
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
        return min_item