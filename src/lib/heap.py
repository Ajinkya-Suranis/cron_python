class heap:
    def __init__(self):
        self.heap_items = []
        self.heap_nitems = 0

    def _search_heap(self, item, index=0):
        if index >= self.heap_nitems:
            return False
        if self.heap_items[index] == item:
            return True
        lchild_index = (index << 1) + 1
        rchild_index = lchild_index + 1
        if lchild_index < self.heap_nitems and self.heap_items[lchild_index] <= item:
            if self._search_heap(item, lchild_index) == True:
                return True
        if rchild_index < self.heap_nitems and self.heap_items[rchild_index] <= item:
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
            if parent_index < 0 or self.heap_items[parent_index] < self.heap_items[current_index]:
                break
            self.heap_items[parent_index], self.heap_items[current_index] = \
                self.heap_items[current_index], self.heap_items[parent_index]

    def remove_min(self, item):
        if not self.heap_items or self.search_heap(item) == False:
            return
        self.heap_items.pop(0)
        self.heap_nitems -= 1
        if self.heap_nitems in [0, 1]:
            return