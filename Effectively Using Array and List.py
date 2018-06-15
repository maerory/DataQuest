class Node():
    def __init__(self, value):
        self.value = value
        self.next_node = None
    
    def set_next_node(self, node):
        self.next_node = node
    
    def append(self, value):
        next_node = Node(value)
        self.next_node = next_node
        return next_node
    
    def __getitem__(self, key):
        node = self
        counter = 0
        while counter < key:
            node = node.next_node
            counter += 1
        return node
    
    def insert(self, position, value):
        if position == 0:
            node = Node(value)
            node.next_node = self
            return node
        else:
            node = Node(value)
            split_start = self[position - 1]
            split_end = split_start.next_node
            split_start.next_node = node
            node.next_node = split_end
            return self
    
    def pop(self, position):
        if position == 0:
            return self, self.next_node
        else:
            split_start = self[position-1]
            removed = split_start.next_node
            split_end = split_start.next_node.next_node
            split_start.next_node = split_end
            return removed, self
        
price_1 = Node(all_prices[0])
node = price_1
for i in all_prices[1:5]:
    node = node.append(i)

removed, price_1 = price_1.pop(0)
removed, price_1 = price_1.pop(3)

print(price_1[2].value)
