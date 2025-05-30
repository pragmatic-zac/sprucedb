from typing import Optional, Protocol, TypeVar, Generic
import random

T = TypeVar('T')

class Comparable(Protocol):
    def __lt__(self, other: 'Comparable') -> bool: ...
    def __gt__(self, other: 'Comparable') -> bool: ...
    def __eq__(self, other: object) -> bool: ...

class Node(Generic[T]):
    def __init__(self, key: Comparable, value: T, level: int = 0) -> None:
        self.key = key
        self.value = value
        self.forward: list[Optional['Node']] = [None] * (level + 1)

class SkipList(Generic[T]):
    def __init__(self, p: float = 0.5, max_level: int = 4) -> None:
        self.max_level = max_level
        self.level = 0
        self.p = p
        self.head = Node(None, None, level=max_level - 1)
        
    def _create_node(self, key: Comparable, value: T, level: int) -> Node:
        return Node(key, value, level)
    
    def _random_level(self) -> int:
        level = 0
        while random.random() < self.p and level < self.max_level - 1:
            level += 1
        return level

    def insert(self, key: Comparable, value: T):
        # store update positions for each level
        update: list[Optional[Node]] = [None] * self.max_level
        current: Node = self.head

        # start at the highest level and work down
        for i in range(self.level, -1, -1):
            while (current.forward[i] and current.forward[i].key < key):
                current = current.forward[i]
            update[i] = current

        # generate random level for the new node
        level = self._random_level()

        # if the new level is greater than current
        if level > self.level:
            for i in range(self.level + 1, level + 1):
                update[i] = self.head
            self.level = level

        # create new node and update references
        new_node = self._create_node(key, value, level)
        for i in range(level + 1):
            new_node.forward[i] = update[i].forward[i]
            update[i].forward[i] = new_node

    def search(self, key: Comparable) -> Optional[T]:
        current: Node = self.head

        # start at the highest level and work down
        for i in range(self.level, -1, -1):
            while (current.forward[i] and current.forward[i].key < key):
                current = current.forward[i]

        current = current.forward[0]
        
        if current and current.key == key:
            return current.value
        
        return None

