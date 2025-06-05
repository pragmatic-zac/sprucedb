from typing import Optional, Protocol, TypeVar, Generic, List, Any
import random

T = TypeVar('T')

class Comparable(Protocol):
    def __lt__(self, other: Any) -> bool: ...
    def __gt__(self, other: Any) -> bool: ...
    def __eq__(self, other: object) -> bool: ...

class Node(Generic[T]):
    def __init__(self, key: Optional[Comparable], value: Optional[T], level: int = 0) -> None:
        self.key = key
        self.value = value
        self.forward: List[Optional['Node[T]']] = [None] * (level + 1)

class SkipList(Generic[T]):
    def __init__(self, p: float = 0.5, max_level: int = 4) -> None:
        self.max_level = max_level
        self.level = 0
        self.p = p
        self.head: Node[T] = Node(None, None, level=max_level - 1)
        
    def _create_node(self, key: Comparable, value: T, level: int) -> Node[T]:
        return Node(key, value, level)
    
    def _random_level(self) -> int:
        level = 0
        while random.random() < self.p and level < self.max_level - 1:
            level += 1
        return level

    def insert(self, key: Comparable, value: T) -> None:
        update: List[Optional[Node[T]]] = [None] * self.max_level
        current: Optional[Node[T]] = self.head

        for i in range(self.level, -1, -1):
            while True:
                next_node = current.forward[i] if current is not None else None
                if next_node is not None and next_node.key is not None and next_node.key < key:
                    current = next_node
                else:
                    break
            update[i] = current

        level = self._random_level()

        if level > self.level:
            for i in range(self.level + 1, level + 1):
                update[i] = self.head
            self.level = level

        new_node = self._create_node(key, value, level)
        for i in range(level + 1):
            updater = update[i]
            if updater is not None:
                new_node.forward[i] = updater.forward[i]
                updater.forward[i] = new_node

    def search(self, key: Comparable) -> Optional[T]:
        current: Optional[Node[T]] = self.head

        for i in range(self.level, -1, -1):
            while True:
                next_node = current.forward[i] if current is not None else None
                if next_node is not None and next_node.key is not None and next_node.key < key:
                    current = next_node
                else:
                    break

        if current is not None:
            current = current.forward[0]

        if current is not None and current.key is not None and current.key == key:
            return current.value
        return None

    def delete(self, key: Comparable) -> None:
        update: List[Optional[Node[T]]] = [None] * self.max_level
        current: Optional[Node[T]] = self.head

        for i in range(self.level, -1, -1):
            while True:
                next_node = current.forward[i] if current is not None else None
                if next_node is not None and next_node.key is not None and next_node.key < key:
                    current = next_node
                else:
                    break
            update[i] = current

        if current is not None:
            current = current.forward[0]

        if current is not None and current.key is not None and current.key == key:
            for i in range(self.level + 1):
                updater = update[i]
                if updater is not None and updater.forward[i] == current:
                    updater.forward[i] = current.forward[i]
            while self.level > 0 and self.head.forward[self.level] is None:
                self.level -= 1
