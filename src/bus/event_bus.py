from typing import Callable, Dict, List, TypeVar, Any

from src.bus.events.base import BaseEvent

T = TypeVar('T')

class EventBus:
    def __init__(self) -> None:
        self.listeners: Dict[BaseEvent, List[Callable]] = {}

    def on(self, event_type: BaseEvent) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
            if event_type not in self.listeners:
                self.listeners[event_type] = []
            self.listeners[event_type].append(func)
            return func
        return decorator