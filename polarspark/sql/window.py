from typing import List, Union, Any

class Window:
    def __init__(self, partition_by=None, order_by=None):
        self._partition_by = partition_by or []
        self._order_by = order_by or []

    def partitionBy(self, *cols: Union[str, List[str]]) -> "Window":
        if isinstance(self, Window):
            target = self
            actual_cols = cols
        else:
            target = Window()
            actual_cols = [self] + list(cols)
            
        normalized = list(target._partition_by)
        for c in actual_cols:
            if isinstance(c, list): normalized.extend(c)
            else: normalized.append(c)
        return Window(partition_by=normalized, order_by=target._order_by)

    def orderBy(self, *cols: Union[str, List[str]]) -> "Window":
        if isinstance(self, Window):
            target = self
            actual_cols = cols
        else:
            target = Window()
            actual_cols = [self] + list(cols)
            
        normalized = list(target._order_by)
        for c in actual_cols:
            if isinstance(c, list): normalized.extend(c)
            else: normalized.append(c)
        return Window(partition_by=target._partition_by, order_by=normalized)
