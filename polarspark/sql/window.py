import sys
from typing import List, Union, Any, Optional, Tuple


class Window:
    unboundedPreceding: int = -sys.maxsize
    unboundedFollowing: int = sys.maxsize
    currentRow: int = 0

    def __init__(
        self,
        partition_by=None,
        order_by=None,
        frame: Optional[Tuple[str, int, int]] = None,
    ):
        self._partition_by = partition_by or []
        self._order_by = order_by or []
        self._frame = frame  # (type, start, end)

    @staticmethod
    def partitionBy(*cols: Union[str, List[str]]) -> "Window":
        w = Window()
        return w.partitionBy(*cols)

    @staticmethod
    def orderBy(*cols: Union[str, List[str]]) -> "Window":
        w = Window()
        return w.orderBy(*cols)

    def partitionBy(self, *cols: Union[str, List[str]]) -> "Window":
        normalized = list(self._partition_by)
        for c in cols:
            if isinstance(c, list):
                normalized.extend(c)
            else:
                normalized.append(c)
        return Window(
            partition_by=normalized, order_by=self._order_by, frame=self._frame
        )

    def orderBy(self, *cols: Union[str, List[str]]) -> "Window":
        normalized = list(self._order_by)
        for c in cols:
            if isinstance(c, list):
                normalized.extend(c)
            else:
                normalized.append(c)
        return Window(
            partition_by=self._partition_by, order_by=normalized, frame=self._frame
        )

    def rowsBetween(self, start: int, end: int) -> "Window":
        return Window(
            partition_by=self._partition_by,
            order_by=self._order_by,
            frame=("rows", start, end),
        )

    def rangeBetween(self, start: int, end: int) -> "Window":
        return Window(
            partition_by=self._partition_by,
            order_by=self._order_by,
            frame=("range", start, end),
        )
