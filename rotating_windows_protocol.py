# rotating_windows_protocol.py
from __future__ import annotations
from dataclasses import dataclass
from typing import Dict, List, Optional, Set, Tuple


@dataclass(frozen=True)
class Message:
    sender: int
    pad_index: int
    payload: bytes


@dataclass(frozen=True)
class Window:
    start: int  # inclusive, 1-indexed
    end: int    # inclusive


class RotatingOwnershipWindows:


    def __init__(self, n: int, m: int, w: int, g: int = 0):
        if n <= 0:
            raise ValueError("n must be positive")
        if m < 2:
            raise ValueError("m must be >= 2")
        if w <= 0:
            raise ValueError("w must be positive")
        if g < 0:
            raise ValueError("g must be >= 0")

        self.n = n
        self.m = m
        self.w = w
        self.g = g
        self.stride = w + g

        # Number of full windows (only full windows)
        self.num_windows = n // self.stride
        self.windows: List[Window] = []
        for b in range(self.num_windows):
            start = 1 + b * self.stride
            end = start + w - 1
            self.windows.append(Window(start, end))

        # ---- Party state ----
        # next preferred RR window index per party
        self.next_rr: Dict[int, int] = {pid: pid for pid in range(m)}
        # current window that pid has claimed and is consuming
        self.cur_window: Dict[int, Optional[int]] = {pid: None for pid in range(m)}
        # offset inside current window [0..w-1]
        self.offset: Dict[int, int] = {pid: 0 for pid in range(m)}

        # ---- Global claim state ----
        # claimed_by[b] = pid if claimed else -1
        self.claimed_by: List[int] = [-1] * self.num_windows
        # pointer to next globally unclaimed window (for fast reclaim)
        self.next_unclaimed: int = 0

        # For correctness checking
        self.used_pads: Set[int] = set()

    def _advance_next_unclaimed(self) -> None:
        while self.next_unclaimed < self.num_windows and self.claimed_by[self.next_unclaimed] != -1:
            self.next_unclaimed += 1

    def _claim_window(self, pid: int) -> Optional[int]:
        """
        Claim a new window for pid.
        Preference order:
          1) pid's next_rr window if available and unclaimed
          2) next globally unclaimed window (reclaim)
        Returns claimed window index or None if none left.
        """
        # 1) Try preferred RR window
        rr = self.next_rr[pid]
        if rr < self.num_windows and self.claimed_by[rr] == -1:
            self.claimed_by[rr] = pid
            self.cur_window[pid] = rr
            self.offset[pid] = 0
            self.next_rr[pid] += self.m
            return rr

        # Even if rr is already claimed or out of range, we still advance RR pointer
        # so the party's preferred stream continues moving forward.
        if rr < self.num_windows:
            self.next_rr[pid] += self.m

        # 2) Reclaim next globally unclaimed window
        self._advance_next_unclaimed()
        if self.next_unclaimed >= self.num_windows:
            return None

        b = self.next_unclaimed
        self.claimed_by[b] = pid
        self.cur_window[pid] = b
        self.offset[pid] = 0
        self.next_unclaimed += 1  # move forward (will be cleaned by _advance_next_unclaimed when needed)
        return b

    def party_can_send(self, pid: int) -> bool:
        """
        Party can send if it either has a current window with remaining pads,
        or can claim a new window.
        """
        b = self.cur_window[pid]
        if b is not None and self.offset[pid] < self.w:
            return True

        # If no current window or window exhausted, see if any window remains claimable
        # (We do not permanently mutate state here; claim happens in send()).
        # Quick check: if there exists any unclaimed window or rr is unclaimed.
        rr = self.next_rr[pid]
        if rr < self.num_windows and self.claimed_by[rr] == -1:
            return True
        # Or global unclaimed remains
        # We won't scan; use next_unclaimed pointer
        tmp = self.next_unclaimed
        while tmp < self.num_windows and self.claimed_by[tmp] != -1:
            tmp += 1
        return tmp < self.num_windows

    def _current_pad_index(self, pid: int) -> int:
        b = self.cur_window[pid]
        if b is None:
            raise RuntimeError("Party has no current window")
        win = self.windows[b]
        return win.start + self.offset[pid]

    def send(self, pid: int, payload: bytes) -> Message:
        """
        Consume exactly 1 pad index owned by pid and emit a broadcast message.
        """
        # If no current window or exhausted, claim a new one
        if self.cur_window[pid] is None or self.offset[pid] >= self.w:
            claimed = self._claim_window(pid)
            if claimed is None:
                raise RuntimeError(f"Party {pid} cannot send: no windows left to claim")

        pad_index = self._current_pad_index(pid)

        # Correctness check: no pad reuse
        if pad_index in self.used_pads:
            raise RuntimeError(f"Pad reuse detected: pad_index={pad_index}")
        self.used_pads.add(pad_index)

        # Advance within window
        self.offset[pid] += 1
        if self.offset[pid] >= self.w:
            # mark current window exhausted; next send will claim a new one
            self.cur_window[pid] = None

        return Message(sender=pid, pad_index=pad_index, payload=payload)

    def deliver(self, msg: Message) -> None:
        # Delivery doesn't affect ownership in this protocol.
        return

    def pads_used(self) -> int:
        return len(self.used_pads)

    def wasted_pads(self) -> int:
        return self.n - self.pads_used()

    def protocol_capacity_in_messages(self) -> int:
        # Total pads inside windows (gaps/tail excluded)
        return self.num_windows * self.w
