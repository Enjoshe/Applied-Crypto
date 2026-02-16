# rotating_windows_testing.py
from __future__ import annotations
import random
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

from rotating_windows_protocol import RotatingOwnershipWindows, Message


@dataclass
class SimConfig:
    n: int = 10000
    m: int = 4
    d: int = 10           # max undelivered messages
    w: int = 8            # window size
    g: int = 0            # gap between windows
    payload_len: int = 32
    deliver_prob: float = 0.6
    max_steps: int = 2_000_000


def pick_active_parties(m: int, x: int, rng: random.Random) -> List[int]:
    return rng.sample(list(range(m)), k=x)


def maybe_deliver_some(pending: List[Message], proto: RotatingOwnershipWindows, rng: random.Random, deliver_prob: float):
    if not pending:
        return
    if rng.random() > deliver_prob:
        return

    k = rng.randint(1, max(1, len(pending) // 2))
    for _ in range(k):
        if not pending:
            break
        idx = rng.randrange(len(pending))
        msg = pending.pop(idx)
        proto.deliver(msg)


def run_one_execution(cfg: SimConfig, active: List[int], seed: Optional[int] = None) -> Tuple[int, int, int]:
    rng = random.Random(seed)
    proto = RotatingOwnershipWindows(n=cfg.n, m=cfg.m, w=cfg.w, g=cfg.g)
    pending: List[Message] = []

    steps = 0
    while steps < cfg.max_steps:
        steps += 1

        # Enforce <= d undelivered
        if len(pending) >= cfg.d:
            maybe_deliver_some(pending, proto, rng, deliver_prob=1.0)

        maybe_deliver_some(pending, proto, rng, cfg.deliver_prob)

        sender = rng.choice(active)
        if not proto.party_can_send(sender):
            break

        payload = bytes(rng.getrandbits(8) for _ in range(cfg.payload_len))
        msg = proto.send(sender, payload)
        pending.append(msg)

    return proto.pads_used(), proto.wasted_pads(), steps


def run_trials(cfg: SimConfig, x: int, trials: int, seed: int = 1234) -> Dict[str, float]:
    rng = random.Random(seed)
    wastes: List[int] = []
    useds: List[int] = []
    steps_list: List[int] = []

    for _ in range(trials):
        active = pick_active_parties(cfg.m, x, rng)
        used, wasted, steps = run_one_execution(cfg, active, seed=rng.getrandbits(64))
        wastes.append(wasted)
        useds.append(used)
        steps_list.append(steps)

    return {
        "m": cfg.m,
        "x": x,
        "trials": trials,
        "avg_wasted": sum(wastes) / trials,
        "min_wasted": min(wastes),
        "max_wasted": max(wastes),
        "avg_used": sum(useds) / trials,
        "avg_steps": sum(steps_list) / trials,
    }


def main():
    cfg = SimConfig(
        n=10000,
        m=4,
        d=10,
        w=8,
        g=0,
        payload_len=32,
        deliver_prob=0.6
    )

    trials = 300
    xs = [1, 2, 4] if cfg.m == 4 else [1, 2, 3]

    for x in xs:
        stats = run_trials(cfg, x=x, trials=trials, seed=9000 + x)
        print(f"Scenario S.{x} | m={cfg.m}, n={cfg.n}, d={cfg.d}, w={cfg.w}, g={cfg.g}, trials={trials}")
        print(f"  avg_wasted: {stats['avg_wasted']:.2f} (min={stats['min_wasted']}, max={stats['max_wasted']})")
        print(f"  avg_used:   {stats['avg_used']:.2f} | avg_steps: {stats['avg_steps']:.2f}")
        print("")


if __name__ == "__main__":
    main()
