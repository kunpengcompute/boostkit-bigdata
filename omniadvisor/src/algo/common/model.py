from dataclasses import dataclass

# 临时用于测试
@dataclass
class Tuning:
    round: int
    config: dict
    method: str
    rule: str
    status: str
    runtime: float
    trace: dict
