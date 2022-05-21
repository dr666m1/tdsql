from dataclasses import dataclass

@dataclass
class TdsqlTestConfig():
    max_bytes_billed: int = 1024 ** 3 # 1GiB
    max_results: int = 1000
    auto_sort: bool = True
    acceptable_error: float = 0.05

