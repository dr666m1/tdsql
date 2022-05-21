from dataclasses import dataclass

@dataclass(eq=True)
class TdsqlTestConfig():
    database: str # NOTE cannnot use Literal here
    max_bytes_billed: int = 1024 ** 3 # 1GiB
    max_results: int = 1000
    auto_sort: bool = True
    acceptable_error: float = 0.05

