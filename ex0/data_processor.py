import abc
import typing

class DataProcessor(abc.ABC):
    def __init__(self) -> None:
        self.queue: list[tuple[int, str]] = []
        self.rank: int = 0

    @abc.abstractmethod
    def validate(self, data: typing.Any) -> bool:
        pass

    @abc.abstractmethod
    def ingest(self, data: typing.Any) -> None:
        pass

    def output(self) -> tuple[int, str]:
        if not self.queue:
            raise IndexError("No data available to extract.")

        return self.queue.pop(0)

class NumericProcessor(DataProcessor):
    def validate(self, data: typing.Any) -> bool:
        if type(data) in (int, float):
            return True
        if isinstance(data, list):
            for x in data:
                if type(x) not in (int, float):
                    return False
            return True
        return False

    def ingest(self, data: typing.Any) -> None:
        if self.validate(data) == False:
            raise ValueError("Non valid data for NumericProcessor")
        if isinstance(data, list):
            for number in data:
                self.rank += 1
                self.queue.append((self.rank, str(number)))
        else:
            self.rank += 1
            self.queue.append((self.rank, str(data)))


class TextProcessor(DataProcessor):
    def validate(self, data: typing.Any) -> bool:
        if type(data) is str:
            return True
        if isinstance(data, list):
            for x in data:
                if type(x) is not str:
                    return False
            return True
        return False

    def ingest(self, data: typing.Any) -> None:
        if self.validate(data) == False:
            raise ValueError("Non valid data for TextProcessor")
        if isinstance(data, list):
            for letter in data:
                self.rank += 1
                self.queue.append((self.rank, letter))
        else:
            self.rank += 1
            self.queue.append((self.rank, data))


class LogProcessor(DataProcessor):
    def validate(self, data: typing.Any) -> bool:
        if type(data) is dict:
            return True
        if isinstance(data, list):
            for x in data:
                if type(x) is not dict:
                    return False
            return True
        return False

    def ingest(self, data: typing.Any) -> None:
        if self.validate(data) == False:
            raise ValueError("Non valid data for LogProcessor")
        if isinstance(data, dict):
            items_to_process: list[dict[str, str]] = [data]
        else:
            items_to_process = data

        for log in items_to_process:
            self.rank += 1
            formatted_log: str = f"{log.get('log_level', '')}: {log.get('log_message', '')}"
            self.queue.append((self.rank, formatted_log))

if __name__ == "__main__":
    print("=== Code Nexus - Data Processor ===\n")
    print("Testing Numeric Processor...")
    num_p: NumericProcessor = NumericProcessor()
    
    valid_int: int = 42
    valid_str: str = "Hello"
    print(f" Trying to validate input '{valid_int}': {num_p.validate(valid_int)}")
    print(f" Trying to validate input '{valid_str}': {num_p.validate(valid_str)}")

    print(" Test invalid ingestion of string 'foo' without prior validation:")
    try:
        num_p.ingest("foo")
    except ValueError:
        print(" Got exception: Improper numeric data")

    num_data: list[int] = [1, 2, 3, 4, 5]
    print(f" Processing data: {num_data}")
    num_p.ingest(num_data)
    print(" Extracting 3 values...")
    for i in range(3):
        out_num: tuple[int, str] = num_p.output()
        print(f" Numeric value {i}: {out_num[1]}")

    print("\nTesting Text Processor...")
    text_p: TextProcessor = TextProcessor()
    print(f" Trying to validate input '{valid_int}': {text_p.validate(valid_int)}")
    text_data: list[str] = ["Hello", "Nexus", "World"]
    print(f" Processing data: {text_data}")
    text_p.ingest(text_data)
    print(" Extracting 1 value...")
    out_text: tuple[int, str] = text_p.output()
    print(f" Text value 0: {out_text[1]}")

    print("\nTesting Log Processor...")
    log_p: LogProcessor = LogProcessor()
    print(f" Trying to validate input '{valid_str}': {log_p.validate(valid_str)}")
    log_data: list[dict[str, str]] = [
        {"log_level": "NOTICE", "log_message": "Connection to server"},
        {"log_level": "ERROR", "log_message": "Unauthorized access!!"},
    ]
    print(f" Processing data: {log_data}")
    log_p.ingest(log_data)
    print(" Extracting 2 values...")
    out_log1: tuple[int, str] = log_p.output()
    out_log2: tuple[int, str] = log_p.output()
    print(f" Log entry 0: {out_log1[1]}")
    print(f" Log entry 1: {out_log2[1]}")
