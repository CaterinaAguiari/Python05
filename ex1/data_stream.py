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
        if isinstance(data, list):
            for log in data:
                self.rank += 1
                self.queue.append((self.rank, str(log)))
        else:
            self.rank += 1
            self.queue.append((self.rank, str(data)))


class DataStream:
    def __init__(self) -> None:
        self.processors: list[DataProcessor] = []

    def register_processor(self, proc: DataProcessor) -> None:
        self.processors.append(proc)

    def process_stream(self, stream: list[typing.Any]) -> None:
        for item in stream:
            handled: bool = False
            for proc in self.processors:
                if proc.validate(item) == True:
                    proc.ingest(item)
                    handled = True
                    break
            if handled == False:
                print(f"DataStream error - Can't process element in stream: {item}")

    def print_processors_stats(self) -> None:
        print("== DataStream statistics ==")
        if self.processors == False:
            print("No processor found, no data")
            return
            
        for proc in self.processors:
            name: str = proc.__class__.__name__
            
            if name == "NumericProcessor":
                label: str = "Numeric Processor"
            elif name == "TextProcessor":
                label = "Text Processor"
            elif name == "LogProcessor":
                label = "Log Processor"
            else:
                label = name
                
            total_processed: int = proc.rank
            items_in_queue: int = len(proc.queue)
            
            print(f"{label} total {total_processed} items processed, remaining {items_in_queue} on processor")


if __name__ == "__main__":
    print("=== Code Nexus - Data Stream ===\n")
    print("Initialize Data Stream...")
    ds: DataStream = DataStream()
    ds.print_processors_stats()

    num_p: NumericProcessor = NumericProcessor()
    text_p: TextProcessor = TextProcessor()
    log_p: LogProcessor = LogProcessor()

    print("\nRegistering Numeric Processor\n")
    ds.register_processor(num_p)

    batch: list[typing.Any] = [
        "Hello world",
        [3.14, -1, 2.71],
        [{'log_level': 'WARNING', 'log_message': 'Telnet access! Use ssh instead'},
         {'log_level': 'INFO', 'log_message': 'User wil is connected'}],
        42,
        ['Hi', 'five']
    ]

    print(f"Send first batch of data on stream: {batch}")
    ds.process_stream(batch)
    ds.print_processors_stats()

    print(f"\nRegistering other data processors")
    ds.register_processor(text_p)
    ds.register_processor(log_p)

    print("Send the same batch again")
    ds.process_stream(batch)
    ds.print_processors_stats()

    print("\nConsume some elements from the data processors: Numeric 3, Text 2, Log 1")
    for _ in range(3):
        num_p.output()
    for _ in range(2):
        text_p.output()
    for _ in range(1):
        log_p.output()

    ds.print_processors_stats()