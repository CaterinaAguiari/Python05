import abc
import typing

class ExportPlugin(typing.Protocol):
    def process_output(self, data: list[tuple[int, str]]) -> None:
        ...


class CSVExportPlugin:
    def process_output(self, data: list[tuple[int, str]]) -> None:
        print("CSV Output:")
        values: list[str] = []
        
        for item_id, item_val in data:
            values.append(item_val)

        print(",".join(values))


class JSONExportPlugin:
    def process_output(self, data: list[tuple[int, str]]) -> None:
        print("JSON Output:")
        json_entries: list[str] = []
        
        for item_id, item_val in data:
            entry: str = f'"item_{item_id}": "{item_val}"'
            json_entries.append(entry)

        full_json: str = "{" + ", ".join(json_entries) + "}"
        print(full_json)


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

    def output(self, nb: int) -> tuple[int, str]:
        extracted: list[tuple[int, str]] = []
        for _ in range(nb):
            if not self.queue:
                break
            extracted.append(self.queue.pop(0))
        return extracted


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
                formatted_log: str = f"{log.get('log_level', '')}: {log.get('log_message', '')}"
                self.queue.append((self.rank, formatted_log))
        else:
            self.rank += 1
            formatted_log: str = f"{log.get('log_level', '')}: {log.get('log_message', '')}"
            self.queue.append((self.rank, formatted_log))


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
        print("\n== DataStream statistics ==")
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

    def output_pipeline(self, nb: int, plugin: ExportPlugin) -> None:
        for proc in self.processors:
            extracted_data: list[tuple[int, str]] = proc.output(nb)
            if extracted_data:
                plugin.process_output(extracted_data)


if __name__ == "__main__":
    print("=== Code Nexus - Data Pipeline ===\n")
    print("Initialize Data Stream...\n")
    ds: DataStream = DataStream()
    ds.print_processors_stats()

    num_p: NumericProcessor = NumericProcessor()
    text_p: TextProcessor = TextProcessor()
    log_p: LogProcessor = LogProcessor()

    print("\nRegistering Numeric Processors\n")
    ds.register_processor(num_p)
    ds.register_processor(text_p)
    ds.register_processor(log_p)

    batch_1: list[typing.Any] = [
        "Hello world",
        [3.14, -1, 2.71],
        [
            {'log_level': 'WARNING', 'log_message': 'Telnet access! Use ssh instead'},
            {'log_level': 'INFO', 'log_message': 'User wil is connected'}
        ],
        42,
        ['Hi', 'five']
    ]

    print(f"Send first batch of data on stream: {batch_1}")
    ds.process_stream(batch_1)
    ds.print_processors_stats()

    print("\nSend 3 processed data from each processor to a CSV plugin:")
    csv_plugin: CSVExportPlugin = CSVExportPlugin()
    ds.output_pipeline(3, csv_plugin)

    ds.print_processors_stats()

    batch_2: list[typing.Any] = [
        21,
        ['I love AI', 'LLMs are wonderful', 'Stay healthy'],
        [
            {'log_level': 'ERROR', 'log_message': '500 server crash'},
            {'log_level': 'NOTICE', 'log_message': 'Certificate expires in 10 days'}
        ],
        [32, 42, 64, 84, 128, 168],
        'World hello'
    ]

    print(f"\nSend another batch of data: {batch_2}")
    ds.process_stream(batch_2)
    ds.print_processors_stats()

    print("\nSend 5 processed data from each processor to a JSON plugin:")
    json_plugin: JSONExportPlugin = JSONExportPlugin()
    ds.output_pipeline(5, json_plugin)

    ds.print_processors_stats()
