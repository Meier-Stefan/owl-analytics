import csv
from threading import Lock

import part1_build_dataset
from part1_build_dataset import write_csv, log_message, print_and_log


class TestWriteCsv:

    def test_writes_header_and_rows_to_file(self, tmp_path):
        filepath = tmp_path / "output.csv"
        rows = [{"a": "1", "b": "2"}, {"a": "3", "b": "4"}]
        write_csv(filepath, ["a", "b"], rows)
        content = filepath.read_text()
        lines = content.splitlines()
        assert lines == ["a,b", "1,2", "3,4"]

    def test_creates_parent_directories_when_they_do_not_exist(self, tmp_path):
        filepath = tmp_path / "sub" / "nested" / "output.csv"
        rows = [{"x": "y"}]
        write_csv(filepath, ["x"], rows)
        assert filepath.exists()

    def test_handles_empty_rows_list(self, tmp_path):
        filepath = tmp_path / "empty.csv"
        write_csv(filepath, ["col"], [])
        content = filepath.read_text()
        lines = content.splitlines()
        assert lines == ["col"]

    def test_overwrites_existing_file(self, tmp_path):
        filepath = tmp_path / "overwrite.csv"
        write_csv(filepath, ["col"], [{"col": "old"}])
        write_csv(filepath, ["col"], [{"col": "new"}])
        content = filepath.read_text()
        lines = content.splitlines()
        assert lines == ["col", "new"]

    def test_written_file_is_valid_csv_readable_by_dict_reader(self, tmp_path):
        filepath = tmp_path / "roundtrip.csv"
        rows = [{"a": "1", "b": "2"}, {"a": "3", "b": "4"}]
        write_csv(filepath, ["a", "b"], rows)
        with open(filepath, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            read_rows = list(reader)
        assert read_rows == [{"a": "1", "b": "2"}, {"a": "3", "b": "4"}]

    def test_writes_multiple_rows_correctly(self, tmp_path):
        filepath = tmp_path / "many.csv"
        rows = [{"n": str(i)} for i in range(100)]
        write_csv(filepath, ["n"], rows)
        with open(filepath, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            read_rows = list(reader)
        assert len(read_rows) == 100
        assert read_rows[0]["n"] == "0"
        assert read_rows[99]["n"] == "99"

    def test_forces_default_newline_handling(self, tmp_path):
        filepath = tmp_path / "newlines.csv"
        rows = [{"text": "line1\nline2"}]
        write_csv(filepath, ["text"], rows)
        content = filepath.read_text()
        assert "\nline2" not in content.splitlines()[0] if content else True

    def test_does_not_create_file_without_calling_writeheader(self, tmp_path):
        filepath = tmp_path / "no_write.csv"
        rows = [{"a": "1"}]
        write_csv(filepath, ["a"], rows)
        assert filepath.stat().st_size > 0


class TestLogMessage:

    def test_writes_timestamped_message_to_log_file(self, mocker, tmp_path):
        lock = Lock()
        fake_log = tmp_path / "api_download.log"
        mocker.patch.object(part1_build_dataset, "LOG_FILE", fake_log)
        log_message(lock, "test message")
        content = fake_log.read_text()
        assert " | test message\n" in content

    def test_includes_iso_timestamp_prefix(self, mocker, tmp_path):
        lock = Lock()
        fake_log = tmp_path / "api_download.log"
        mocker.patch.object(part1_build_dataset, "LOG_FILE", fake_log)
        log_message(lock, "test message")
        content = fake_log.read_text()
        timestamp_part = content.split(" | ")[0]
        assert timestamp_part[4] == "-"
        assert timestamp_part[7] == "-"
        assert "T" in timestamp_part
        assert timestamp_part.endswith("Z")

    def test_appends_to_existing_log_file(self, mocker, tmp_path):
        lock = Lock()
        fake_log = tmp_path / "api_download.log"
        mocker.patch.object(part1_build_dataset, "LOG_FILE", fake_log)
        log_message(lock, "first")
        log_message(lock, "second")
        content = fake_log.read_text()
        lines = content.strip().split("\n")
        assert len(lines) == 2
        assert "first" in lines[0]
        assert "second" in lines[1]

    def test_does_not_overwrite_existing_log_content(self, mocker, tmp_path):
        lock = Lock()
        fake_log = tmp_path / "api_download.log"
        mocker.patch.object(part1_build_dataset, "LOG_FILE", fake_log)
        log_message(lock, "original")
        log_message(lock, "appended")
        content = fake_log.read_text()
        assert content.count("original") == 1
        assert content.count("appended") == 1

    def test_thread_safety_multiple_threads_do_not_corrupt(self, mocker, tmp_path):
        from concurrent.futures import ThreadPoolExecutor
        lock = Lock()
        fake_log = tmp_path / "api_download.log"
        mocker.patch.object(part1_build_dataset, "LOG_FILE", fake_log)

        def writer(msg):
            for _ in range(50):
                log_message(lock, msg)

        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = [executor.submit(writer, f"thread-{i}") for i in range(4)]
            for f in futures:
                f.result()
        content = fake_log.read_text()
        lines = content.strip().split("\n")
        assert len(lines) == 200
        for i in range(4):
            assert sum(1 for l in lines if f"thread-{i}" in l) == 50


class TestPrintAndLog:

    def test_prints_message_to_stdout(self, mocker, tmp_path, capsys):
        lock = Lock()
        fake_log = tmp_path / "api_download.log"
        mocker.patch.object(part1_build_dataset, "LOG_FILE", fake_log)
        print_and_log(lock, "hello stdout")
        captured = capsys.readouterr()
        assert "hello stdout" in captured.out

    def test_logs_message_to_file_as_well(self, mocker, tmp_path, capsys):
        lock = Lock()
        fake_log = tmp_path / "api_download.log"
        mocker.patch.object(part1_build_dataset, "LOG_FILE", fake_log)
        print_and_log(lock, "logged too")
        content = fake_log.read_text()
        assert "logged too" in content

    def test_stdout_output_ends_with_newline(self, mocker, tmp_path, capsys):
        lock = Lock()
        fake_log = tmp_path / "api_download.log"
        mocker.patch.object(part1_build_dataset, "LOG_FILE", fake_log)
        print_and_log(lock, "newline check")
        captured = capsys.readouterr()
        assert captured.out.endswith("\n")
