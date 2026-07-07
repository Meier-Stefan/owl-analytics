from io import StringIO

from io_utils import _Tee


class TestTeeWrite:

    def test_writes_text_to_file(self, tmp_path):
        log = tmp_path / "test.log"
        with open(log, "w") as f:
            tee = _Tee(f)
            tee.write("hello\n")
        assert log.read_text() == "hello\n"

    def test_writes_text_to_stdout(self, tmp_path):
        log = tmp_path / "test.log"
        fake_stdout = StringIO()
        import sys
        original = sys.__stdout__
        sys.__stdout__ = fake_stdout
        try:
            with open(log, "w") as f:
                tee = _Tee(f)
                tee.write("world\n")
            assert fake_stdout.getvalue() == "world\n"
        finally:
            sys.__stdout__ = original

    def test_multiple_writes_accumulate_in_file(self, tmp_path):
        log = tmp_path / "test.log"
        with open(log, "w") as f:
            tee = _Tee(f)
            tee.write("a\n")
            tee.write("b\n")
            tee.write("c\n")
        assert log.read_text() == "a\nb\nc\n"

    def test_returns_number_of_bytes_written(self, tmp_path):
        log = tmp_path / "test.log"
        fake_stdout = StringIO()
        import sys
        original = sys.__stdout__
        sys.__stdout__ = fake_stdout
        try:
            with open(log, "w") as f:
                tee = _Tee(f)
                result = tee.write("abcd")
            assert result == 4
        finally:
            sys.__stdout__ = original


class TestTeeFlush:

    def test_flush_calls_flush_on_file(self, tmp_path, mocker):
        log = tmp_path / "test.log"
        with open(log, "w") as f:
            tee = _Tee(f)
            spy = mocker.spy(f, "flush")
            tee.flush()
            spy.assert_called_once()

    def test_flush_calls_flush_on_stdout(self, tmp_path, mocker):
        log = tmp_path / "test.log"
        import sys
        original = sys.__stdout__
        sys.__stdout__ = StringIO()
        try:
            with open(log, "w") as f:
                tee = _Tee(f)
                spy = mocker.spy(sys.__stdout__, "flush")
                tee.flush()
                spy.assert_called_once()
        finally:
            sys.__stdout__ = original
