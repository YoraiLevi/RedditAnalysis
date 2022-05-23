import zstandard as zstd
import io


class Zreader:
    def __init__(self, file):
        """Init method"""
        self.fh = open(file, "rb")
        self.dctx = zstd.ZstdDecompressor(max_window_size=2147483648)
        self.reader = self.dctx.stream_reader(self.fh)
        self.text = io.TextIOWrapper(self.reader, encoding="utf-8")

    def readlines(self):
        """Generator method that creates an iterator for each line of JSON"""
        # stream_reader = self.dctx.stream_reader(fh)
        for line in self.text:
            yield line
