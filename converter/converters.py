from io import TextIOWrapper
from abc import abstractmethod
from pathlib import Path
from typing import Tuple, override

import pandas as pd


class InvalidIlluminaReportError(Exception):
    pass


class Converter:
    @abstractmethod
    def convert(self, data: any) -> any:
        pass


class IlluminaReportConverter(Converter):
    data_headers = []
    report_headers = {}

    def __init__(self):
        super().__init__()

    def read_illumina_report(self, filepath: Path) -> pd.DataFrame:
        with open(filepath, 'r') as f:
            nextline = 0
            while True:
                line = f.readline()
                nextline += 1
                if line.startswith('[Header]'):
                    break
            self.report_headers, nextline = self._read_report_headers(f, nextline)
            # Parse Data headers
            if line == '':
                raise InvalidIlluminaReportError('No data header found in the report')
            self.data_headers, nextline = self._read_data_headers(f, nextline)

        rt = pd.read_csv(filepath, sep='\t', skiprows=nextline, names=self.data_headers)
        return rt

    def _read_data_headers(self, f: TextIOWrapper, nextline: int) -> Tuple[Tuple[str], int]:
        line = f.readline()
        nextline += 1
        if line == '':
            raise InvalidIlluminaReportError('No data header found in the report')
        headers = line.strip().split('\t')

        return tuple(headers), nextline

    def _read_report_headers(self, f: TextIOWrapper, nextline: int) -> Tuple[dict[str], int]:
        headers = {}
        while True:
            line = f.readline()
            nextline += 1

            if line == '' or line.startswith('['):
                break
            vals = line.strip().split('\t')
            headers[vals[0]] = vals[1]
        return headers, nextline
    @override
    def convert(self, data: any) -> any:
        pass
