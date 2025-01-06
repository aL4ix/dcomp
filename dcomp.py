import datetime
import pathlib
import stat
import zlib
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Iterable, Callable

import pandas as pd


class FileMode(Enum):
    FILE = 1
    DIR = 2
    UNK = 3


@dataclass
class Stats:
    root: str
    path: str
    leaf: str
    decision: bool
    keep: bool
    checksum: int
    size: int
    mtime: datetime
    mode: FileMode
    ctime: datetime
    atime: datetime

    def get_checksum(self):
        if self.checksum == 1:
            self.checksum = calculate_checksum(self.get_complete_file_path())
        return self.checksum

    def get_complete_file_path(self):
        return self.root + self.path + self.leaf


def walk(root: str, results: list[Stats], extractor: Callable[[pathlib.Path, str], tuple[str, str]], ori_root=None):
    if ori_root is None:
        ori_root = root

    for file in pathlib.Path(root).iterdir():
        leaf, path = extractor(file, ori_root)
        print(file)
        stats = file.stat()
        stat_mode = stats.st_mode
        if stat.S_ISREG(stat_mode):
            mode = FileMode.FILE
        elif stat.S_ISDIR(stat_mode):
            mode = FileMode.DIR
            walk(str(file), results, extractor, ori_root)
        else:
            mode = FileMode.UNK
        size = stats.st_size
        mtime = datetime.fromtimestamp(stats.st_mtime)
        ctime = datetime.fromtimestamp(stats.st_ctime)
        atime = datetime.fromtimestamp(stats.st_atime)
        res = Stats(ori_root, path, leaf, False, True, 1, size, mtime, mode, ctime, atime)
        results.append(res)


def get_first_match_from_iterator(predicate: Callable[[Stats], bool], it: Iterable):
    return next((n for n in it if predicate(n)), None)


def calculate_checksum(file, chunk_size=65536):
    with open(file, 'rb') as f:
        checksum = 1
        while chunk := f.read(chunk_size):
            checksum = zlib.adler32(chunk, checksum)
    return checksum


def algo(node_a: Stats, m: Stats, list_b: list[Stats], comparator: Callable[[Stats, Stats], int], merged: list[Stats]):
    same = False
    if m.mode == node_a.mode:
        if m.mode == FileMode.DIR:
            same = True

        if m.mode == FileMode.FILE and m.size == node_a.size:
            if m.mtime == node_a.mtime:
                same = True
            else:
                if m.get_checksum() == node_a.get_checksum():
                    same = True
    list_b.remove(m)
    if not same:
        print(f'DECISION: "{m.leaf}"')
        m.decision = True
        node_a.decision = True
        keep_a = comparator(node_a, m)
        if keep_a > 0:
            m.keep = False
        elif keep_a < 0:
            node_a.keep = False
        merged.append(m)
    else:
        node_a.decision = False
        m.decision = False
        m.keep = False


def filter_matches(list_to_filter: list[Stats], fil_func: Callable) -> tuple[int, list[Stats], Stats]:
    matches = list(filter(fil_func, list_to_filter))
    length = len(matches)
    m = None
    if length > 0:
        m = matches[0]
    return length, matches, m


def get_match(node_a: Stats, list_a: list[Stats], list_b: list[Stats]):
    # Maybe there is more than one match from a to b or the other way
    # Check if this node's name matches only once from both sides, if not get lists from both, a and b, and look for the
    # exact matches and discard them, then get the first in the list, if any
    length_m, matches_m, m = filter_matches(list_b, lambda x: x.leaf == node_a.leaf)
    if m:  # At least one m
        length_a, matches_a, a = filter_matches(list_a, lambda x: x.leaf == m.leaf)  # Check matches for a with first m
        if length_a == 1:  # Maybe many m but first m has only one match
            return m

        # Many to many

        # Quit
        for match_m in matches_m:
            match_m.decision = True
        for match_a in matches_a:
            match_a.decision = True
        m = None
        # length, matches, m = filter_matches(lambda x: x.size == node_a.size, node_a, matches_m)
        # if length > 1:
        #     length, matches, m = filter_matches(lambda x: x.get_checksum() == node_a.get_checksum(), node_a, matches)
        #     if length > 1:
        #         m = matches[0]
    return m


def compare_two_lists(list_a: list[Stats], list_b: list[Stats], comparator: Callable[[Stats, Stats], int]) \
        -> list[Stats]:
    merged = []
    for node_a in list_a:
        m = get_match(node_a, list_a, list_b)
        merged.append(node_a)
        if m:
            algo(node_a, m, list_b, comparator, merged)

    merged.extend(list_b)
    return merged


def compare_within_one_list(l: list[Stats], comparator: Callable[[Stats, Stats], int]) -> list[Stats]:
    merged = []
    for node in l:
        for node2 in l:
            if node is not node2:
                if node.leaf == node2.leaf:
                    merged.append(node)
                    algo(node, node2, l, comparator, merged)
    merged.extend(l)
    return merged


def create_report(merged):
    df = pd.DataFrame(merged)
    df.to_csv('comparison.csv', index=False)


def compare_two_folders(a: str, b: str, extractor: Callable[[pathlib.Path, str], tuple[str, str]],
                        comparator: Callable[[Stats, Stats], int]):
    list_a = []
    list_b = []
    walk(a, list_a, extractor)
    walk(b, list_b, extractor)
    merged = compare_two_lists(list_a, list_b, comparator)
    create_report(merged)


def compare_within_folder(path: str):
    l = []
    walk(path, l, extract_only_caring_about_filename)
    merged = compare_within_one_list(l, keep_bigger)
    create_report(merged)


def keep_bigger(a: Stats, b: Stats) -> int:
    return a.size - b.size


def keep_older(a: Stats, b: Stats) -> int:
    return int((a.mtime - b.mtime).total_seconds())


def extract_only_caring_about_filename(file: pathlib.Path, ori_root: str):
    leaf = file.name
    path = str(file).removeprefix(ori_root).removesuffix(leaf)
    return leaf, path


def extract_with_complete_path(file: pathlib.Path, ori_root: str):
    leaf = str(file).removeprefix(ori_root)
    path = ''
    return leaf, path


def main():
    a = '/home/a'
    b = '/home/b'
    compare_two_folders(a, b, extract_only_caring_about_filename, keep_bigger)


if __name__ == '__main__':
    main()
