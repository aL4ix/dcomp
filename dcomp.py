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
            print(f'GENERATING checksum for {self.get_complete_file_path()}')
            if self.mode == FileMode.DIR:
                print('WARNING: ISDIR!')
            else:
                self.checksum = calculate_checksum(self.get_complete_file_path())
        return self.checksum

    def get_complete_file_path(self):
        return self.root + self.path + self.leaf


def walk(root: str, results: list[Stats], extractor: Callable[[pathlib.Path, str], tuple[str, str]], ori_root=None):
    if ori_root is None:
        ori_root = root

    for file in pathlib.Path(root).iterdir():
        leaf, path = extractor(file, ori_root)
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


def filter_matches(list_to_filter: list[Stats], fil_func: Callable) -> tuple[int, list[Stats], Stats]:
    """
    Take a list of Stats and apply fil_func to filter it.

    :param list_to_filter: the original list to be used to iterate and filter
    :param fil_func: function used to filter
    :return: a tuple of (length, a list of matches, and the first match if any)
    """
    matches = list(filter(fil_func, list_to_filter))
    length = len(matches)
    m = matches[0] if length > 0 else None
    return length, matches, m


def try_to_find_one_match_or_closest(mode: FileMode, closest_match_vs_exact: bool, list_: list[Stats], *args) -> tuple[
    int, list[Stats], Stats]:
    prev_length = len(list_)
    prev_matches = list_
    prev_m = list_[0] if prev_length > 0 else None
    for index, fil_func in enumerate(args):
        length, matches, m = filter_matches(prev_matches, fil_func)
        if length > 0:
            print(f'Try to find one match, iter {index} found {length} matches')
        if ((length == 1 and closest_match_vs_exact)
            or (mode == FileMode.DIR and index >= 1)
            or (length == 0 and index < 2)
            or (length == 0 and not closest_match_vs_exact)):
            return length, matches, m
        elif length == 0:
            break
        else:
            print('next')
            prev_length = length
            prev_matches = matches
            prev_m = m
    return prev_length, prev_matches, prev_m


def get_match(node_a: Stats, list_a: list[Stats], list_b: list[Stats], merged, to_delete, comparator: Callable):
    # Maybe there is more than one match from a to b or the other way
    # Check if this node's name matches only once from both sides, if not get lists from both, a and b, and look for the
    # exact matches and discard them, then get the first in the list, if any
    mode = node_a.mode
    length_m, matches_m, first_m = try_to_find_one_match_or_closest(mode, True, list_b,
                                                                    lambda b: b.leaf == node_a.leaf,
                                                                    lambda b: b.mode == node_a.mode,
                                                                    lambda b: b.size == node_a.size,
                                                                    lambda b: b.get_checksum() == node_a.get_checksum())
    matches_a = []
    if length_m > 0:
        for index, m in enumerate(matches_m):
            print(f'Find b in a iter={index}')
            length_a, matches_a, new_a = try_to_find_one_match_or_closest(mode, False, list_a,
                                                                          lambda a: a.leaf == m.leaf,
                                                                          lambda a: a.mode == m.mode,
                                                                          lambda a: a.size == m.size,
                                                                          lambda a: a.get_checksum() == m.get_checksum())
            if node_a in matches_a and not(mode == FileMode.FILE and length_a != 1):  # Found the one
                print('found the one')
                list_b.remove(m)
                to_delete.append(m)
                node_a.decision = False
                m.decision = False
                m.keep = False
                return m

    # Quit
    if matches_m:
        print(f'DECISION:')
        for match_a in matches_a:
            print(f' A {match_a.get_complete_file_path()} {match_a.get_checksum()}')
            match_a.decision = True
        for match_m in matches_m:
            print(f' M {match_m.get_complete_file_path()} {match_m.get_checksum()}')
            match_m.decision = True
            match_m.keep = False
        # FIXME keep algo can be improved
        all_aprox = [node_a]
        all_aprox.extend(matches_m)
        all_aprox.sort(key=lambda x: x.size)
        all_aprox[0].keep = True
    return None


def compare_two_lists(list_a: list[Stats], list_b: list[Stats], comparator: Callable[[Stats, Stats], int]) \
        -> tuple[list[Stats], list[Stats]]:
    merged = []
    to_delete = []
    for node_a in list_a:
        print(node_a.get_complete_file_path())
        get_match(node_a, list_a, list_b, merged, to_delete, comparator)
        merged.append(node_a)

    merged.extend(list_b)
    print('TO_DELETE')
    print('\n'.join(repr((d.get_complete_file_path(), d.get_checksum())) for d in to_delete))
    return merged, to_delete


def compare_within_one_list(l: list[Stats], comparator: Callable[[Stats, Stats], int]) -> list[Stats]:
    merged = []
    for node in l:
        for node2 in l:
            if node is not node2:
                if node.leaf == node2.leaf:
                    merged.append(node)
                    # FIXME No algo yet
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
    print(f'WALK: {len(list_a)+len(list_b)}')
    merged, to_delete = compare_two_lists(list_a, list_b, comparator)
    print(len(merged)+len(to_delete))
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
    path_a = '/home/a'
    path_b = '/home/b'
    compare_two_folders(path_a, path_b, extract_only_caring_about_filename, keep_bigger)


if __name__ == '__main__':
    main()
