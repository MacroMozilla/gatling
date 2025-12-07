import datetime
from typing import Optional, IO, Any, BinaryIO, Literal
import traceback
from gatling.storage.g_table.base_table import BaseTableAO
from gatling.storage.g_table.help_tools.file_tools import readline_forward, append_line, extend_lines, readline_backward, goto_tail, get_pos, set_pos
from gatling.utility.error_tools import FileAlreadyOpenedForWriteError, FileAlreadyOpenedError, FileAlreadyOpenedForReadError, FileNotOpenError
from gatling.utility.io_fctns import remove_file
from dataclasses import dataclass

keytype_to_sent = {
    str: str,
    int: str,
    float: str,
    bool: lambda x: str(int(x)),
    datetime.date: lambda x: x.strftime('%Y-%m-%d'),
    datetime.time: lambda x: x.strftime('%H:%M:%S'),
    datetime.datetime: lambda x: x.strftime('%Y-%m-%d %H:%M:%S'),
}

keytype_fm_sent = {
    str: lambda x: x,
    int: int,
    float: float,
    bool: lambda x: bool(int(x)),
    datetime.date: lambda x: datetime.datetime.strptime(x, '%Y-%m-%d').date(),
    datetime.time: lambda x: datetime.datetime.strptime(x, '%H:%M:%S').time(),
    datetime.datetime: lambda x: datetime.datetime.strptime(x, '%Y-%m-%d %H:%M:%S'),
}

sent_2_keytype = {
    'str': str,
    'int': int,
    'float': float,
    'bool': bool,
    'date': datetime.date,
    'time': datetime.time,
    'datetime': datetime.datetime,
}

KEY_IDX = "*"


@dataclass
class FileTableAOState:
    file: Optional[BinaryIO] = None
    key2type: Optional[dict[str, Any]] = None
    next_idx: Optional[int] = None


def head2sent(key2type):
    return '\t'.join([f"{keyname}.{keytype.__name__}" for keyname, keytype in key2type.items()])


def sent2head(sent):
    try:
        return {k: sent_2_keytype[v] for k, v in (item.rsplit('.', 1) for item in sent.split('\t'))}
    except ValueError as e:
        print(f"{e} error parsing (rsplit failed): {sent!r}")

        print(traceback.format_exc())
        raise
    except KeyError as e:
        print(f"{e} error parsing (unknown keytype {e}): {sent!r}")
        print(traceback.format_exc())
        raise


def row2sent(row, key2type):
    return '\t'.join(keytype_to_sent[ktype](row[kname]) for kname, ktype in key2type.items())


def sent2row(sent, key2type):
    values = sent.split('\t')
    return {kname: keytype_fm_sent[ktype](val) for (kname, ktype), val in zip(key2type.items(), values)}


def is_write_mode(file: IO) -> bool:
    """Check if the file is opened with write permission."""
    return bool(set(file.mode) & {"w", "a", "+", "x"})


class FileTableAO(BaseTableAO):

    def __init__(self, fpath):
        super().__init__()
        self.fpath = fpath
        self.state = FileTableAOState()

    def get_key2type(self):
        target_file = self.state.file
        if target_file is not None and is_write_mode(target_file):
            raise FileAlreadyOpenedForWriteError(f'{self.fpath} is already opened with write permission.')

        with open(self.fpath, 'rb') as f:
            key2type = sent2head(readline_forward(f).decode())
        return key2type

    def get_first_row(self, key2type=None):
        target_file = self.state.file
        if target_file is not None and is_write_mode(target_file):
            raise FileAlreadyOpenedForWriteError(f'{self.fpath} is already opened with write permission.')
        if key2type is None:
            key2type = self.get_key2type()
        with open(self.fpath, 'rb') as f:
            _ = readline_forward(f) # skip head

            first_sent = readline_forward(f).decode()
            if first_sent == '':
                return {}
            else:
                return sent2row(first_sent, key2type)

    def get_last_row(self, key2type=None):
        target_file = self.state.file
        if target_file is not None and is_write_mode(target_file):
            raise FileAlreadyOpenedForWriteError(f'{self.fpath} is already opened with write permission.')
        if key2type is None:
            key2type = self.get_key2type()
        with open(self.fpath, 'rb') as f:
            goto_tail(f)
            last_sent = readline_backward(f).decode()
            if last_sent[0] == KEY_IDX:
                return {}
            else:
                return sent2row(last_sent, key2type)

    def clear(self):
        target_file = self.state.file
        if target_file is not None:
            raise FileAlreadyOpenedError(f'{self.fpath} is already opened with read or write permission.')
        remove_file(self.fpath)

    def initialize(self, key2type):
        target_file = self.state.file
        if target_file is not None:
            raise FileAlreadyOpenedError(f'{self.fpath} is already opened with read or write permission.')
        key2type = {KEY_IDX: int, **key2type}
        with open(self.fpath, 'wb') as f:
            append_line(f, head2sent(key2type).encode())

    def _build_state(self, ori_state: Optional[FileTableAOState] = None, open_mode: Literal['rb', 'rb+', 'ab'] = 'rb+') -> FileTableAOState:
        if ori_state is not None:
            target_file = ori_state.file
            if target_file is None:
                pass
            else:
                if is_write_mode(target_file):
                    raise FileAlreadyOpenedForWriteError(f'{self.fpath} is already opened with write permission.')
                elif open_mode != {'rb'}:
                    raise FileAlreadyOpenedForReadError(f'{self.fpath} is already opened with read permission.')

        fts = FileTableAOState() if ori_state is None else ori_state
        fts.key2type = self.get_key2type()
        last_row = self.get_last_row(fts.key2type)
        fts.next_idx = last_row[KEY_IDX] + 1 if last_row else 0
        fts.file = open(self.fpath, open_mode)
        return fts

    def __enter__(self):
        self.state = self._build_state(self.state)
        return self

    def _clean_state(self, ori_state: Optional[FileTableAOState]):
        target_file = ori_state.file
        if target_file is None:
            raise FileNotOpenError(f'{self.fpath} is not opened.')
        ori_state.file.close()
        ori_state.file = None
        ori_state.key2type = None
        ori_state.next_idx = None

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._clean_state(self.state)

    # ============= The functions above should not be called within a context manager =============
    def append(self, row):
        if self.state.file is None:
            temp_state = self._build_state(open_mode='ab')
            try:
                append_line(temp_state.file, row2sent({KEY_IDX: temp_state.next_idx, **row}, temp_state.key2type).encode())
            finally:
                self._clean_state(temp_state)
        else:
            cur_state = self.state
            cur_pos = get_pos(cur_state.file)
            goto_tail(cur_state.file)
            append_line(cur_state.file, row2sent({KEY_IDX: cur_state.next_idx, **row}, cur_state.key2type).encode())
            cur_state.next_idx += 1
            set_pos(cur_state.file, cur_pos)

    def extend(self, rows):
        if self.state.file is None:
            temp_state = self._build_state(open_mode='ab')
            try:
                start_idx = temp_state.next_idx
                extend_lines(temp_state.file, [
                    row2sent({KEY_IDX: start_idx + i, **row}, temp_state.key2type).encode()
                    for i, row in enumerate(rows)
                ])
            finally:
                self._clean_state(temp_state)
        else:
            cur_state = self.state
            start_idx = cur_state.next_idx
            cur_pos = get_pos(cur_state.file)
            goto_tail(cur_state.file)
            extend_lines(cur_state.file, [
                row2sent({KEY_IDX: start_idx + i, **row}, cur_state.key2type).encode()
                for i, row in enumerate(rows)
            ])
            cur_state.next_idx += len(rows)
            set_pos(cur_state.file, cur_pos)

    def keys(self):
        if self.state.file is None:
            temp_state = self._build_state(open_mode='rb')
            res_key2type = temp_state.key2type.keys()
            self._clean_state(temp_state)
            return res_key2type
        else:
            return self.state.key2type.keys()

    def __len__(self):
        if self.state.file is None:
            temp_state = self._build_state(open_mode='rb')
            res_len = temp_state.next_idx
            return res_len

        else:
            return self.state.next_idx

    # def __getitem__(self, idx):
    #     if self._file is None:
    #         with open(self.fpath, 'rb') as f:
    #             if idx >= 0:
    #                 for _ in range(idx + 2):
    #                     row = self._sent2row(readline_forward(f).decode(), self._key2type)
    #                 return row
    #             else:
    #                 for _ in range(-idx):
    #                     row = self._sent2row(readline_backward(f).decode(), self._key2type)
    #                 return row
    #     else:
    #         if idx >= 0:
    #             for _ in range(idx + 1):
    #                 row = self._sent2row(readline_forward(self._file).decode(), self._key2type)
    #             return row
    #         else:
    #             for _ in range(-idx):
    #                 row = self._sent2row(readline_backward(self._file).decode(), self._key2type)
    #             return row


if __name__ == '__main__':
    pass

    ft = FileTableAO('test.tsv')
    ft.clear()

    from gatling.utility.xprint import printi

    printi(ft.get_key2type())
    printi(ft.get_first_row())
    printi(ft.get_last_row())

    # ft.initialize(key2type={
    #     'name': str,
    #     'age': int,
    #     'score': float,
    #     'active': bool,
    #     'birthday': datetime.date,
    #     'alarm': datetime.time,
    #     'created_at': datetime.datetime,
    # })
    #
    # # printi(ft.get_key2type())
    # # printi(ft.get_first_row())
    # # printi(ft.get_last_row())
    #
    # item = {
    #     'name': 'Harry Mozilla',
    #     'age': 25,
    #     'score': 98.5,
    #     'active': True,
    #     'birthday': datetime.date(1999, 5, 20),
    #     'alarm': datetime.time(8, 30, 0),
    #     'created_at': datetime.datetime(2024, 1, 15, 14, 30, 45),
    # }
    #
    # ft.append(item)
    #
    # printi(ft.get_key2type())
    # printi(ft.get_first_row())
    # printi(ft.get_last_row())
    # printi('#######')
    #
    # item = {
    #     'name': 'Bunny Mozilla',
    #     'age': 32,
    #     'score': 87.3,
    #     'active': False,
    #     'birthday': datetime.date(1992, 11, 8),
    #     'alarm': datetime.time(7, 15, 0),
    #     'created_at': datetime.datetime(2023, 6, 22, 9, 45, 12),
    # }
    # ft.append(item)
    #
    # printi(ft.get_key2type())
    # printi(ft.get_first_row())
    # printi(ft.get_last_row())
