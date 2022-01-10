import logging
import os
import posixpath
import stat
from base64 import b32decode, b32encode
from typing import Dict, Tuple, Optional, List, Set

from .config import SyncConfig
from .checksum import init_file_checksum, one_file_checksum, checksum_walk, ChecksumWalkResult
from .utils import wrap_oserror


class SyncWalkResult:
    def __init__(self):
        # The values of an empty directory
        self.total_size = 0
        self.total_files = 1
        self.total_transfer_size = 0
        self.total_transfer_files = 1
        self.force_retain = False
        self.hard_link_map: Dict[Tuple[int, int], bytes] = {}
        self.hard_link_list: List[Tuple[bytes, bytes]] = []
        # files_to_tar is None -> some child has already tar'd
        # not include the current directory
        self.files_to_tar: Optional[Set[bytes]] = set()
        self.files_to_delete: List[bytes] = []
        self.retained_directories: Optional[List[bytes]] = None
        self.metadata: Optional[dict] = None
        self.first_checksum: Optional[bytes] = None
        self.second_checksum: Optional[bytes] = None

    def set_force_retain(self, path: bytes):
        if not self.force_retain:
            self.force_retain = True
            self.files_to_tar = None
            self.metadata = {'files': [], 'children': {}}
            self.retained_directories = [path]


def upload_walk(path: bytes, remote_path: str, st: os.stat_result, metadata: Optional[dict], conf: SyncConfig,
                is_root: bool) -> Optional[SyncWalkResult]:
    """
    Metadata format:
    {
      "files": [ // each represents a remote file
        {
          "name": "xxx.tar", // always .tar and contains only [A-Za-z0-9_.]
          "list": [b32encode of first-layer dir/files],
          "file_size_checksum": "...", <if conf.use_file_checksum>
          "file_checksum": "...", <if conf.use_file_checksum>
          "mtime_checksum": "...", <if not conf.use_file_checksum>
        }
      ],
      "children": {
        <b32encode of name>: (another metadata), ...
      },
    }
    """
    # noinspection PyUnusedLocal
    scan = None
    with wrap_oserror(conf, path):
        scan = os.scandir(path)
    if scan is None:
        return None

    res = SyncWalkResult()

    def decode_child(name: str):
        pad_length = len(name) % 8
        return b32decode(name + '=' * (pad_length and 8 - pad_length))

    def encode_child(name: bytes):
        return b32encode(name).decode().rstrip('=')

    def remote_del(name: bytes):
        if conf.delete_after_upload:
            res.files_to_delete.append(os.path.join(path, name))
        else:
            print(f'Delete {os.path.join(path, name)}')
            # TODO

    def upload(files: List[bytes], dest: str) -> bool:
        print(f'Upload {path}/{files} -> {dest}')
        # TODO
        return True

    with scan as it:
        # noinspection PyUnresolvedReferences
        local_map: Dict[bytes, os.DirEntry] = {f.name: f for f in it}
        stat_map: Dict[bytes, os.stat_result] = {}
        for child in local_map:
            with wrap_oserror(conf, os.path.join(path, child)):
                stat_map[child] = local_map[child].stat(follow_symlinks=False)
        # all stat'able children
        child_list: Set[bytes] = set(stat_map)

    # check metadata
    remote_names: Set[bytes] = set()
    if metadata and 'files' in metadata:
        for remote_file in metadata['files']:
            file_list: List[bytes] = [decode_child(i) for i in remote_file['list']]
            keep = False
            walk_res = ChecksumWalkResult()
            if all([f in stat_map for f in file_list]):
                # We compute this checksum in a separate run.
                # This run only affects decision of whether to upload a file, so it does not matter if the files had
                #   changed between this run and the actual checksum computation for metadata generation.
                # (Those changes will possibly make some file changed between two runs not uploaded during this sync,
                #  but it is correctable by later syncs.)
                walk_list = [(f, stat_map[f]) for f in file_list]
                if conf.use_file_checksum:
                    keep = (
                        'file_size_checksum' in remote_file and
                        'file_checksum' in remote_file and
                        checksum_walk(walk_list, path, conf, False, walk_res) == remote_file['file_size_checksum'] and
                        checksum_walk(walk_list, path, conf, True) == remote_file['file_checksum']
                    )
                else:
                    keep = ('mtime_checksum' in remote_file and
                            checksum_walk(walk_list, path, conf, False, walk_res) == remote_file['mtime_checksum'])
            if keep:
                child_list.difference_update(file_list)
                remote_names.add(remote_file['name'])
                res.set_force_retain(path)
                res.metadata['files'].append(remote_file)
                res.total_size += walk_res.total_size
                res.total_files += walk_res.total_files
                # We don't care about cross-child hard links here,
                #   since it would be handled already by the previous sync
                res.hard_link_map.update(walk_res.hard_link_map)
            else:
                remote_del(remote_file['name'].encode())

    # all children that needs to be uploaded
    dir_result_map: Dict[bytes, SyncWalkResult] = {}
    size_map: Dict[bytes, int] = {}
    reg_files: List[bytes] = []
    for child in child_list:
        child_st = stat_map[child]
        if stat.S_ISDIR(child_st.st_mode):
            meta_child = metadata and metadata['children'].get(encode_child(child))
            child_res = upload_walk(os.path.join(path, child), posixpath.join(remote_path, encode_child(child)),
                                    child_st, meta_child, conf, False)
            if child_res is not None:
                dir_result_map[child] = child_res
                if child_res.force_retain:
                    res.set_force_retain(path)
                    res.retained_directories += child_res.retained_directories
                    res.metadata['children'][encode_child(child)] = child_res.metadata
                else:
                    size_map[child] = child_res.total_size + child_res.total_files * conf.file_base_bytes
                res.total_size += child_res.total_size
                res.total_files += child_res.total_files
                res.total_transfer_size += child_res.total_transfer_size
                res.total_transfer_files += child_res.total_transfer_files
        else:
            reg_files.append(child)
            size_map[child] = child_st.st_size + conf.file_base_bytes
            res.total_size += child_st.st_size
            res.total_files += 1
            res.total_transfer_size += child_st.st_size
            res.total_transfer_files += 1

    def multifile_checksum(names: List[bytes], second_pass: bool, hash_obj=None):
        """
        names already sorted
        """
        if hash_obj is None:
            hash_obj = conf.hash_function()
        for ch in names:
            ch_st = stat_map[ch]
            if stat.S_ISDIR(ch_st.st_mode):
                ch_res = dir_result_map[ch]
                if second_pass and conf.use_file_checksum:
                    hash_obj.update(ch_res.second_checksum)
                else:
                    hash_obj.update(ch_res.first_checksum)
            else:
                hash_obj.update(one_file_checksum(ch, os.path.join(path, ch), ch_st, conf, second_pass))
        return hash_obj

    # If it is not root, no children is tar'd and the directory size is not larger than the merge threshold,
    #   calculate hashes and recurse back, thus treating the directory as one file
    if not is_root and not res.force_retain and \
            res.total_size + res.total_files * conf.file_base_bytes <= conf.merge_threshold:
        res.files_to_tar.add(path)
        for i in size_map:
            res.files_to_tar.add(os.path.join(path, i))
        for i in dir_result_map.values():
            res.files_to_tar.update(i.files_to_tar)
            res.hard_link_map.update(i.hard_link_map)
        # calculate hashes
        first_hash = init_file_checksum(os.path.basename(path), st, conf)
        if conf.use_file_checksum:
            second_hash = first_hash.copy()
            res.second_checksum = multifile_checksum(sorted(child_list), True, second_hash).digest()
        res.first_checksum = multifile_checksum(sorted(child_list), False, first_hash).digest()
        # update hardlink map
        for child in child_list:
            child_st = stat_map[child]
            if not stat.S_ISDIR(child_st.st_mode):
                if child_st.st_nlink > 1:
                    res.hard_link_map[(child_st.st_dev, child_st.st_ino)] = os.path.join(path, child)
        return res

    res.set_force_retain(path)
    res.retained_directories.append(path)
    for i in dir_result_map.values():
        if i.retained_directories:
            res.retained_directories += i.retained_directories
        res.files_to_delete += i.files_to_delete
    if conf.grouping_order == 'size':
        group_list = sorted(size_map, key=lambda x: (size_map[x], x))
    elif conf.grouping_order == 'mtime':
        group_list = sorted(size_map, key=lambda x: (stat_map[x].st_mtime_ns, x))
    elif conf.grouping_order == 'ctime':
        group_list = sorted(size_map, key=lambda x: (stat_map[x].st_ctime_ns, x))
    else:
        raise NotImplementedError

    group_size = 0
    file_idx = 0
    current_group: List[bytes] = []
    for i, child in enumerate(group_list):
        group_size += size_map[child]
        current_group.append(child)
        if group_size > conf.merge_threshold or i == len(group_list) - 1:
            while True:
                upload_name = f'{conf.reserved_prefix}{file_idx:05d}.tar'
                if upload_name not in remote_names:
                    break
                file_idx += 1
            current_group.sort()
            meta_item = {'name': upload_name, 'list': [encode_child(f) for f in current_group]}
            if conf.use_file_checksum:
                meta_item['file_size_checksum'] = multifile_checksum(current_group, False).hexdigest()
                meta_item['file_checksum'] = multifile_checksum(current_group, True).hexdigest()
            else:
                meta_item['mtime_checksum'] = multifile_checksum(current_group, False).hexdigest()
            res.metadata['files'].append(meta_item)
            upload_list: List[bytes] = []
            for f in current_group:
                if f in dir_result_map:
                    upload_list += list(dir_result_map[f].files_to_tar)
                else:
                    upload_list.append(os.path.join(path, f))
            for idx in range(len(upload_list)):
                assert path == upload_list[idx][:len(path)], f'{path} {upload_list[idx]}'
                upload_list[idx] = os.path.relpath(upload_list[idx], path)
            upload_list.sort()
            # TODO: update hard_link_list, hard_link_map
            # If the file state changed from A to B between the checksum computation and the time of upload,
            #   and later rollbacked to A, the remote will stay at state B and remain undetected by further syncs.
            # This is also the reason that we always include mtime in checksum, so that this edge case is less likely to
            #   happen without intentional action.
            if not upload(upload_list, posixpath.join(remote_path, upload_name)):
                log_name = repr(os.path.join(path, upload_name.encode()))[1:]
                msg = f'{log_name} failed to upload'
                if conf.error_abort:
                    raise RuntimeError(msg)
                else:
                    logging.warning(msg)
            current_group = []
            group_size = 0
            file_idx += 1
    return res
