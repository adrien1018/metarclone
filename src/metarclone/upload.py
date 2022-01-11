import logging
import os
import posixpath
import stat
from typing import Dict, Tuple, Optional, List, Set

from disjoint_set import DisjointSet

from .config import UploadConfig
from .checksum import init_file_checksum, one_file_checksum, checksum_walk, ChecksumWalkResult
from .utils import wrap_oserror, decode_child, encode_child
from .rclone import rclone_upload, rclone_delete


class SyncState:
    def __init__(self):
        self.hard_link_map: Dict[Tuple[int, int], bytes] = {}
        self.hard_link_list: List[Tuple[bytes, bytes]] = []


class SyncWalkResult:
    def __init__(self):
        # The values of an empty directory
        self.total_size = 0
        self.total_files = 1
        self.total_transfer_size = 0
        self.total_transfer_files = 1
        self.real_transfer_size = 0
        self.real_transfer_files = 0
        self.force_retain = False
        self.hard_link_map: Optional[Dict[Tuple[int, int], bytes]] = {}
        # files_to_tar is None -> some child has already tar'd
        # not include the current directory
        self.files_to_tar: Optional[Set[bytes]] = set()
        self.files_to_delete: List[Tuple[str, bool]] = []
        self.retained_directories: Optional[List[bytes]] = None
        self.metadata: Optional[dict] = None
        self.first_checksum: Optional[bytes] = None
        self.second_checksum: Optional[bytes] = None
        self.error_count: int = 0

    def set_force_retain(self, path: bytes):
        if not self.force_retain:
            self.force_retain = True
            self.files_to_tar = None
            self.metadata = {'files': {}, 'children': {}}
            self.retained_directories = [path]
            self.hard_link_map = None


def upload_walk(path: bytes, remote_path: str, st: os.stat_result, metadata: Optional[dict], conf: UploadConfig,
                state: SyncState, is_root: bool) -> Optional[SyncWalkResult]:
    """
    Metadata format:
    {
      "files": { // each represents a remote file
        "xxx.tar.gz": { // filename, contains only [A-Za-z0-9_.]
          "list": [b32encode of first-layer dir/files],
          "file_size_checksum": "...", <if conf.use_file_checksum>
          "file_checksum": "...", <if conf.use_file_checksum>
          "mtime_checksum": "...", <if not conf.use_file_checksum>
        }
      },
      "children": {
        <b32encode of name>: (another metadata), ...
      },
    }

    Note that we upload each inode once in every tar file it is in, so hard links across different tar files will be
      uploaded multiple times.
    It is possible to only upload once for each inode globally, but it requires extra metadata information and passes to
      check and correctly update changes of the hardlink structure, so currently we do not implement it.
    """
    # noinspection PyUnusedLocal
    dir_list: Optional[List[bytes]] = None
    with wrap_oserror(path):
        dir_list = os.listdir(path)
    if dir_list is None:
        return None

    res = SyncWalkResult()

    def remote_del(name: str, is_dir: bool):
        del_path = os.path.join(remote_path, name)
        if conf.delete_after_upload:
            res.files_to_delete.append((del_path, is_dir))
        else:
            # logging.debug(f'Delete {os.path.join(remote_path, name)}')
            if not rclone_delete(del_path, is_dir, conf):
                logging.warning(f'Failed to delete remote: {del_path}')
                res.error_count += 1

    def remote_upload(files: List[bytes], dest: str) -> bool:
        # logging.debug(f'Upload {path}/{files} -> {dest}')
        nbytes = rclone_upload(path, files, dest, conf)
        if nbytes < 0:
            return False
        res.real_transfer_size += nbytes
        res.real_transfer_files += 1
        return True

    def update_hardlink_map(key: Tuple[int, int], fpath: bytes, new_hardlinks: Dict[Tuple[int, int], bytes]):
        if key in state.hard_link_map:
            state.hard_link_list.append((state.hard_link_map[key], fpath))
        else:
            new_hardlinks[key] = fpath

    stat_map: Dict[bytes, os.stat_result] = {}
    for child in dir_list:
        child_path = os.path.join(path, child)
        with wrap_oserror(child_path):
            stat_map[child] = os.stat(child_path, follow_symlinks=False)
    # all stat'able children
    child_list: Set[bytes] = set(stat_map)

    # check metadata
    remote_names: Set[str] = set()
    if metadata:
        for filename, remote_file in metadata['files'].items():
            filename: str
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
                remote_names.add(filename)
                res.set_force_retain(path)
                res.metadata['files'][filename] = remote_file
                res.total_size += walk_res.total_size
                res.total_files += walk_res.total_files
                # Update for complete hardlink metadata
                hardlinks: Dict[Tuple[int, int], bytes] = {}
                for item_key, item_path in walk_res.hard_link_map.items():
                    update_hardlink_map(item_key, item_path, hardlinks)
                state.hard_link_map.update(hardlinks)
            else:
                # Note that because we base32-encoded directory names, they will never clash with tar names because of
                #   the .tar extension
                remote_del(filename, False)
                if conf.delete_after_upload:
                    remote_names.add(filename)

        # Delete remote directories
        for i in metadata['children']:
            if decode_child(i) not in child_list or not stat.S_ISDIR(stat_map[decode_child(i)].st_mode):
                remote_del(i, True)

    # all children that needs to be uploaded
    dir_result_map: Dict[bytes, SyncWalkResult] = {}
    size_map: Dict[bytes, int] = {}
    for child in child_list:
        child_st = stat_map[child]
        if stat.S_ISDIR(child_st.st_mode):
            meta_child = metadata and metadata['children'].get(encode_child(child))
            child_res = upload_walk(os.path.join(path, child), posixpath.join(remote_path, encode_child(child)),
                                    child_st, meta_child, conf, state, False)
            if child_res is not None:
                dir_result_map[child] = child_res
                if child_res.force_retain:
                    res.set_force_retain(path)
                    res.retained_directories += child_res.retained_directories
                    res.metadata['children'][encode_child(child)] = child_res.metadata
                    res.real_transfer_size += child_res.real_transfer_size
                    res.real_transfer_files += child_res.real_transfer_files
                else:
                    size_map[child] = child_res.total_size + child_res.total_files * conf.file_base_bytes
                res.total_size += child_res.total_size
                res.total_files += child_res.total_files
                res.total_transfer_size += child_res.total_transfer_size
                res.total_transfer_files += child_res.total_transfer_files
                res.files_to_delete += child_res.files_to_delete
                res.error_count += child_res.error_count
        else:
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
            current_group.sort()
            # Find an available filename
            while True:
                upload_name = f'{conf.reserved_prefix}{file_idx:05d}.tar{conf.compression_suffix}'
                if upload_name not in remote_names:
                    break
                file_idx += 1
            # Update metadata
            meta_item = {'list': [encode_child(f) for f in current_group]}
            if conf.use_file_checksum:
                meta_item['file_size_checksum'] = multifile_checksum(current_group, False).hexdigest()
                meta_item['file_checksum'] = multifile_checksum(current_group, True).hexdigest()
            else:
                meta_item['mtime_checksum'] = multifile_checksum(current_group, False).hexdigest()
            res.metadata['files'][upload_name] = meta_item

            # Get list of all files need to be tar'd and update hardlink map
            upload_list: List[bytes] = []
            hardlinks: Dict[Tuple[int, int], bytes] = {}
            for f in current_group:
                f_st = stat_map[f]
                if stat.S_ISDIR(f_st.st_mode):
                    f_res = dir_result_map[f]
                    upload_list += list(f_res.files_to_tar)
                    for item_key, item_path in f_res.hard_link_map.items():
                        update_hardlink_map(item_key, item_path, hardlinks)
                else:
                    f_path = os.path.join(path, f)
                    upload_list.append(f_path)
                    if f_st.st_nlink > 1:
                        update_hardlink_map((f_st.st_dev, f_st.st_ino), f_path, hardlinks)
            state.hard_link_map.update(hardlinks)

            # Make paths relative to current path
            for idx in range(len(upload_list)):
                assert path == upload_list[idx][:len(path)], f'{path} {upload_list[idx]}'
                upload_list[idx] = os.path.relpath(upload_list[idx], path)
            upload_list.sort()

            remote_name = posixpath.join(remote_path, upload_name)
            logging.info(f'Upload {current_group} in {repr(path)[1:]} to {remote_name}')
            # If the file state changed from A to B between the checksum computation and the time of upload,
            #   and later rollbacked to A, the remote will stay at state B and remain undetected by further syncs.
            # Since we detect mtime changes, this edge case is less likely to happen without intentional action.
            if not remote_upload(upload_list, remote_name):
                log_name = repr(os.path.join(path, upload_name.encode()))[1:]
                logging.warning(f'Failed to upload: {log_name}')
                # delete from metadata so later syncs can detect it
                del res.metadata['files'][upload_name]
                res.error_count += 1

            current_group = []
            group_size = 0
            file_idx += 1

    return res


def upload(path: bytes, remote_path: str, metadata: Optional[dict], conf: UploadConfig) -> Optional[SyncWalkResult]:
    st = os.stat(path)
    state = SyncState()
    res = upload_walk(path, remote_path, st, metadata and metadata['meta'], conf, state, True)
    if not res:
        return None
    for file, is_dir in res.files_to_delete:
        if not rclone_delete(file, is_dir, conf):
            res.error_count += 1
    root_name = f'{conf.reserved_prefix}ROOT.tar{conf.compression_suffix}'
    # always update retained directories
    nbytes = rclone_upload(path, sorted(res.retained_directories), posixpath.join(remote_path, root_name), conf)
    if nbytes >= 0:
        res.real_transfer_size += nbytes
        res.real_transfer_files += 1
    hard_link_djs = DisjointSet()
    for x, y in state.hard_link_list:
        hard_link_djs.union(x, y)
    res.metadata = {'version': conf.metadata_version,
                    'meta': res.metadata,
                    'root_name': root_name,
                    'checksum': {
                        'use_file_checksum': conf.use_file_checksum,
                        'use_directory_mtime': conf.use_directory_mtime,
                        'hash_function': conf.hash_function().name,
                    },
                    'hard_links': [{'group': [encode_child(os.path.relpath(j, path)) for j in i]}
                                   for i in hard_link_djs.itersets()]}
    return res
