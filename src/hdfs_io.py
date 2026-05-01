# -*- coding: utf-8 -*-
"""
HDFS 读写辅助（pandas + pyarrow HadoopFileSystem）。
需在运行环境配置好 HADOOP_HOME / JAVA_HOME，且能访问 NameNode。
"""

from __future__ import annotations

import io
import os
import subprocess
import tempfile
import uuid

import pandas as pd


def parse_hdfs_uri(uri: str):
    """
    解析 hdfs://host:port/abs/path → (host, port, abs_path)。
    支持 hdfs://host/path（无端口时默认 9000）。
    """
    if not uri.startswith("hdfs://"):
        raise ValueError("非 HDFS URI: {}".format(uri))
    rest = uri[len("hdfs://") :]
    slash = rest.find("/")
    if slash < 0:
        raise ValueError("HDFS URI 缺少路径: {}".format(uri))
    hostport = rest[:slash]
    path = rest[slash:]
    if not path.startswith("/"):
        path = "/" + path
    if ":" in hostport:
        host, port_s = hostport.rsplit(":", 1)
        port = int(port_s)
    else:
        host = hostport
        port = 9000
    return host, port, path


def _hdfs_fs():
    from pyarrow import fs as pafs

    from src import config

    return pafs.HadoopFileSystem(
        host=config.HDFS_NAMENODE_HOST,
        port=config.HDFS_NAMENODE_PORT,
        user=config.HDFS_USER,
    )


def _namenode_container():
    """Docker HDFS fallback: NameNode container name."""
    return os.environ.get("HDFS_DOCKER_NAMENODE", "hdfs-namenode")


def _docker_exec_hdfs(args, capture_output=False):
    cmd = ["docker", "exec", _namenode_container(), "hdfs", "dfs"] + list(args)
    return subprocess.run(
        cmd,
        check=True,
        stdout=subprocess.PIPE if capture_output else None,
        stderr=subprocess.PIPE if capture_output else None,
    )


def _decode_csv_bytes(data, encoding=None):
    encodings = [encoding] if encoding else []
    encodings.extend(["utf-8", "gbk", "gb18030"])
    seen = set()
    ordered = []
    for enc in encodings:
        if not enc or enc in seen:
            continue
        seen.add(enc)
        ordered.append(enc)
    last_err = None
    for enc in ordered:
        try:
            text = data.decode(enc)
            pdf = pd.read_csv(io.StringIO(text), dtype=str, on_bad_lines="skip")
            return pdf, enc
        except Exception as e:
            last_err = e
            continue
    raise RuntimeError(
        "无法从 HDFS 解码 CSV（已尝试 {}）: {}".format(ordered, last_err)
    )


def _should_fallback_to_docker(err):
    msg = str(err)
    return (
        os.environ.get("HDFS_IO_MODE", "").lower() == "docker"
        or "Unable to load libhdfs" in msg
        or "hdfs.dll" in msg
        or "libhdfs" in msg
    )


def _read_csv_pandas_docker(hpath, encoding=None):
    proc = _docker_exec_hdfs(["-cat", hpath], capture_output=True)
    return _decode_csv_bytes(proc.stdout, encoding)


def _write_parquet_from_pandas_docker(pdf: pd.DataFrame, hpath: str) -> None:
    import pyarrow as pa
    import pyarrow.parquet as pq

    parent = hpath.rsplit("/", 1)[0]
    tmp_name = "mb_{}.parquet".format(uuid.uuid4().hex)
    container_tmp = "/tmp/{}".format(tmp_name)
    with tempfile.TemporaryDirectory() as tmpdir:
        local_tmp = os.path.join(tmpdir, tmp_name)
        table = pa.Table.from_pandas(pdf, preserve_index=False)
        pq.write_table(
            table,
            local_tmp,
            coerce_timestamps="us",
            allow_truncated_timestamps=True,
        )
        if parent:
            _docker_exec_hdfs(["-mkdir", "-p", parent])
        subprocess.run(
            ["docker", "cp", local_tmp, "{}:{}".format(_namenode_container(), container_tmp)],
            check=True,
        )
        try:
            _docker_exec_hdfs(["-put", "-f", container_tmp, hpath])
        finally:
            subprocess.run(
                ["docker", "exec", _namenode_container(), "rm", "-f", container_tmp],
                check=False,
            )


def _read_parquet_to_pandas_docker(hpath: str) -> pd.DataFrame:
    tmp_name = "mb_{}_parquet".format(uuid.uuid4().hex)
    container_tmp = "/tmp/{}".format(tmp_name)
    with tempfile.TemporaryDirectory() as tmpdir:
        local_tmp = os.path.join(tmpdir, tmp_name)
        try:
            _docker_exec_hdfs(["-copyToLocal", "-f", hpath, container_tmp])
            subprocess.run(
                ["docker", "cp", "{}:{}".format(_namenode_container(), container_tmp), local_tmp],
                check=True,
            )
            return pd.read_parquet(local_tmp)
        finally:
            subprocess.run(
                ["docker", "exec", _namenode_container(), "rm", "-rf", container_tmp],
                check=False,
            )


def read_csv_pandas(path: str, encoding=None):
    """
    从 HDFS 读取 CSV，编码尝试顺序与 preprocess 一致。
    返回 (DataFrame, encoding_used)。
    """
    _, _, hpath = parse_hdfs_uri(path)
    try:
        fs = _hdfs_fs()
        with fs.open_input_stream(hpath) as stream:
            data = stream.read()
        return _decode_csv_bytes(data, encoding)
    except Exception as e:
        if not _should_fallback_to_docker(e):
            raise
        return _read_csv_pandas_docker(hpath, encoding)


def write_parquet_from_pandas(pdf: pd.DataFrame, uri: str) -> None:
    import pyarrow as pa
    import pyarrow.parquet as pq

    _, _, hpath = parse_hdfs_uri(uri)
    try:
        fs = _hdfs_fs()
        parent = hpath.rsplit("/", 1)[0]
        if parent:
            try:
                fs.create_dir(parent, recursive=True)
            except Exception:
                pass
        table = pa.Table.from_pandas(pdf, preserve_index=False)
        pq.write_table(
            table,
            hpath,
            filesystem=fs,
            coerce_timestamps="us",
            allow_truncated_timestamps=True,
        )
    except Exception as e:
        if not _should_fallback_to_docker(e):
            raise
        _write_parquet_from_pandas_docker(pdf, hpath)


def read_parquet_to_pandas(uri: str) -> pd.DataFrame:
    import pyarrow.parquet as pq

    _, _, hpath = parse_hdfs_uri(uri)
    try:
        fs = _hdfs_fs()
        table = pq.read_table(hpath, filesystem=fs)
        return table.to_pandas()
    except Exception as e:
        if not _should_fallback_to_docker(e):
            raise
        return _read_parquet_to_pandas_docker(hpath)


def is_hdfs_uri(s: str | None) -> bool:
    return isinstance(s, str) and s.startswith("hdfs://")
