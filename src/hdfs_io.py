# -*- coding: utf-8 -*-
"""
HDFS 读写辅助（pandas + pyarrow HadoopFileSystem）。
需在运行环境配置好 HADOOP_HOME / JAVA_HOME，且能访问 NameNode。
"""

from __future__ import annotations

import io

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


def read_csv_pandas(path: str, encoding=None):
    """
    从 HDFS 读取 CSV，编码尝试顺序与 preprocess 一致。
    返回 (DataFrame, encoding_used)。
    """
    _, _, hpath = parse_hdfs_uri(path)
    fs = _hdfs_fs()
    with fs.open_input_stream(hpath) as stream:
        data = stream.read()
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


def write_parquet_from_pandas(pdf: pd.DataFrame, uri: str) -> None:
    import pyarrow as pa
    import pyarrow.parquet as pq

    _, _, hpath = parse_hdfs_uri(uri)
    fs = _hdfs_fs()
    parent = hpath.rsplit("/", 1)[0]
    if parent:
        try:
            fs.create_dir(parent, recursive=True)
        except Exception:
            pass
    table = pa.Table.from_pandas(pdf, preserve_index=False)
    pq.write_table(table, hpath, filesystem=fs)


def read_parquet_to_pandas(uri: str) -> pd.DataFrame:
    import pyarrow.parquet as pq

    _, _, hpath = parse_hdfs_uri(uri)
    fs = _hdfs_fs()
    table = pq.read_table(hpath, filesystem=fs)
    return table.to_pandas()


def is_hdfs_uri(s: str | None) -> bool:
    return isinstance(s, str) and s.startswith("hdfs://")
