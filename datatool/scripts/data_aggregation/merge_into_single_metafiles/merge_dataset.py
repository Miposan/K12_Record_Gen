"""
重新整合数据
"""
import os
import sys

import uuid
import pickle
import random
import shutil

from glob import glob
from tqdm import tqdm
from copy import deepcopy
from omegaconf import OmegaConf
from filelock import FileLock

from datatool.config import gconfig
from datatool.utils.parallel import pre_allocated_multiprocess
from datatool.utils.file_io import (
    load_json, save_json, load_jsonlines, save_pickle, detect_image_type, load_media_size, save_jsonlines
)
from datatool.logger import log
from datatool.utils.format_check import extract_boxed_answer

def filter_fields(data):
    # 只保留需要的字段
    keep_keys = ["uuid", "conversations", "metadata", "images", "videos", "audios"]
    return {k: v for k, v in data.items() if k in keep_keys and v is not None}


def extract_data_iter(all_files, jsonl_size=10000):
    batch = []
    for c_file in tqdm(all_files):
        for c_data in load_jsonlines(c_file):
            batch.append(filter_fields(c_data))
            if len(batch) >= jsonl_size:
                yield batch
                batch = []
    if batch:
        yield batch

@pre_allocated_multiprocess
def process(all_files, meta_save_dir=None, chunk_size=None, process_id=0, lock_path=None, merged_filename="merged.jsonl"):
    if not chunk_size or chunk_size <= 0:
        save_path = os.path.join(meta_save_dir, merged_filename)
        lock_path = save_path + ".lock"
        for batch in extract_data_iter(all_files, jsonl_size=10000):
            with FileLock(lock_path):
                save_jsonlines(batch, save_path, mode="a")
    else:
        for idx, batch in enumerate(extract_data_iter(all_files, jsonl_size=chunk_size)):
            save_path = os.path.join(meta_save_dir, f"meta_{process_id:06d}_{idx:06d}.jsonl")
            save_jsonlines(batch, save_path)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--save_dir", type=str, required=True,
                        help="合并后数据保存目录")
    parser.add_argument("--dataset_yaml", type=str, required=True,
                        help="输入数据集YAML配置文件路径")
    parser.add_argument("--dst_dataset_yaml", type=str, required=True,
                        help="输出数据集YAML配置文件路径")
    parser.add_argument("--chunk_size", type=int, default=1000,
                        help="每个jsonl文件最大条数，None或0表示全部合并到一个文件")
    parser.add_argument("--num_workers", type=int, default=1,
                        help="并行进程数")
    parser.add_argument("--merged_filename", type=str, default="merged.jsonl",
                        help="合并输出的jsonl文件名，默认merged.jsonl, 如果是多个jsonl文件分批存，该个参数无效")
    args = parser.parse_args()

    dataset_config = OmegaConf.load(args.dataset_yaml)
    log(f"加载数据集配置: {args.dataset_yaml}")

    updated_configs = {}
    for dataset, config in dataset_config["Datasets"].items():
        log(f"合并 {dataset}")
        meta_dirs = config["MetaFiles"]
        if isinstance(meta_dirs, str):
            meta_dirs = [meta_dirs]
        meta_save_dir = os.path.join(args.save_dir, dataset, "MetaFiles")
        os.makedirs(meta_save_dir, exist_ok=True)
        all_files = []
        for meta_dir in meta_dirs:
            all_files.extend(glob(os.path.join(meta_dir, "**", "*.jsonl"), recursive=True))
        lock_path = os.path.join(meta_save_dir, args.merged_filename + ".lock")
        process(all_files, num_workers=args.num_workers, meta_save_dir=meta_save_dir, chunk_size=args.chunk_size, lock_path=lock_path, merged_filename=args.merged_filename)
        updated_configs[dataset] = {"MetaFiles": meta_save_dir}

    new_dataset_config = {
        "DataDir": args.save_dir,
        "Datasets": updated_configs
    }
    with open(args.dst_dataset_yaml, "w") as f:
        OmegaConf.save(new_dataset_config, f)
    log(f"保存新数据集配置到 {args.dst_dataset_yaml}")

    # 全部数据集处理完后，统一删除 lock 文件
    for dataset, config in dataset_config["Datasets"].items():
        meta_save_dir = os.path.join(args.save_dir, dataset, "MetaFiles")
        lock_file = os.path.join(meta_save_dir, args.merged_filename + ".lock")
        if os.path.exists(lock_file):
            os.remove(lock_file)