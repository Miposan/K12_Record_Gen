"""
数据集重建脚本 - 符合原有逻辑结构（修复版）

功能描述:
    这个脚本用于重建和整合数据集，输入一个包含多个数据集的目录路径，
    自动遍历各数据集的MetaFiles目录，收集媒体文件并去重重建。

主要功能:
    1. 🚀 多进程并发处理数据集重建
    2. 📁 自动遍历目录下的数据集MetaFiles
    3. 🔄 从jsonl中收集媒体文件并去重
    4. 📦 重新组织MediaFiles目录结构
    5. 🛡️ 错误处理和进度显示

工作流程:
    1. 扫描输入目录，找到所有数据集的MetaFiles
    2. 并发处理每个数据集
    3. 从jsonl收集媒体文件路径并去重
    4. 复制到新的MediaFiles/{media_type}/{idx}.{suffix}结构
    5. 更新jsonl中的路径引用
    6. 生成新的配置文件

使用示例:
    # 重建数据集
    python reconstruct_dataset.py \\
        --save_dir /workspace/tmp_data_reconstructed \\
        --datasets_dir /workspace/source_datasets \\
        --num_workers 8
    
    # 使用自定义MetaFiles目录名
    python reconstruct_dataset.py \\
        --save_dir /workspace/tmp_data_reconstructed \\
        --datasets_dir /workspace/source_datasets \\
        --metafiles_name CustomMetaFiles \\
        --num_workers 8
    
    # 复制媒体文件（使用并发哈希计算）
    python reconstruct_dataset.py \\
        --save_dir /workspace/tmp_data_reconstructed \\
        --datasets_dir /workspace/source_datasets \\
        --copy_media \\
        --hash_threads 8 \\
        --num_workers 8
"""

import os
import sys
import uuid
import shutil
import yaml
import json
import hashlib
from pathlib import Path
from glob import glob
from tqdm import tqdm
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
import threading

from datatool.utils.parallel import post_allocated_multiprocess, post_allocated_multithread
from datatool.logger import log


def get_file_hash_fast(file_path):
    """快速获取文件哈希值，优化版本
    - 首先比较文件大小进行初步去重
    - 对于大文件，只计算文件头和尾部的哈希
    - 对于小文件，计算完整MD5
    """
    try:
        file_path = Path(file_path)
        file_size = file_path.stat().st_size
        
        # 空文件直接返回特殊哈希
        if file_size == 0:
            return "empty_file"
        
        # 小文件（<10MB）计算完整MD5
        if file_size < 10 * 1024 * 1024:
            hash_md5 = hashlib.md5()
            with open(file_path, "rb") as f:
                for chunk in iter(lambda: f.read(8192), b""):
                    hash_md5.update(chunk)
            return hash_md5.hexdigest()
        
        # 大文件只计算头部、中间、尾部的哈希
        hash_md5 = hashlib.md5()
        with open(file_path, "rb") as f:
            # 读取文件头部 1MB
            head_data = f.read(1024 * 1024)
            hash_md5.update(head_data)
            
            # 读取文件中间 1MB
            if file_size > 2 * 1024 * 1024:
                f.seek(file_size // 2)
                middle_data = f.read(1024 * 1024)
                hash_md5.update(middle_data)
            
            # 读取文件尾部 1MB
            if file_size > 1024 * 1024:
                f.seek(max(0, file_size - 1024 * 1024))
                tail_data = f.read()
                hash_md5.update(tail_data)
        
        # 将文件大小也加入哈希计算，增加唯一性
        hash_md5.update(str(file_size).encode())
        return f"fast_{hash_md5.hexdigest()}"
        
    except Exception as e:
        log(f"⚠️ 计算文件哈希失败 {file_path}: {e}")
        return None


def get_file_hash(file_path):
    """获取文件的MD5哈希值，用于去重（保持向后兼容）"""
    return get_file_hash_fast(file_path)


@post_allocated_multithread
def compute_hash_for_file_thread(file_info, **kwargs):
    """并发计算单个文件的哈希值（线程安全）"""
    thread_id = kwargs.get('thread_id', 'unknown')
    
    try:
        src_path = file_info["src_path"]
        file_hash = get_file_hash(src_path)
        
        return {
            "status": "success",
            "src_path": src_path,
            "file_hash": file_hash,
            "media_file": file_info,
            "worker_id": thread_id
        }
    except Exception as e:
        return {
            "status": "error",
            "src_path": file_info.get("src_path", "unknown"),
            "error": str(e),
            "worker_id": thread_id
        }


@post_allocated_multiprocess 
def compute_hash_for_file_process(file_info, **kwargs):
    """并发计算单个文件的哈希值（进程安全）"""
    process_id = kwargs.get('process_id', 'unknown')
    
    try:
        src_path = file_info["src_path"]
        file_hash = get_file_hash(src_path)
        
        return {
            "status": "success",
            "src_path": src_path,
            "file_hash": file_hash,
            "media_file": file_info,
            "worker_id": process_id
        }
    except Exception as e:
        return {
            "status": "error",
            "src_path": file_info.get("src_path", "unknown"),
            "error": str(e),
            "worker_id": process_id
        }


def process_hash_group_concurrent(media_files_group, media_counters, unique_files, path_mapping, media_save_dir, dataset_name, max_workers=4, use_process=False):
    """并发处理需要计算哈希的文件组"""
    
    # 准备哈希计算任务
    hash_tasks = []
    for media_file in media_files_group:
        src_path = Path(media_file["src_path"])
        if src_path.exists():
            hash_tasks.append(media_file)
    
    if not hash_tasks:
        return
    
    worker_type = "进程" if use_process else "线程"
    log(f"🔧 {dataset_name}: 并发计算 {len(hash_tasks)} 个文件的哈希值 (使用 {min(max_workers, len(hash_tasks))} 个{worker_type})...")
    
    # 选择并发计算方式
    if use_process:
        # CPU密集型：使用多进程
        hash_results = compute_hash_for_file_process(
            hash_tasks,
            num_workers=min(max_workers, len(hash_tasks))
        )
    else:
        # I/O密集型：使用多线程
        hash_results = compute_hash_for_file_thread(
            hash_tasks,
            num_workers=min(max_workers, len(hash_tasks))
        )
    
    # 处理哈希结果，进行去重和复制
    group_hash_map = {}
    for result in hash_results:
        if result["status"] != "success" or not result["file_hash"]:
            continue
        
        file_hash = result["file_hash"]
        media_file = result["media_file"]
        src_path = Path(media_file["src_path"])
        
        if file_hash not in group_hash_map:
            # 新的唯一文件
            media_type = media_file["media_type"]
            media_counters[media_type] += 1
            idx = media_counters[media_type]
            suffix = media_file["suffix"]
            
            # 生成新的文件名和路径
            new_filename = f"{idx}{suffix}"
            dst_subdir = Path(media_save_dir) / media_type
            dst_subdir.mkdir(parents=True, exist_ok=True)
            dst_path = dst_subdir / new_filename
            
            # 复制文件
            try:
                shutil.copy2(src_path, dst_path)
                
                group_hash_map[file_hash] = {
                    "src_path": str(src_path),
                    "dst_path": str(dst_path),
                    "media_type": media_type,
                    "idx": idx,
                    "suffix": suffix
                }
                
                unique_files[file_hash] = group_hash_map[file_hash]
                
                # 记录路径映射（绝对路径）
                path_mapping[str(src_path)] = str(dst_path)
                
            except Exception as e:
                log(f"⚠️ 复制文件失败 {src_path} -> {dst_path}: {e}")
                continue
        else:
            # 重复文件，使用已有的路径映射
            existing_info = group_hash_map[file_hash]
            path_mapping[str(src_path)] = existing_info["dst_path"]


@post_allocated_multiprocess
def process_single_jsonl_file(jsonl_file_info, **kwargs):
    """处理单个jsonl文件（多进程安全）"""
    jsonl_file, dataset_name, meta_save_dir, media_save_dir = jsonl_file_info
    process_id = kwargs.get('process_id', 0)
    
    try:
        log(f"🔧 进程 {process_id} 开始处理: {Path(jsonl_file).name}")
        
        # 读取jsonl数据
        jsonl_data = []
        media_files = []
        
        with open(jsonl_file, 'r', encoding='utf-8') as f:
            for line_idx, line in enumerate(f):
                line = line.strip()
                if not line:
                    continue
                try:
                    item = json.loads(line)
                    item_id = item.get("id", f"item_{line_idx}")
                    
                    # 保存原始数据
                    jsonl_data.append({
                        "data": item,
                        "line_idx": line_idx,
                        "src_file": str(jsonl_file)
                    })
                    
                    # 收集媒体文件路径
                    for media_type in ["images", "videos", "audios"]:
                        media_paths = item.get(media_type, [])
                        if isinstance(media_paths, list):
                            for idx, media_path in enumerate(media_paths):
                                if media_path and Path(media_path).exists():
                                    media_files.append({
                                        "item_id": item_id,
                                        "line_idx": line_idx,
                                        "media_type": media_type,
                                        "media_idx": idx,
                                        "src_path": str(Path(media_path)),
                                        "suffix": Path(media_path).suffix,
                                        "from_jsonl": str(jsonl_file)
                                    })
                                    
                except json.JSONDecodeError as e:
                    log(f"⚠️ JSON解析错误 {jsonl_file}:{line_idx}: {e}")
                    continue
        
        return {
            "status": "success",
            "jsonl_file": str(jsonl_file),
            "dataset_name": dataset_name,
            "jsonl_data": jsonl_data,
            "media_files": media_files,
            "sample_count": len(jsonl_data),
            "process_id": process_id
        }
        
    except Exception as e:
        log(f"❌ 进程 {process_id} 处理文件失败 {jsonl_file}: {e}")
        return {
            "status": "error",
            "jsonl_file": str(jsonl_file),
            "error": str(e),
            "process_id": process_id
        }


def deduplicate_and_rebuild_dataset(dataset_results, meta_save_dir, media_save_dir, dataset_name, copy_media_files=True, hash_threads=4, use_process=False):
    """去重并重建单个数据集"""
    # 合并所有数据
    all_jsonl_data = []
    all_media_files = []
    
    for result in dataset_results:
        if result["status"] == "success":
            all_jsonl_data.extend(result["jsonl_data"])
            all_media_files.extend(result["media_files"])
    
    if not all_jsonl_data:
        log(f"⚠️ {dataset_name}: 没有有效的数据")
        return 0
    
    # 检查是否需要复制媒体文件
    if not copy_media_files or media_save_dir is None:
        log(f"📋 {dataset_name}: 跳过媒体文件复制，仅处理MetaFiles")
        
        # 仅保存jsonl数据，不更新路径
        updated_data = []
        for item_info in all_jsonl_data:
            updated_data.append(item_info["data"])
        
        # 保存jsonl文件
        jsonl_path = os.path.join(meta_save_dir, f"{dataset_name}.jsonl")
        with open(jsonl_path, 'w', encoding='utf-8') as f:
            for item in updated_data:
                f.write(json.dumps(item, ensure_ascii=False) + '\n')
        
        log(f"📁 保存jsonl: {jsonl_path} ({len(updated_data)} 个数据项)")
        return len(updated_data)
    
    log(f"🔍 {dataset_name}: 开始媒体文件去重处理...")
    
    # 去重处理 - 优化版本
    unique_files = {}  # {file_hash: file_info}
    path_mapping = {}  # {src_path: new_rel_path}
    media_counters = defaultdict(int)  # {media_type: count}
    size_groups = defaultdict(list)  # {file_size: [media_files]} - 按文件大小分组
    
    log(f"🔍 {dataset_name}: 第一阶段 - 按文件大小分组...")
    # 第一阶段：按文件大小分组，快速过滤
    for media_file in tqdm(all_media_files, desc=f"分组{dataset_name}媒体文件"):
        src_path = Path(media_file["src_path"])
        
        if not src_path.exists():
            log(f"⚠️ 文件不存在，跳过: {src_path}")
            continue
        
        try:
            file_size = src_path.stat().st_size
            media_file["file_size"] = file_size
            size_groups[file_size].append(media_file)
        except Exception as e:
            log(f"⚠️ 获取文件大小失败 {src_path}: {e}")
            continue
    
    log(f"📊 {dataset_name}: 大小分组统计 - {len(size_groups)} 个不同大小的组")
    
    log(f"🔍 {dataset_name}: 第二阶段 - 计算哈希去重...")
    # 第二阶段：对每个大小组内的文件计算哈希
    for file_size, media_files_group in tqdm(size_groups.items(), desc=f"处理{dataset_name}大小组"):
        if len(media_files_group) == 1:
            # 该大小只有一个文件，无需计算哈希，直接处理
            media_file = media_files_group[0]
            src_path = Path(media_file["src_path"])
            
            media_type = media_file["media_type"]
            media_counters[media_type] += 1
            idx = media_counters[media_type]
            suffix = media_file["suffix"]
            
            # 生成新的文件名和路径
            new_filename = f"{idx}{suffix}"
            dst_subdir = Path(media_save_dir) / media_type
            dst_subdir.mkdir(parents=True, exist_ok=True)
            dst_path = dst_subdir / new_filename
            
            # 复制文件
            try:
                shutil.copy2(src_path, dst_path)
                
                # 使用文件大小作为唯一标识（无需计算哈希）
                size_hash = f"size_{file_size}_{str(src_path)}"
                unique_files[size_hash] = {
                    "src_path": str(src_path),
                    "dst_path": str(dst_path),
                    "media_type": media_type,
                    "idx": idx,
                    "suffix": suffix
                }
                
                # 记录路径映射（绝对路径）
                path_mapping[str(src_path)] = str(dst_path)
                
            except Exception as e:
                log(f"⚠️ 复制文件失败 {src_path} -> {dst_path}: {e}")
                continue
        
        else:
            # 该大小有多个文件，需要计算哈希去重（使用并发）
            process_hash_group_concurrent(
                media_files_group, 
                media_counters, 
                unique_files, 
                path_mapping, 
                media_save_dir, 
                dataset_name,
                max_workers=hash_threads,
                use_process=use_process
            )
    
    log(f"📊 {dataset_name} 媒体文件处理统计:")
    log(f"   原始文件数: {len(all_media_files)}")
    log(f"   去重后文件数: {len(unique_files)}")
    log(f"   实际复制文件数: {len(unique_files)}")
    for media_type, count in media_counters.items():
        log(f"   {media_type}: {count} 个文件")
    
    # 更新jsonl数据中的路径
    updated_data = []
    for item_info in all_jsonl_data:
        item = item_info["data"].copy()
        
        # 更新媒体文件路径
        for media_type in ["images", "videos", "audios"]:
            if media_type in item:
                media_paths = item[media_type]
                if isinstance(media_paths, list):
                    new_paths = []
                    for src_path in media_paths:
                        src_path_str = str(Path(src_path))
                        if src_path_str in path_mapping:
                            new_paths.append(path_mapping[src_path_str])
                        else:
                            log(f"⚠️ 找不到路径映射: {src_path}")
                            new_paths.append(src_path)  # 保留原路径
                    item[media_type] = new_paths
        
        updated_data.append(item)
    
    # 保存更新后的jsonl文件
    jsonl_path = os.path.join(meta_save_dir, f"{dataset_name}.jsonl")
    with open(jsonl_path, 'w', encoding='utf-8') as f:
        for item in updated_data:
            f.write(json.dumps(item, ensure_ascii=False) + '\n')
    
    log(f"📁 保存更新后的jsonl: {jsonl_path} ({len(updated_data)} 个数据项)")
    return len(updated_data)


def stat_sample_nums(meta_save_dir):
    """统计样本数量"""
    total_samples = 0
    jsonl_files = glob(os.path.join(meta_save_dir, "*.jsonl"))
    
    for jsonl_file in jsonl_files:
        try:
            with open(jsonl_file, 'r', encoding='utf-8') as f:
                for line in f:
                    if line.strip():
                        total_samples += 1
        except Exception as e:
            log(f"⚠️ 统计文件失败 {jsonl_file}: {e}")
    
    return total_samples


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="数据集重建工具 - 目录输入版本")
    parser.add_argument("--save_dir", type=str, required=True,
                        help="重建数据集的保存目录")
    parser.add_argument("--datasets_dir", type=str, required=True,
                        help="源数据集目录路径（包含多个数据集子目录）")
    parser.add_argument("--save_yaml", type=str, default=None,
                        help="重建数据集的配置文件保存路径（可选）")
    parser.add_argument("--metafiles_name", type=str, default="MetaFiles",
                        help="MetaFiles目录名称 (默认: MetaFiles)")
    parser.add_argument("--copy_media", action="store_true", default=False,
                        help="是否复制媒体文件 (默认: False，不复制)")
    parser.add_argument("--hash_threads", type=int, default=16,
                        help="哈希计算并发线程数 (默认: 16)")
    parser.add_argument("--use_process", action="store_true",
                        help="使用多进程进行哈希计算（CPU密集型优化）")
    parser.add_argument("--num_workers", type=int, default=8,
                        help="并发进程数")
    
    args = parser.parse_args()
    
    # 检查输入目录
    datasets_dir = Path(args.datasets_dir)
    if not datasets_dir.exists():
        log(f"❌ 输入目录不存在: {datasets_dir}")
        sys.exit(1)
    
    log(f"🔧 开始重建数据集")
    log(f"  源目录: {datasets_dir}")
    log(f"  目标目录: {args.save_dir}")
    log(f"  MetaFiles目录名: {args.metafiles_name}")
    log(f"  复制媒体文件: {args.copy_media}")
    log(f"  哈希计算线程数: {args.hash_threads}")
    log(f"  使用多进程哈希: {args.use_process}")
    log(f"  进程数: {args.num_workers}")
    
    reconstruct_total_sample_nums = 0
    updated_configs = {}
    
    # 自动发现数据集
    dataset_dirs = [d for d in datasets_dir.iterdir() if d.is_dir()]
    
    for dataset_dir in dataset_dirs:
        dataset_name = dataset_dir.name
        metafiles_dir = dataset_dir / args.metafiles_name
        
        if not metafiles_dir.exists():
            log(f"⚠️ 跳过 {dataset_name}: {args.metafiles_name}目录不存在")
            continue
        
        log(f"🔄 重建数据集: {dataset_name}")
        
        # 创建保存目录
        meta_save_dir = os.path.join(args.save_dir, dataset_name, args.metafiles_name)
        os.makedirs(meta_save_dir, exist_ok=True)
        
        # 只有需要复制媒体文件时才创建MediaFiles目录
        if args.copy_media:
            media_save_dir = os.path.join(args.save_dir, dataset_name, "MediaFiles")
            os.makedirs(media_save_dir, exist_ok=True)
        else:
            media_save_dir = None
        
        # 处理jsonl文件
        all_files = glob(os.path.join(str(metafiles_dir), "**", "*.jsonl"), recursive=True)
        
        if not all_files:
            log(f"⚠️ 跳过 {dataset_name}: 未找到jsonl文件")
            continue
        
        log(f"📋 {dataset_name}: 找到 {len(all_files)} 个jsonl文件")
        
        # 准备多进程任务
        jsonl_tasks = []
        for jsonl_file in all_files:
            jsonl_tasks.append([jsonl_file, dataset_name, meta_save_dir, media_save_dir])
        
        # 多进程处理jsonl文件
        log(f"🚀 {dataset_name}: 开始多进程处理 (使用 {min(args.num_workers, len(jsonl_tasks))} 个进程)...")
        dataset_results = process_single_jsonl_file(
            jsonl_tasks,
            num_workers=min(args.num_workers, len(jsonl_tasks))
        )
        
        # 去重并重建数据集
        c_sample_nums = deduplicate_and_rebuild_dataset(
            dataset_results, meta_save_dir, media_save_dir, dataset_name, args.copy_media, args.hash_threads, args.use_process
        )
        
        reconstruct_total_sample_nums += c_sample_nums
        
        updated_configs[dataset_name] = {
            args.metafiles_name: meta_save_dir,
            "sample_nums": c_sample_nums
        }
        
        log(f"✅ 完成重建: {dataset_name} ({c_sample_nums} 个样本)")
    
    # 保存配置文件
    if args.save_yaml is not None:
        new_dataset_config = {
            "DataDir": args.save_dir,
            "Datasets": updated_configs,
            'TotalSampleNums': reconstruct_total_sample_nums
        }
        
        with open(args.save_yaml, "w", encoding='utf-8') as f:
            yaml.dump(new_dataset_config, f, default_flow_style=False, allow_unicode=True)
        
        log(f"📋 保存新配置文件: {args.save_yaml}")
    
    log(f"🎉 重建完成!")
    log(f"  处理数据集: {len(updated_configs)}")
    log(f"  总样本数: {reconstruct_total_sample_nums}")
    log(f"  保存目录: {args.save_dir}")
