"""
数据集合并脚本 - 多进程并发版本

功能描述:
    这个脚本用于将多个数据集合并到目标数据集中，支持多进程并发处理。
    只处理MetaFiles和MediaFiles

主要功能:
    1. 🚀 多进程并发处理数据集合并
    2. 📁 复制MetaFiles和MediaFiles到目标位置
    3. 📋 自动更新YAML配置文件
    4. 🔄 支持新数据集类别的添加
    5. 🛡️ 错误处理和进度显示

工作流程:
    1. 加载目标数据集配置
    2. 遍历要合并的数据集配置文件
    3. 多进程并发复制数据集内容
    4. 更新目标YAML配置文件

使用示例:
    # 将tmp.yaml中的数据集复制到目标目录
    python merge_dataset.py \\
        --src_dataset_yaml /path/to/tmp.yaml \\
        --dst_root_dir /path/to/target/directory \\
        --num_workers 4

    # 合并多个配置文件到现有目标（使用并发哈希）
    python merge_dataset.py \\
        --src_dataset_yaml /path/to/tmp.yaml \\
        --dst_root_dir /path/to/target/directory \\
        --copy_media \\
        --hash_threads 16 \\
        --num_workers 8

注意事项:
    - 确保目标目录有足够的存储空间
    - 合并前建议备份重要数据
    - 进程数建议根据磁盘I/O能力调整
"""

import os
import sys
import uuid
import shutil
import yaml
import json
from pathlib import Path
from glob import glob
from tqdm import tqdm

import hashlib
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
import threading

from datatool.utils.parallel import post_allocated_multiprocess, post_allocated_multithread
from datatool.logger import log


def simple_copy_dataset(src_dataset, dst_dataset, dataset_name):
    """将 src_dataset 合并到 dst_dataset 中（只处理MetaFiles和MediaFiles）"""
    src_metadir = Path(src_dataset["MetaFiles"])
    dst_metadir = Path(dst_dataset["MetaFiles"])
    
    # 确保目标MetaFiles目录存在
    dst_metadir.mkdir(parents=True, exist_ok=True)
    
    # 复制MetaFiles
    if src_metadir.exists():
        # 创建唯一的子目录名避免冲突
        unique_id = str(uuid.uuid4())[:8]
        copyto_metadir = dst_metadir / f"datatool_merge_{dataset_name}_{unique_id}"
        shutil.copytree(src_metadir, copyto_metadir)
        log(f"✅ 复制 MetaFiles: {src_metadir} -> {copyto_metadir}")
    else:
        log(f"⚠️ 源MetaFiles目录不存在: {src_metadir}")
    
    # 处理MediaFiles（如果存在的话）
    src_dataset_dir = src_metadir.parent
    src_mediadir = src_dataset_dir / "MediaFiles"
    
    if src_mediadir.exists():
        dst_dataset_dir = dst_metadir.parent
        dst_mediadir = dst_dataset_dir / "MediaFiles"
        dst_mediadir.mkdir(parents=True, exist_ok=True)
        
        # 创建唯一的MediaFiles子目录
        unique_id = str(uuid.uuid4())[:8]
        copyto_mediadir = dst_mediadir / f"datatool_merge_{dataset_name}_{unique_id}"
        shutil.copytree(src_mediadir, copyto_mediadir)
        log(f"✅ 复制 MediaFiles: {src_mediadir} -> {copyto_mediadir}")
    else:
        log(f"📋 源MediaFiles目录不存在: {src_mediadir}")


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


def collect_media_files_from_jsonl(metafiles_dir):
    """从MetaFiles目录的jsonl文件中收集所有媒体文件路径"""
    metafiles_dir = Path(metafiles_dir)
    media_files = []
    
    if not metafiles_dir.exists():
        log(f"⚠️ MetaFiles目录不存在: {metafiles_dir}")
        return media_files
    
    # 查找所有jsonl文件
    jsonl_files = list(metafiles_dir.glob("*.jsonl"))
    
    for jsonl_file in jsonl_files:
        try:
            with open(jsonl_file, 'r', encoding='utf-8') as f:
                for line_idx, line in enumerate(f):
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        item = json.loads(line)
                        
                        # 收集各种媒体文件路径
                        for media_type in ["images", "videos", "audios"]:
                            media_paths = item.get(media_type, [])
                            if isinstance(media_paths, list):
                                for media_path in media_paths:
                                    if media_path and Path(media_path).exists():
                                        media_files.append({
                                            "src_path": str(Path(media_path)),
                                            "media_type": media_type,
                                            "suffix": Path(media_path).suffix,
                                            "from_jsonl": str(jsonl_file)
                                        })
                                        
                    except json.JSONDecodeError as e:
                        log(f"⚠️ JSON解析错误 {jsonl_file}:{line_idx}: {e}")
                        continue
                        
        except Exception as e:
            log(f"⚠️ 读取jsonl文件失败 {jsonl_file}: {e}")
            continue
    
    return media_files


def copy_media_files_with_dedup(media_files, dst_mediadir, dataset_name, hash_threads=4, use_process=False):
    """复制媒体文件并去重（优化版：两阶段去重 + 并发哈希）"""
    dst_mediadir = Path(dst_mediadir)
    dst_mediadir.mkdir(parents=True, exist_ok=True)
    
    # 去重处理 - 优化版本
    unique_files = {}  # {file_hash: file_info}
    media_counters = defaultdict(int)  # {media_type: count}
    copied_files = []
    path_mapping = {}  # {src_path: dst_path} - 路径映射表
    size_groups = defaultdict(list)  # {file_size: [media_files]} - 按文件大小分组
    
    log(f"🔍 {dataset_name}: 第一阶段 - 按文件大小分组...")
    # 第一阶段：按文件大小分组，快速过滤
    for media_file in tqdm(media_files, desc=f"分组{dataset_name}媒体文件"):
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
            dst_subdir = dst_mediadir / media_type
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
                
                copied_files.append({
                    "src_path": str(src_path),
                    "dst_path": str(dst_path),
                    "media_type": media_type
                })
                
                # 记录路径映射（绝对路径）
                path_mapping[str(src_path)] = str(dst_path)
                
            except Exception as e:
                log(f"⚠️ 复制文件失败 {src_path} -> {dst_path}: {e}")
                continue
        
        else:
            # 该大小有多个文件，需要计算哈希去重（使用并发）
            process_hash_group_concurrent_merge(
                media_files_group, 
                media_counters, 
                unique_files, 
                path_mapping, 
                copied_files,
                dst_mediadir, 
                dataset_name,
                max_workers=hash_threads,
                use_process=use_process
            )
    
    log(f"📊 {dataset_name} 媒体文件处理统计:")
    log(f"   原始文件数: {len(media_files)}")
    log(f"   去重后文件数: {len(unique_files)}")
    log(f"   实际复制文件数: {len(copied_files)}")
    for media_type, count in media_counters.items():
        log(f"   {media_type}: {count} 个文件")
    
    return copied_files, path_mapping


def process_hash_group_concurrent_merge(media_files_group, media_counters, unique_files, path_mapping, copied_files, dst_mediadir, dataset_name, max_workers=4, use_process=False):
    """并发处理需要计算哈希的文件组（merge专用）"""
    
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
            dst_subdir = dst_mediadir / media_type
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
                
                copied_files.append({
                    "src_path": str(src_path),
                    "dst_path": str(dst_path),
                    "media_type": media_type
                })
                
                # 记录路径映射（绝对路径）
                path_mapping[str(src_path)] = str(dst_path)
                
            except Exception as e:
                log(f"⚠️ 复制文件失败 {src_path} -> {dst_path}: {e}")
                continue
        else:
            # 重复文件，使用已有的路径映射
            existing_info = group_hash_map[file_hash]
            path_mapping[str(src_path)] = existing_info["dst_path"]


def update_jsonl_paths(src_metadir, dst_metadir, path_mapping, dataset_name):
    """更新JSONL文件中的媒体路径为绝对路径"""
    src_metadir = Path(src_metadir)
    dst_metadir = Path(dst_metadir)
    
    if not path_mapping:
        log(f"📋 {dataset_name}: 没有路径映射，跳过JSONL更新")
        return
    
    log(f"🔧 {dataset_name}: 开始更新JSONL文件中的媒体路径...")
    
    # 查找所有jsonl文件
    jsonl_files = list(dst_metadir.glob("*.jsonl"))
    
    for jsonl_file in jsonl_files:
        try:
            updated_items = []
            
            with open(jsonl_file, 'r', encoding='utf-8') as f:
                for line_idx, line in enumerate(f):
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        item = json.loads(line)
                        
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
                                            new_paths.append(src_path)  # 保留原路径
                                    item[media_type] = new_paths
                        
                        updated_items.append(item)
                        
                    except json.JSONDecodeError as e:
                        log(f"⚠️ JSON解析错误 {jsonl_file}:{line_idx}: {e}")
                        continue
            
            # 重写更新后的jsonl文件
            with open(jsonl_file, 'w', encoding='utf-8') as f:
                for item in updated_items:
                    f.write(json.dumps(item, ensure_ascii=False) + '\n')
            
            log(f"✅ 更新JSONL文件: {jsonl_file.name} ({len(updated_items)} 个数据项)")
            
        except Exception as e:
            log(f"⚠️ 更新JSONL文件失败 {jsonl_file}: {e}")
            continue


def process_media_files(src_metadir, dst_dir, dataset_name, copy_media_files=True, hash_threads=4, use_process=False):
    """处理媒体文件：优先使用MediaFiles目录，否则从jsonl收集"""
    if not copy_media_files:
        log(f"📋 跳过媒体文件复制: {dataset_name}")
        return [], {}
    
    src_metadir = Path(src_metadir)
    dst_dir = Path(dst_dir)
    
    # 检查是否存在MediaFiles目录
    src_dataset_dir = src_metadir.parent
    src_mediadir = src_dataset_dir / "MediaFiles"
    
    if src_mediadir.exists():
        # 直接复制MediaFiles目录
        dst_mediadir = dst_dir / "MediaFiles"
        if dst_mediadir.exists():
            shutil.rmtree(dst_mediadir)
        shutil.copytree(src_mediadir, dst_mediadir)
        log(f"✅ 直接复制 MediaFiles: {src_mediadir} -> {dst_mediadir}")
        return [], {}  # 直接复制时无需路径映射
    else:
        # 从jsonl文件中收集媒体文件
        log(f"🔍 MediaFiles目录不存在，从jsonl收集媒体文件: {dataset_name}")
        media_files = collect_media_files_from_jsonl(src_metadir)
        
        if media_files:
            dst_mediadir = dst_dir / "MediaFiles"
            copied_files, path_mapping = copy_media_files_with_dedup(media_files, dst_mediadir, dataset_name, hash_threads, use_process)
            return copied_files, path_mapping
        else:
            log(f"📋 未找到媒体文件: {dataset_name}")
            return [], {}


@post_allocated_multiprocess
def process_dataset(data, **kwargs):
    """处理单个数据集的合并（多进程安全）"""
    args = kwargs["args"]
    dst_dataset_config = kwargs.get("dst_dataset_config", {})
    process_id = kwargs.get("process_id", "unknown")
    
    dataset_name, config = data
    
    try:
        log(f"🔧 进程 {process_id} 开始处理: {dataset_name}")
        
        # 确定目标目录
        dst_root_dir = Path(args.dst_root_dir)
        dst_root_dir.mkdir(parents=True, exist_ok=True)
        
        # 目标数据集目录
        dst_dir = dst_root_dir / dataset_name
        dst_dir.mkdir(parents=True, exist_ok=True)
        
        # 复制MetaFiles
        src_metadir = Path(config["MetaFiles"])
        dst_metadir = dst_dir / "MetaFiles"
        
        if src_metadir.exists():
            if dst_metadir.exists():
                shutil.rmtree(dst_metadir)
            shutil.copytree(src_metadir, dst_metadir)
            log(f"✅ 进程 {process_id} - {dataset_name}: 复制 MetaFiles 完成")
        else:
            log(f"⚠️ 进程 {process_id} - {dataset_name}: MetaFiles目录不存在")
            return {
                "status": "error",
                "dataset": dataset_name,
                "error": "MetaFiles目录不存在",
                "process_id": process_id
            }
        
        # 处理媒体文件
        copied_media_files = []
        path_mapping = {}
        if args.copy_media_files:
            copied_media_files, path_mapping = process_media_files(
                src_metadir, dst_dir, dataset_name, args.copy_media_files, args.hash_threads, args.use_process
            )
            
            # 更新JSONL文件中的路径（如果有路径映射）
            if path_mapping:
                update_jsonl_paths(src_metadir, dst_metadir, path_mapping, dataset_name)
        
        return {
            "status": "success",
            "dataset": dataset_name,
            "action": "created_new",
            "dst_dir": str(dst_dir),
            "sample_nums": config.get("sample_nums", 0),
            "copied_media_files": len(copied_media_files),
            "process_id": process_id
        }
                
    except Exception as e:
        log(f"❌ 进程 {process_id} - {dataset_name}: 处理失败 - {e}")
        return {
            "status": "error",
            "dataset": dataset_name,
            "error": str(e),
            "process_id": process_id
        }


def update_target_yaml(dst_yaml_path, results, dst_root_dir):
    """更新目标YAML配置文件"""
    dst_yaml_path = Path(dst_yaml_path)
    dst_root_dir = Path(dst_root_dir)
    
    # 生成新的配置
    new_config = {
        "DataDir": str(dst_root_dir),
        "Datasets": {},
        "TotalSampleNums": 0
    }
    
    total_samples = 0
    
    # 基于实际复制结果更新配置
    for result in results:
        if result["status"] == "success":
            dataset_name = result["dataset"]
            sample_nums = result.get("sample_nums", 0)
            
            new_config["Datasets"][dataset_name] = {
                "MetaFiles": str(dst_root_dir / dataset_name / "MetaFiles"),
                "sample_nums": sample_nums
            }
            total_samples += sample_nums
    
    new_config["TotalSampleNums"] = total_samples
    
    # 保存新配置
    with open(dst_yaml_path, 'w', encoding='utf-8') as f:
        yaml.dump(new_config, f, default_flow_style=False, allow_unicode=True)
    
    log(f"📋 更新配置文件: {dst_yaml_path}")
    log(f"📊 总数据集数: {len(new_config['Datasets'])}")
    log(f"📊 总样本数: {total_samples}")


def merge_datasets(src_yaml_path, dst_root_dir, dst_yaml_path=None, copy_media_files=True, hash_threads=16, use_process=False, num_workers=4):
    """主函数：合并数据集"""
    src_yaml_path = Path(src_yaml_path)
    dst_root_dir = Path(dst_root_dir)
    
    log(f"🔧 配置信息:")
    log(f"  源配置文件: {src_yaml_path}")
    log(f"  目标根目录: {dst_root_dir}")
    log(f"  目标配置文件: {dst_yaml_path or '(自动生成)'}")
    log(f"  复制媒体文件: {copy_media_files}")
    log(f"  哈希计算线程数: {hash_threads}")
    log(f"  使用多进程哈希: {use_process}")
    log(f"  进程数: {num_workers}")
    
    # 加载源配置
    with open(src_yaml_path, 'r', encoding='utf-8') as f:
        src_config = yaml.safe_load(f)
    
    # 加载目标配置（如果存在）
    dst_config = {}
    if dst_yaml_path and Path(dst_yaml_path).exists():
        with open(dst_yaml_path, 'r', encoding='utf-8') as f:
            dst_config = yaml.safe_load(f)
        log(f"📋 加载现有目标配置: {dst_yaml_path}")
    
    # 准备处理任务
    all_data = []
    for dataset_name, config in src_config["Datasets"].items():
        all_data.append([dataset_name, config])
    
    if not all_data:
        log("❌ 源配置中没有找到数据集")
        return
    
    log(f"📊 找到 {len(all_data)} 个数据集需要处理")
    
    # 准备参数
    class Args:
        def __init__(self):
            self.dst_root_dir = str(dst_root_dir)
            self.copy_media_files = copy_media_files
            self.hash_threads = hash_threads
    
    args = Args()
    
    # 多进程处理
    log(f"🚀 开始多进程处理 (使用 {num_workers} 个进程)...")
    results = process_dataset(
        all_data,
        num_workers=num_workers,
        dst_dataset_config=dst_config,
        args=args
    )
    
    # 统计结果
    success_count = 0
    error_count = 0
    skipped_count = 0
    total_samples = 0
    
    for result in results:
        if result["status"] == "success":
            success_count += 1
            total_samples += result.get("sample_nums", 0)
        elif result["status"] == "error":
            error_count += 1
        elif result["status"] == "skipped":
            skipped_count += 1
    
    # 生成目标配置文件
    if not dst_yaml_path:
        dst_yaml_path = dst_root_dir / "dataset_config.yaml"
    
    update_target_yaml(dst_yaml_path, results, dst_root_dir)
    
    # 输出最终统计
    log(f"🎉 处理完成!")
    log(f"  ✅ 成功处理: {success_count} 个数据集")
    log(f"  ❌ 处理失败: {error_count} 个数据集")
    log(f"  ⏭️ 跳过处理: {skipped_count} 个数据集")
    log(f"  📊 总样本数: {total_samples}")
    log(f"  📁 目标目录: {dst_root_dir}")
    log(f"  📋 配置文件: {dst_yaml_path}")


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="数据集合并工具 - 多进程并发版本")
    parser.add_argument("--src_dataset_yaml", required=True, 
                        help="源数据集YAML配置文件路径 (例如: tmp.yaml)")
    parser.add_argument("--dst_root_dir", required=True,
                        help="目标根目录路径 (例如: /workspace/tmp_data)")
    parser.add_argument("--dst_dataset_yaml", default=None,
                        help="目标数据集YAML配置文件路径 (可选，如果不指定将自动生成)")
    parser.add_argument("--copy_media", action="store_true",
                        help="是否复制媒体文件 (默认: True)")
    parser.add_argument("--hash_threads", type=int, default=16,
                        help="哈希计算并发线程数 (默认: 16)")
    parser.add_argument("--use_process", action="store_true",
                        help="使用多进程进行哈希计算（CPU密集型优化）")
    parser.add_argument("--num_workers", type=int, default=4,
                        help="并发进程数 (默认: 4)")
    
    args = parser.parse_args()
    
    merge_datasets(
        src_yaml_path=args.src_dataset_yaml,
        dst_root_dir=args.dst_root_dir,
        dst_yaml_path=args.dst_dataset_yaml,
        copy_media_files=args.copy_media,
        hash_threads=args.hash_threads,
        use_process=args.use_process,
        num_workers=args.num_workers
    )