"""
数据集打包压缩脚本 - 高效并发版本

功能描述:
    这个脚本用于将分布在不同目录的数据集统一打包压缩，支持高效的多线程并发处理、
    媒体文件去重、智能分卷压缩。生成的压缩包可以方便地在不同服务器间传输和部署。

主要功能:
    1. 🚀 多线程并发处理文件收集和路径更新 (大幅提升处理速度)
    2. 🔄 智能媒体文件去重 (基于文件内容哈希，避免重复存储)
    3. 📦 智能分卷压缩 (避免单个文件过大)
    4. 💾 流式压缩处理 (节省内存占用)
    5. 📋 自动生成统一的配置文件
    6. 🛡️ 错误处理和进度显示

工作流程:
    阶段1: 收集所有数据项（单线程，快速）
    阶段2: 多线程并行收集所有媒体文件信息
    阶段3: 单线程进行媒体文件去重（避免竞争条件）
    阶段4: 多线程并行更新每个item的媒体文件路径
    阶段5: 流式创建zip文件，支持智能分卷

媒体文件路径结构优化:
    旧格式: {dataset_name}/MediaFiles/{item_id}/{media_type}/{idx}{suffix}
    新格式: {dataset_name}/MediaFiles/{media_type}/{1,2,3...}{suffix}
    
    优势: 相同内容的媒体文件只存储一次，不同样本可以共享同一个媒体文件

输入格式:
    YAML配置文件包含数据集信息:
    ```yaml
    DataDir: /path/to/datasets
    Datasets:
      Dataset1:
        MetaFiles: /path/to/dataset1/MetaFiles
        sample_nums: 1000
      Dataset2:
        MetaFiles: /path/to/dataset2/MetaFiles  
        sample_nums: 2000
    TotalSampleNums: 3000
    ```
使用示例:
    # 基本用法
    python scripts/zip_dataset.py \\
        --datasets_yaml configs/my_dataset.yaml \\
        --save_dir /tmp/exports \\
        --workers 16

使用场景:
    - 数据集跨服务器迁移
    - 数据集版本管理和归档
    - 数据集分发和共享
    - 存储空间优化

性能优化:
    - 多线程处理: 根据硬件配置调整--workers参数
    - 分卷大小: 根据网络和存储环境调整--max_zip_size参数
    - 内存优化: 流式处理，不会一次性加载所有文件到内存

输出文件:
    - 单文件: dataset_name.zip
    - 分卷文件: dataset_name_part1.zip, dataset_name_part2.zip...

注意事项:
    - 确保有足够的磁盘空间存放压缩文件
    - 线程数建议设置为CPU核心数的1-2倍
    - 分卷大小建议根据网络传输需求设置
    - 压缩过程中避免修改源文件
"""
import os
import json
from tqdm import tqdm
import yaml
import zipfile
from glob import glob
from pathlib import Path
import threading
from concurrent.futures import ThreadPoolExecutor
from collections import defaultdict
import io
import hashlib
from typing import Dict, List, Tuple, Any

from datatool.utils.parallel import post_allocated_multithread, post_allocated_multiprocess
from datatool.logger import log
import concurrent.futures
import time


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
def compute_hash_for_file_zip_thread(file_info, **kwargs):
    """并发计算单个文件的哈希值（线程安全）- zip专用"""
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
def compute_hash_for_file_zip_process(file_info, **kwargs):
    """并发计算单个文件的哈希值（进程安全）- zip专用"""
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


def collect_all_data_items(yaml_path):
    """收集所有需要处理的数据项（不去重）"""
    with open(yaml_path, "r") as f:
        dataset_config = yaml.safe_load(f)
    
    all_items = []
    
    # 遍历所有 Datasets
    for dataset_name, config in dataset_config["Datasets"].items():
        metafile_dir = config["MetaFiles"]
        
        # 递归找到所有 .jsonl 文件
        all_metafiles = glob(os.path.join(metafile_dir, "**", "*.jsonl"), recursive=True)
        
        for metafile in all_metafiles:
            with open(metafile, "r") as f:
                lines = f.readlines()
                for line_idx, line in enumerate(lines):
                    if not line.strip():
                        continue
                    try:
                        meta_item = json.loads(line)
                        all_items.append({
                            "dataset_name": dataset_name,
                            "metafile": metafile,
                            "line_idx": line_idx,
                            "meta_item": meta_item
                        })
                    except json.JSONDecodeError as e:
                        log(f"JSON decode error in {metafile}:{line_idx}: {e}")
    
    return all_items, dataset_config


@post_allocated_multithread
def collect_media_files_from_item(item_data: Dict[str, Any], **kwargs) -> List[Dict[str, Any]]:
    """从单个item中收集所有媒体文件信息（多线程安全）"""
    dataset_name = item_data["dataset_name"]
    meta_item = item_data["meta_item"]
    item_id = meta_item["id"]
    
    media_files = []
    
    # 处理 images/videos/audios
    for media_type in ["images", "videos", "audios"]:
        media_paths = meta_item.get(media_type, [])
        if not media_paths:
            continue
        
        for idx, src_path in enumerate(media_paths):
            src_path = Path(src_path)
            if not src_path.exists():
                log(f"⚠️ 文件不存在: {src_path}")
                continue
                
            try:
                file_size = src_path.stat().st_size
                suffix = src_path.suffix if src_path.suffix else ""
                
                media_files.append({
                    "src_path": str(src_path),
                    "media_type": media_type,
                    "suffix": suffix,
                    "size": file_size,
                    "dataset_name": dataset_name,
                    "item_id": item_id,
                    "item_idx": idx  # 在item内的索引
                })
            except Exception as e:
                log(f"⚠️ 获取文件信息失败 {src_path}: {e}")
                continue
        
    return media_files


def deduplicate_media_files_byhash(all_media_files: List[Dict[str, Any]], hash_threads: int = 16, use_process: bool = False) -> Tuple[Dict[str, Dict], Dict[str, str]]:
    """对媒体文件进行去重，返回唯一文件映射和路径映射（优化版：两阶段去重 + 并发哈希）
    
    Returns:
        unique_files: {file_hash: {media_type, src_path, idx, suffix, size}}
        path_mapping: {original_src_path: new_relative_path}
    """
    log("🔍 开始媒体文件去重...")
    
    unique_files = {}  # {file_hash: file_info}
    path_mapping = {}  # {src_path: relative_path}
    media_counters = defaultdict(lambda: defaultdict(int))  # {dataset: {media_type: counter}} - 按数据集分别计数
    size_groups = defaultdict(list)  # {file_size: [media_files]} - 按文件大小分组
    
    # 第一阶段：按文件大小分组，快速过滤
    log("🔍 第一阶段：按文件大小分组...")
    processed_paths = set()  # 用于跟踪已处理的路径
    for media_file in tqdm(all_media_files, desc="分组媒体文件"):
        src_path = media_file["src_path"]
        
        # 如果已经处理过这个路径，跳过
        if src_path in processed_paths:
            continue
        processed_paths.add(src_path)
        
        try:
            file_size = Path(src_path).stat().st_size
            media_file["file_size"] = file_size
            size_groups[file_size].append(media_file)
        except Exception as e:
            log(f"⚠️ 获取文件大小失败 {src_path}: {e}")
            continue
    
    log(f"📊 大小分组统计 - {len(size_groups)} 个不同大小的组")
    
    # 第二阶段：对每个大小组内的文件计算哈希
    log("🔍 第二阶段：计算哈希去重...")
    for file_size, media_files_group in tqdm(size_groups.items(), desc="处理大小组"):
        if len(media_files_group) == 1:
            # 该大小只有一个文件，无需计算哈希，直接处理
            media_file = media_files_group[0]
            src_path = media_file["src_path"]
            
            media_type = media_file["media_type"]
            dataset_name = media_file["dataset_name"]
            media_counters[dataset_name][media_type] += 1
            idx = media_counters[dataset_name][media_type]
            suffix = media_file["suffix"]
            
            # 使用文件大小作为唯一标识（无需计算哈希）
            size_hash = f"size_{file_size}_{src_path}"
            unique_files[size_hash] = {
                "media_type": media_type,
                "src_path": src_path,
                "idx": idx,
                "suffix": suffix,
                "size": media_file["size"],
                "original_dataset": dataset_name
            }
            
            rel_path = f"MediaFiles/{media_type}/{idx}{suffix}"
            path_mapping[src_path] = rel_path
        
        else:
            # 该大小有多个文件，需要计算哈希去重（使用并发）
            process_hash_group_concurrent_zip(
                media_files_group, 
                media_counters, 
                unique_files, 
                path_mapping,
                max_workers=hash_threads,
                use_process=use_process
            )
    
    log(f"📊 媒体文件去重统计:")
    log(f"   原始文件数: {len(all_media_files)}")
    log(f"   去重后文件数: {len(unique_files)}")
    log(f"   节省文件数: {len(all_media_files) - len(unique_files)}")
    
    # 按数据集统计
    for dataset_name, dataset_counters in media_counters.items():
        total_dataset_files = sum(dataset_counters.values())
        log(f"   {dataset_name}: {total_dataset_files} 个唯一文件")
        for media_type, count in dataset_counters.items():
            log(f"     {media_type}: {count} 个")
    
    return unique_files, path_mapping

def deduplicate_media_files_by_path(all_media_files: List[Dict[str, Any]]) -> Tuple[Dict[str, Dict], Dict[str, str]]:
    """
    简化版：仅基于文件路径去重。
    
    Returns:
        unique_files: {src_path: {media_type, src_path, idx, suffix, size, original_dataset}}
        path_mapping: {src_path: relative_path}
    """
    log("🔍 开始基于路径的媒体文件去重...")
    
    unique_files = {}
    path_mapping = {}
    media_counters = defaultdict(lambda: defaultdict(int))
    processed_paths = set()
    
    for media_file in tqdm(all_media_files, desc="检查媒体文件路径"):
        src_path = media_file["src_path"]
        if src_path in processed_paths:
            continue  # 已出现过的路径直接跳过
        
        processed_paths.add(src_path)
        
        media_type = media_file["media_type"]
        dataset_name = media_file["dataset_name"]
        suffix = media_file["suffix"]
        
        media_counters[dataset_name][media_type] += 1
        idx = media_counters[dataset_name][media_type]

        # 生成保存路径
        rel_path = f"MediaFiles/{media_type}/{idx}{suffix}"
        
        unique_files[src_path] = {
            "media_type": media_type,
            "src_path": src_path,
            "idx": idx,
            "suffix": suffix,
            "size": media_file.get("size"),
            "original_dataset": dataset_name,
        }
        path_mapping[src_path] = rel_path
    
    log(f"📊 路径去重统计: 原始文件 {len(all_media_files)} → 唯一文件 {len(unique_files)}")
    return unique_files, path_mapping


def process_hash_group_concurrent_zip(media_files_group, media_counters, unique_files, path_mapping, max_workers=16, use_process=False):
    """并发处理需要计算哈希的文件组（zip专用）"""
    
    # 准备哈希计算任务
    hash_tasks = []
    for media_file in media_files_group:
        src_path = media_file["src_path"]
        if Path(src_path).exists():
            hash_tasks.append(media_file)
    
    if not hash_tasks:
        return
    
    worker_type = "进程" if use_process else "线程"
    log(f"🔧 并发计算 {len(hash_tasks)} 个文件的哈希值 (使用 {min(max_workers, len(hash_tasks))} 个{worker_type})...")
    
    # 选择并发计算方式
    if use_process:
        # CPU密集型：使用多进程
        hash_results = compute_hash_for_file_zip_process(
            hash_tasks,
            num_workers=min(max_workers, len(hash_tasks))
        )
    else:
        # I/O密集型：使用多线程
        hash_results = compute_hash_for_file_zip_thread(
            hash_tasks,
            num_workers=min(max_workers, len(hash_tasks))
        )
    
    # 处理哈希结果，进行去重
    group_hash_map = {}
    for result in hash_results:
        if result["status"] != "success" or not result["file_hash"]:
            continue
        
        file_hash = result["file_hash"]
        media_file = result["media_file"]
        src_path = media_file["src_path"]
        
        if file_hash not in group_hash_map:
            # 新的唯一文件
            media_type = media_file["media_type"]
            dataset_name = media_file["dataset_name"]
            media_counters[dataset_name][media_type] += 1
            idx = media_counters[dataset_name][media_type]
            suffix = media_file["suffix"]
            
            group_hash_map[file_hash] = {
                "media_type": media_type,
                "src_path": src_path,
                "idx": idx,
                "suffix": suffix,
                "size": media_file["size"],
                "original_dataset": dataset_name
            }
            
            unique_files[file_hash] = group_hash_map[file_hash]
            
            rel_path = f"MediaFiles/{media_type}/{idx}{suffix}"
            path_mapping[src_path] = rel_path
        else:
            # 重复文件，使用已有的路径映射
            existing_info = group_hash_map[file_hash]
            rel_path = f"MediaFiles/{existing_info['media_type']}/{existing_info['idx']}{existing_info['suffix']}"
            path_mapping[src_path] = rel_path


@post_allocated_multithread
def update_item_paths(item_data: Dict[str, Any], path_mapping: Dict[str, str], **kwargs) -> Dict[str, Any]:
    """更新单个item中的媒体文件路径（多线程安全）"""
    dataset_name = item_data["dataset_name"]
    meta_item = item_data["meta_item"].copy()
    
    # 更新媒体文件路径
    for media_type in ["images", "videos", "audios"]:
        media_paths = meta_item.get(media_type, [])
        if not media_paths:
            continue
        
        new_paths = []
        for src_path in media_paths:
            # 标准化路径
            src_path = str(Path(src_path))
            if src_path in path_mapping:
                new_paths.append(path_mapping[src_path])
            else:
                log(f"⚠️ 找不到路径映射: {src_path}")
        
        meta_item[media_type] = new_paths
    
    return {
        "dataset_name": dataset_name,
        "metafile": item_data["metafile"],
        "updated_meta_item": meta_item
    }


def create_single_zip_concurrent(zip_info):
    """并发创建单个zip文件（高性能版本）"""
    zip_path, files_to_add, jsonl_content, config_content = zip_info
    
    try:
        start_time = time.time()
        log(f"🔧 开始创建: {Path(zip_path).name} (包含 {len(files_to_add)} 个文件)")
        
        # 确保目标目录存在
        Path(zip_path).parent.mkdir(parents=True, exist_ok=True)
        
        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED, compresslevel=1, allowZip64=True) as zipf:
            # 添加配置文件
            if config_content:
                zipf.writestr("dataset_config.yaml", config_content)
            
            # 添加jsonl文件
            for jsonl_path, content in jsonl_content.items():
                zipf.writestr(jsonl_path, content.encode('utf-8'))
            
            # 批量添加媒体文件（优化I/O）
            for i, file_info in enumerate(files_to_add):
                src_path = file_info["src_path"]
                zip_path_in_archive = file_info["zip_path"]
                
                try:
                    # 直接写入，减少中间缓存
                    zipf.write(src_path, zip_path_in_archive)
                    
                    # 每100个文件显示一次进度
                    if (i + 1) % 100 == 0:
                        log(f"🔧 {Path(zip_path).name}: 已处理 {i+1}/{len(files_to_add)} 个文件")
                except Exception as e:
                    log(f"⚠️ 添加文件失败 {src_path}: {e}")
                    continue
        
        elapsed = time.time() - start_time
        log(f"✅ 完成创建: {Path(zip_path).name} ({elapsed:.2f}s)")
        return {"status": "success", "zip_path": zip_path, "time": elapsed}
        
    except Exception as e:
        log(f"❌ 创建zip失败 {zip_path}: {e}")
        return {"status": "error", "zip_path": zip_path, "error": str(e)}


def allocate_files_to_volumes(unique_file_paths, jsonl_data, config_content, max_zip_size):
    """智能分配文件到不同卷"""
    volumes = []
    current_volume = {
        "files": [],
        "jsonl": jsonl_data,  # 每个卷都包含完整的JSONL数据
        "config": config_content,  # 只有第一个卷包含配置
        "size": 0
    }
    
    # 计算基础开销（JSONL + config）
    base_overhead = sum(len(content.encode('utf-8')) for content in jsonl_data.values())
    if config_content:
        base_overhead += len(config_content.encode('utf-8'))
    
    current_volume["size"] = base_overhead
    
    # 按文件大小排序，大文件优先
    sorted_files = sorted(unique_file_paths, key=lambda x: x["size"], reverse=True)
    
    for file_info in sorted_files:
        file_size = file_info["size"]
        
        # 检查是否需要新卷
        if current_volume["size"] + file_size > max_zip_size and current_volume["files"]:
            # 当前卷已满，创建新卷
            volumes.append(current_volume)
            current_volume = {
                "files": [],
                "jsonl": jsonl_data,
                "config": None,  # 只有第一个卷有配置
                "size": base_overhead
            }
        
        current_volume["files"].append(file_info)
        current_volume["size"] += file_size
    
    # 添加最后一个卷
    if current_volume["files"]:
        volumes.append(current_volume)
    
    return volumes


def create_zip_files_streaming_optimized(updated_results, unique_files, output_path, max_zip_size_mb=2048, num_workers=4):
    """优化的流式创建zip文件，支持分卷和并发压缩"""
    max_zip_size = max_zip_size_mb * 1024 * 1024  # 转换为字节
    
    # 重新组织数据，按原始jsonl文件分组
    file_results = defaultdict(list)  # {(dataset_name, metafile_path): [items]}
    dataset_results = defaultdict(list)  # {dataset_name: [items]} - 保持兼容性
    
    for result in updated_results:
        dataset_name = result["dataset_name"]
        metafile_path = result["metafile"]
        updated_item = result["updated_meta_item"]
        
        # 按原始文件分组
        file_key = (dataset_name, metafile_path)
        file_results[file_key].append(updated_item)
        
        # 按数据集分组（用于配置文件）
        dataset_results[dataset_name].append(updated_item)
    
    # 创建dataset_config.yaml内容
    updated_datasets = {}
    for dataset_name, items in dataset_results.items():
        updated_datasets[dataset_name] = {
            "MetaFiles": f"{dataset_name}/MetaFiles",
            "sample_nums": len(items)
        }
    
    updated_config = {
        "DataDir": None,
        "Datasets": updated_datasets,
        "TotalSampleNums": sum(len(items) for items in dataset_results.values())
    }
    
    # 生成唯一文件路径列表（简化版）
    unique_file_paths = []
    dataset_file_counters = defaultdict(lambda: defaultdict(int))  # {dataset: {media_type: count}}
    
    # 直接从unique_files生成文件路径列表
    for file_hash, info in unique_files.items():
        src_path = info['src_path']
        media_type = info['media_type']
        suffix = info['suffix']
        original_dataset = info['original_dataset']
        
        # 为原始数据集生成文件路径
        dataset_file_counters[original_dataset][media_type] += 1
        idx = dataset_file_counters[original_dataset][media_type]
        
        zip_path = f"{original_dataset}/MediaFiles/{media_type}/{idx}{suffix}"
        unique_file_paths.append({
            "src_path": src_path,
            "zip_path": zip_path,
            "size": info['size'],
            "file_hash": file_hash,
            "dataset": original_dataset
        })
    
    # 计算总大小
    total_size = sum(fp["size"] for fp in unique_file_paths)
    # 加上配置文件大小（估算）
    config_yaml = yaml.dump(updated_config, default_flow_style=False, allow_unicode=True)
    total_size += len(config_yaml.encode('utf-8'))
    
    # 加上jsonl文件大小（估算）
    for dataset_name, items in dataset_results.items():
        jsonl_size = sum(len(json.dumps(item, ensure_ascii=False)) + 1 for item in items)
        total_size += jsonl_size
    
    total_size_mb = total_size / (1024 * 1024)
    log(f"📊 总数据大小: {total_size_mb:.1f} MB (去重后)")
    
    # 按文件大小排序，大文件优先（更好的分卷平衡）
    unique_file_paths.sort(key=lambda x: x["size"], reverse=True)
    
    if total_size <= max_zip_size:
        # 单个zip文件 - 使用优化的并发压缩逻辑
        zip_path = f"{output_path}.zip"
        log(f"📦 创建单个zip文件: {zip_path}")
        
        # 准备JSONL数据（与分卷版本相同的逻辑）
        jsonl_data = {}
        for (dataset_name, metafile_path), items in file_results.items():
            original_filename = Path(metafile_path).name
            jsonl_path = f"{dataset_name}/MetaFiles/{original_filename}"
            
            jsonl_content = ""
            for item in items:
                jsonl_content += json.dumps(item, ensure_ascii=False) + "\n"
            
            jsonl_data[jsonl_path] = jsonl_content
        
        # 使用统一的并发压缩函数
        zip_info = (zip_path, unique_file_paths, jsonl_data, config_yaml)
        result = create_single_zip_concurrent(zip_info)
        
        if result["status"] == "success":
            return [zip_path]
        else:
            log(f"❌ 压缩失败: {result.get('error', 'Unknown error')}")
            return []
    
    else:
        # 分卷压缩 - 智能分配 + 并发压缩
        log(f"📦 数据过大，开始智能分卷压缩...")
        
        # 准备JSONL数据（与单文件版本相同的逻辑）
        jsonl_data = {}
        for (dataset_name, metafile_path), items in file_results.items():
            original_filename = Path(metafile_path).name
            jsonl_path = f"{dataset_name}/MetaFiles/{original_filename}"
            
            jsonl_content = ""
            for item in items:
                jsonl_content += json.dumps(item, ensure_ascii=False) + "\n"
            
            jsonl_data[jsonl_path] = jsonl_content
        
        volumes = allocate_files_to_volumes(unique_file_paths, jsonl_data, config_yaml, max_zip_size)
        log(f"📦 智能分配为 {len(volumes)} 个分卷")
        
        # 准备并发压缩任务
        zip_tasks = []
        for vol_idx, volume in enumerate(volumes):
            zip_path = f"{output_path}_part{vol_idx + 1}.zip"
            zip_info = (zip_path, volume["files"], volume["jsonl"], volume["config"])
            zip_tasks.append(zip_info)
        
        # 并发压缩所有分卷
        log(f"🚀 并发压缩 {len(zip_tasks)} 个分卷 (使用 {min(num_workers, len(zip_tasks))} 个线程)...")
        
        zip_paths = []
        total_time = 0
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=min(num_workers, len(zip_tasks))) as executor:
            # 提交所有压缩任务
            future_to_path = {
                executor.submit(create_single_zip_concurrent, zip_info): zip_info[0] 
                for zip_info in zip_tasks
            }
            
            # 收集结果
            for future in concurrent.futures.as_completed(future_to_path):
                zip_path = future_to_path[future]
                try:
                    result = future.result()
                    if result["status"] == "success":
                        zip_paths.append(result["zip_path"])
                        total_time += result["time"]
                    else:
                        log(f"❌ 分卷压缩失败 {zip_path}: {result.get('error', 'Unknown error')}")
                except Exception as e:
                    log(f"❌ 压缩任务异常 {zip_path}: {e}")
            
        # 按序排列结果
        zip_paths.sort()
        
        log(f"✅ 所有分卷压缩完成 (总耗时: {total_time:.2f}s)")
        return zip_paths


def process_datasets(yaml_path, save_dir, max_zip_size_mb=2048, num_workers=16, use_process=False):
    """主处理函数 - 分阶段并发安全处理"""
    yaml_path = Path(yaml_path)
    save_dir = Path(save_dir)
    save_dir.mkdir(parents=True, exist_ok=True)
    
    # 从yaml文件名推断输出文件名
    output_name = yaml_path.stem
    output_path = save_dir / output_name
    
    # 阶段1: 收集所有数据项
    log("📋 阶段1: 收集数据项...")
    all_items, dataset_config = collect_all_data_items(yaml_path)
    log(f"📋 共找到 {len(all_items)} 个数据项")
    
    if not all_items:
        log("❌ 没有找到任何数据项")
        return
    
    # 阶段2: 并行收集所有媒体文件信息（多线程）
    log(f"🚀 阶段2: 并行收集媒体文件信息 (使用 {num_workers} 个线程)...")
    all_media_files_nested = collect_media_files_from_item(all_items, num_workers=num_workers)
    
    # 展平嵌套列表
    all_media_files = []
    for media_list in all_media_files_nested:
        if isinstance(media_list, list):
            all_media_files.extend(media_list)
        else:
            all_media_files.append(media_list)
    
    log(f"📊 收集到 {len(all_media_files)} 个媒体文件")
        
    # 阶段3: 并发进行媒体文件去重
    if args.need_deduplicate:    
        worker_type = "多进程" if use_process else "多线程"
        log(f"🔄 阶段3: 媒体文件去重（{worker_type}）...")
        unique_files, path_mapping = deduplicate_media_files_by_path(all_media_files)
    
        #阶段4：并行更新每个item的路径（多线程）
        log(f"📝 阶段4: 并行更新item路径 (使用 {num_workers} 个线程)...")
        updated_results = update_item_paths(all_items, path_mapping=path_mapping, num_workers=num_workers)
    else:
        updated_results = all_items
        unique_files = all_media_files
    
    # 阶段5: 流式创建zip文件
    log("📦 阶段5: 开始流式压缩...")
    zip_files = create_zip_files_streaming_optimized(updated_results, unique_files, str(output_path), max_zip_size_mb, num_workers)
    
    log(f"🎉 完成！共处理 {len(updated_results)} 个数据项")
    log(f"📁 输出文件: {', '.join(zip_files)}")


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="处理数据集并直接压缩（支持分卷）")
    parser.add_argument("--datasets_yaml",default="/home/gary/AI-Project/yjy/test/datatooltest/test.yaml", help="输入的yaml配置文件路径")
    parser.add_argument("--save_dir",default="/home/gary/AI-Project/yjy/test/datatooltest/zip/", help="输出目录")
    parser.add_argument('--max_zip_size', type=int, default=1024, help="单个zip文件最大大小(MB)，超过会自动分卷")
    parser.add_argument('--num_workers', type=int, default=16, help="并发线程数")
    parser.add_argument('--use_process', action='store_true', help="使用多进程进行哈希计算（CPU密集型优化）")
    parser.add_argument('--need_deduplicate', type=bool, default= True, help="是否进行重复media文件去重")
    
    args = parser.parse_args()
    process_datasets(args.datasets_yaml, args.save_dir, args.max_zip_size, args.num_workers, args.use_process)
