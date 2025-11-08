#!/usr/bin/env python3
"""
数据集解压和路径重构脚本 - 高效优化版本

功能描述:
    这个脚本用于解压由 zip_dataset.py 生成的数据集压缩包，并将其中的相对路径
    转换为目标服务器上的绝对路径。支持单个zip文件和分卷zip文件的解压。

主要功能:
    1. 🚀 多进程并发解压zip文件 (充分利用多CPU，大幅提升解压速度)
    2. 📦 智能识别和处理zip文件 (自动匹配*.zip, *_part*.zip等模式)
    3. 🔄 高效批量更新jsonl文件中的媒体文件路径 (images/videos/audios)
    4. 📋 更新yaml配置文件中的数据集路径
    5. 🛡️ 完善的错误处理和进度显示

使用场景:
    - 数据集在不同服务器间迁移
    - 从压缩包恢复数据集到新的存储位置
    - 批量更新数据集路径配置

目录结构:
    解压前的zip内容:
    ├── dataset_config.yaml
    ├── Dataset1/
    │   ├── MetaFiles/Dataset1.jsonl
    │   └── MediaFiles/item1/images/0.jpg
    
    解压后更新路径:
    ├── dataset_config.yaml (DataDir更新为new_base_path)
    └── Dataset1/
        ├── MetaFiles/Dataset1.jsonl (路径更新为绝对路径)
        └── MediaFiles/... (文件位置不变)


注意事项:
    - extract_dir 和 base_path 通常设置为相同值
    - 确保目标目录有足够的磁盘空间
    - 多进程数量建议设置为CPU核心数的1-2倍
"""

import os
import json
import yaml
import argparse
import zipfile
from pathlib import Path
from glob import glob
from tqdm import tqdm
import multiprocessing
from concurrent.futures import ProcessPoolExecutor, as_completed
import time
from typing import List, Dict, Any, Tuple

from datatool.utils.parallel import post_allocated_multiprocess
from datatool.logger import log


@post_allocated_multiprocess
def extract_zip_parallel(zip_file_info: Tuple[str, str], **kwargs) -> Dict[str, Any]:
    """使用post_allocated_multiprocess并行解压单个zip文件"""
    return extract_single_zip(zip_file_info, **kwargs)


def extract_single_zip(zip_file_info: Tuple[str, str], **kwargs) -> Dict[str, Any]:
    """解压单个zip文件（多进程安全）"""
    zip_file, extract_dir = zip_file_info
    process_id = kwargs.get("process_id", "unknown")
    
    try:
        if not Path(zip_file).exists():
            return {"status": "error", "file": zip_file, "message": "文件不存在"}
        
        log(f"🔧 进程 {process_id} 开始解压: {Path(zip_file).name}")
        start_time = time.time()
        
        with zipfile.ZipFile(zip_file, 'r') as zipf:
            # 获取文件信息用于进度显示
            file_count = len(zipf.namelist())
            
            # 安全解压所有文件（处理并发目录创建冲突）
            try:
                zipf.extractall(extract_dir)
            except FileExistsError as e:
                # 处理并发目录创建冲突，逐个文件解压
                log(f"🔄 进程 {process_id} 检测到目录冲突，切换到安全模式解压")
                for member in zipf.namelist():
                    try:
                        zipf.extract(member, extract_dir)
                    except FileExistsError:
                        # 如果是目录冲突，忽略；如果是文件冲突，跳过
                        if not member.endswith('/'):
                            # 检查文件是否已存在且内容相同
                            target_file = Path(extract_dir) / member
                            if target_file.exists():
                                continue  # 文件已存在，跳过
                        continue
        
        duration = time.time() - start_time
        
        return {
            "status": "success", 
            "file": zip_file, 
            "file_count": file_count,
            "duration": duration,
            "size_mb": Path(zip_file).stat().st_size / (1024 * 1024),
            "process_id": process_id
        }
        
    except Exception as e:
        return {"status": "error", "file": zip_file, "message": str(e), "process_id": process_id}


def extract_zip_files(zip_pattern: str, zip_source_dir: str, extract_dir: str, num_workers: int = 8) -> Path:
    """智能多进程解压所有zip文件"""
    zip_source_dir = Path(zip_source_dir)
    extract_dir = Path(extract_dir)
    extract_dir.mkdir(parents=True, exist_ok=True)
    
    # 智能查找zip文件
    def find_zip_files():
        if "*" in zip_pattern:
            # 通配符模式
            if not os.path.isabs(zip_pattern):
                search_pattern = zip_source_dir / zip_pattern
            else:
                search_pattern = zip_pattern
            files = glob(str(search_pattern))
        else:
            # 直接指定文件
            if not os.path.isabs(zip_pattern):
                zip_file = zip_source_dir / zip_pattern
            else:
                zip_file = Path(zip_pattern)
            files = [str(zip_file)] if zip_file.exists() else []
        
        # 如果没找到文件，尝试常见的模式
        if not files and "*" not in zip_pattern:
            log(f"💡 未找到 {zip_pattern}，尝试常见模式...")
            common_patterns = [
                f"*{zip_pattern}*",
                f"{zip_pattern}_part*.zip",
                f"{Path(zip_pattern).stem}_part*.zip"
            ]
            
            for pattern in common_patterns:
                search_path = zip_source_dir / pattern
                files = glob(str(search_path))
                if files:
                    log(f"💡 自动匹配到模式: {pattern}")
                    break
        
        return sorted(files)  # 确保顺序处理
    
    log(f"🔍 在目录 {zip_source_dir} 中搜索: {zip_pattern}")
    zip_files = find_zip_files()
    
    if not zip_files:
        log(f"❌ 未找到匹配的zip文件: {zip_pattern}")
        log(f"💡 请检查:")
        log(f"  1. zip文件是否在 {zip_source_dir} 目录中")
        log(f"  2. 文件名模式是否正确: {zip_pattern}")
        # 列出目录中的zip文件
        existing_zips = list(zip_source_dir.glob("*.zip"))
        if existing_zips:
            log(f"  3. 目录中现有的zip文件:")
            for zf in existing_zips:
                log(f"     - {zf.name}")
        return extract_dir
    
    log(f"📦 找到 {len(zip_files)} 个zip文件，将解压到: {extract_dir}")
    for i, zf in enumerate(zip_files, 1):
        size_mb = Path(zf).stat().st_size / (1024 * 1024) if Path(zf).exists() else 0
        log(f"  {i}. {Path(zf).name} ({size_mb:.1f} MB)")
    
    # 智能调整进程数：不超过zip文件数量
    actual_workers = min(num_workers, len(zip_files))
    if actual_workers != num_workers:
        log(f"💡 调整进程数从 {num_workers} 到 {actual_workers} (匹配zip文件数量)")
    
    # 检查是否为分卷文件（通过文件名模式判断）
    is_volume_set = any("part" in Path(zf).name.lower() for zf in zip_files)
    if is_volume_set and len(zip_files) > 1:
        log(f"🔍 检测到分卷文件，使用优化的解压策略")
    
    # 准备多进程参数 - 解压到extract_dir
    extract_tasks = [(zip_file, extract_dir) for zip_file in zip_files]
    
    # 使用post_allocated_multiprocess进行多进程解压
    log(f"🚀 开始多进程解压 (使用 {actual_workers} 个进程)...")
    start_time = time.time()
    
    # 使用post_allocated_multiprocess批量处理
    results = extract_zip_parallel(extract_tasks, num_workers=actual_workers)
    
    # 统计结果
    total_files = 0
    total_size_mb = 0
    successful_extracts = 0
    
    for result in results:
        if result["status"] == "success":
            file_name = Path(result["file"]).name
            duration = result["duration"]
            size_mb = result["size_mb"]
            file_count = result["file_count"]
            process_id = result.get("process_id", "unknown")
            
            log(f"✅ 进程{process_id} - {file_name}: {file_count} 个文件, {size_mb:.1f}MB, {duration:.1f}秒")
            
            total_files += file_count
            total_size_mb += size_mb
            successful_extracts += 1
        else:
            process_id = result.get("process_id", "unknown")
            log(f"❌ 进程{process_id} - {Path(result['file']).name}: {result['message']}")
    
    total_duration = time.time() - start_time
    
    log(f"🎉 解压完成!")
    log(f"  ✅ 成功: {successful_extracts}/{len(zip_files)} 个文件")
    log(f"  📁 总文件数: {total_files}")
    log(f"  💾 总大小: {total_size_mb:.1f} MB")
    log(f"  ⏱️  总耗时: {total_duration:.1f} 秒")
    log(f"  🚀 平均速度: {total_size_mb/total_duration:.1f} MB/s")
    log(f"  📂 从: {zip_source_dir}")
    log(f"  📂 到: {extract_dir}")
    
    # 添加解压完整性验证
    if is_volume_set and successful_extracts == len(zip_files):
        log(f"🔍 验证分卷解压完整性...")
        verification_result = verify_extraction_completeness(zip_files, extract_dir)
        if verification_result["success"]:
            log(f"✅ 解压完整性验证通过: {verification_result['total_files']} 个文件")
        else:
            log(f"⚠️ 解压完整性验证失败: {verification_result['message']}")
    
    return extract_dir


def verify_extraction_completeness(zip_files: List[str], extract_dir: Path) -> Dict[str, Any]:
    """验证分卷解压的完整性"""
    try:
        # 统计zip文件中的总文件数
        expected_files = set()
        total_expected = 0
        
        for zip_file in zip_files:
            with zipfile.ZipFile(zip_file, 'r') as zipf:
                for name in zipf.namelist():
                    if not name.endswith('/'):  # 排除目录
                        expected_files.add(name)
                        total_expected += 1
        
        # 统计实际解压的文件数
        actual_files = set()
        for root, dirs, files in os.walk(extract_dir):
            for file in files:
                rel_path = os.path.relpath(os.path.join(root, file), extract_dir)
                actual_files.add(rel_path)
        
        missing_files = expected_files - actual_files
        extra_files = actual_files - expected_files
        
        if missing_files:
            return {
                "success": False,
                "message": f"缺少 {len(missing_files)} 个文件",
                "missing_files": list(missing_files)[:10],  # 只显示前10个
                "total_files": len(actual_files)
            }
        
        return {
            "success": True,
            "total_files": len(actual_files),
            "expected_files": len(expected_files)
        }
        
    except Exception as e:
        return {
            "success": False,
            "message": f"验证过程出错: {str(e)}",
            "total_files": 0
        }


@post_allocated_multiprocess
def process_jsonl_file(file_info, **kwargs):
    """并发处理单个jsonl文件中的路径更新"""
    jsonl_file, dataset_dir, new_base_path = file_info
    
    try:
        updated_items = []
        
        with open(jsonl_file, "r") as f:
            lines = f.readlines()
        
        for line in lines:
            if not line.strip():
                continue
            
            try:
                item = json.loads(line)
                
                # 更新媒体文件路径
                for media_type in ["images", "videos", "audios"]:
                    if media_type in item and item[media_type]:
                        updated_paths = []
                        for rel_path in item[media_type]:
                            # 从相对路径构建绝对路径
                            abs_path = new_base_path / dataset_dir.name / rel_path
                            updated_paths.append(str(abs_path))
                        item[media_type] = updated_paths
                
                updated_items.append(item)
                
            except json.JSONDecodeError as e:
                log(f"JSON decode error in {jsonl_file}: {e}")
                continue
        
        # 写回文件
        with open(jsonl_file, "w") as f:
            for item in updated_items:
                f.write(json.dumps(item, ensure_ascii=False) + "\n")
        
        return {
            "file": str(jsonl_file),
            "items_count": len(updated_items),
            "status": "success"
        }
        
    except Exception as e:
        return {
            "file": str(jsonl_file),
            "status": "error", 
            "message": str(e)
        }


def update_yaml_config(yaml_file, new_base_path):
    """更新yaml配置文件中的路径"""
    with open(yaml_file, "r") as f:
        config = yaml.safe_load(f)
    
    # 更新 DataDir
    config["DataDir"] = str(new_base_path)
    
    # 更新每个数据集的MetaFiles路径
    updated_datasets = {}
    for dataset_name, dataset_config in config["Datasets"].items():
        metafiles_rel_path = dataset_config["MetaFiles"]
        metafiles_abs_path = new_base_path / metafiles_rel_path
        
        updated_datasets[dataset_name] = {
            "MetaFiles": str(metafiles_abs_path),
            "sample_nums": dataset_config["sample_nums"]
        }
    
    config["Datasets"] = updated_datasets
    
    # 写回文件
    with open(yaml_file, "w") as f:
        yaml.dump(config, f, default_flow_style=False, allow_unicode=True)
    
    log(f"✅ 更新配置文件: {yaml_file}")
    return config


def unzip_update_dataset_paths(zip_pattern, zip_source_dir, new_base_path, num_workers=8):
    """解压zip文件并更新所有路径为绝对路径"""
    zip_source_dir = Path(zip_source_dir)
    new_base_path = Path(new_base_path)
    
    log(f"🔧 配置信息:")
    log(f"  ZIP模式: {zip_pattern}")
    log(f"  ZIP源目录: {zip_source_dir}")
    log(f"  解压到目录: {new_base_path}")
    log(f"  进程数量: {num_workers}")
    
    # 多进程解压zip文件 - 从zip_source_dir解压到new_base_path
    dataset_dir = extract_zip_files(zip_pattern, zip_source_dir, new_base_path, num_workers)
    
    # 查找yaml配置文件 - 在解压后的目录中查找
    yaml_files = list(dataset_dir.glob("*.yaml")) + list(dataset_dir.glob("*.yml"))
    if not yaml_files:
        log("❌ 未找到yaml配置文件")
        return
    
    yaml_file = yaml_files[0]
    log(f"📋 处理配置文件: {yaml_file}")
    
    # 更新yaml配置 - 路径指向new_base_path
    config = update_yaml_config(yaml_file, new_base_path)
    
    # 收集所有需要处理的jsonl文件
    jsonl_tasks = []
    for dataset_name in config["Datasets"].keys():
        dataset_subdir = dataset_dir / dataset_name
        if not dataset_subdir.exists():
            log(f"⚠️ 数据集子目录不存在: {dataset_subdir}")
            continue
        
        # 查找MetaFiles目录中的jsonl文件
        metafiles_dir = dataset_subdir / "MetaFiles"
        if not metafiles_dir.exists():
            log(f"⚠️ MetaFiles目录不存在: {metafiles_dir}")
            continue
        
        jsonl_files = list(metafiles_dir.glob("*.jsonl"))
        for jsonl_file in jsonl_files:
            jsonl_tasks.append((jsonl_file, dataset_subdir, new_base_path))
    
    if not jsonl_tasks:
        log("⚠️ 未找到需要处理的jsonl文件")
        return
    
    log(f"📝 找到 {len(jsonl_tasks)} 个jsonl文件需要更新路径")
    
    # 多进程更新jsonl文件路径
    log(f"🚀 开始多进程更新路径 (使用 {min(num_workers, len(jsonl_tasks))} 个进程)...")
    
    results = process_jsonl_file(
        jsonl_tasks, 
        num_workers=min(num_workers, len(jsonl_tasks))
    )
    
    # 统计结果
    total_items = 0
    success_count = 0
    
    for result in results:
        if result["status"] == "success":
            items_count = result["items_count"]
            total_items += items_count
            success_count += 1
            log(f"  ✅ {Path(result['file']).name}: {items_count} 个数据项")
        else:
            log(f"  ❌ {Path(result['file']).name}: {result['message']}")
    
    log(f"🎉 完成!")
    log(f"  📦 解压位置: {new_base_path}")
    log(f"  ✅ 成功文件: {success_count}/{len(jsonl_tasks)}")
    log(f"  📊 总数据项: {total_items}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="解压数据集zip文件并更新路径为绝对路径 (支持多进程)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
使用示例:
  # 解压目录中的所有zip文件（默认模式）
  python scripts/unzip_repath_dataset.py \\
      --zip_source_dir "/home/data/zips" \\
      --target_dir "/data/extracted" \\
      --workers 8

  # 解压特定分卷zip文件
  python scripts/unzip_repath_dataset.py \\
      --zip_pattern "dataset_part*.zip" \\
      --zip_source_dir "/home/data/zips" \\
      --target_dir "/data/extracted" \\
      --workers 12
        """
    )
    
    parser.add_argument("--zip_pattern", default="*.zip",
                       help="zip文件名模式 (默认: *.zip，支持通配符)")
    parser.add_argument("--zip_source_dir", required=True, help="zip文件所在目录")
    parser.add_argument("--target_dir", required=True, help="解压目标目录 (数据最终存放位置)")
    parser.add_argument("--workers", type=int, default=8, help="并发进程数 (默认: 8，会自动调整为zip文件数量)")
    
    args = parser.parse_args()
    
    unzip_update_dataset_paths(args.zip_pattern, args.zip_source_dir, args.target_dir, args.workers)
