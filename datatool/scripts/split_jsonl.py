"""
JSONL文件分割脚本 - 多线程并发版本

功能描述:
    这个脚本用于将大型JSONL文件按指定的样本数量上限进行分割，支持多线程并发处理。
    分割完成后会自动删除原始文件，确保数据目录的整洁。

主要功能:
    1. 🚀 多线程并发处理多个JSONL文件
    2. ✂️ 按用户指定样本上限分割文件
    3. 🗑️ 自动删除原始文件
    4. 📋 智能命名新分割的文件
    5. 🛡️ 错误处理和进度显示

工作流程:
    1. 扫描数据集配置，找到所有MetaFiles目录
    2. 多线程并发处理每个JSONL文件
    3. 按样本上限分割文件
    4. 安全删除原始文件
    5. 生成处理报告

使用示例:
    # 分割所有JSONL文件，每个文件最多1000个样本
    python scripts/split_jsonl.py \\
        --datasets_yaml configs/my_dataset.yaml \\
        --max_samples 1000 \\
        --workers 8

    # 指定输出文件名前缀
    python scripts/split_jsonl.py \\
        --datasets_yaml configs/my_dataset.yaml \\
        --max_samples \\
        --output_prefix "train_split" \\
        --workers 16

注意事项:
    - 确保有足够的磁盘空间存放分割后的文件
    - 原始文件会被删除，请提前备份重要数据
    - 线程数建议设置为CPU核心数的1-2倍
"""

import os
import json
import yaml
from pathlib import Path
from typing import Dict, List, Any, Tuple
from collections import defaultdict
from tqdm import tqdm

from datatool.utils.parallel import post_allocated_multithread
from datatool.logger import log


@post_allocated_multithread
def split_single_jsonl_file(file_info: Dict[str, Any], **kwargs) -> Dict[str, Any]:
    """分割单个JSONL文件（多线程安全）"""
    jsonl_file = Path(file_info["jsonl_file"])
    max_samples = file_info["max_samples_per_file"]
    output_prefix = file_info.get("output_prefix", "part")
    thread_id = kwargs.get("thread_id", "unknown")
    
    try:
        log(f"🔧 线程 {thread_id} 开始处理: {jsonl_file.name}")
        
        # 读取所有样本
        all_samples = []
        with open(jsonl_file, 'r', encoding='utf-8') as f:
            for line_idx, line in enumerate(f):
                line = line.strip()
                if not line:
                    continue
                try:
                    sample = json.loads(line)
                    all_samples.append(sample)
                except json.JSONDecodeError as e:
                    log(f"⚠️ JSON解析错误 {jsonl_file}:{line_idx}: {e}")
                    continue
        
        if not all_samples:
            log(f"⚠️ 文件为空，跳过: {jsonl_file}")
            return {
                "status": "skipped",
                "file": str(jsonl_file),
                "reason": "empty_file",
                "thread_id": thread_id
            }
        
        total_samples = len(all_samples)
        
        # 如果样本数不超过限制，无需分割
        if total_samples <= max_samples:
            log(f"📋 线程 {thread_id} - {jsonl_file.name}: {total_samples} 样本，无需分割")
            return {
                "status": "no_split_needed",
                "file": str(jsonl_file),
                "total_samples": total_samples,
                "thread_id": thread_id
            }
        
        # 计算需要分割的文件数
        num_parts = (total_samples + max_samples - 1) // max_samples
        
        # 生成分割后的文件
        output_files = []
        output_dir = jsonl_file.parent
        base_name = jsonl_file.stem
        
        for part_idx in range(num_parts):
            start_idx = part_idx * max_samples
            end_idx = min((part_idx + 1) * max_samples, total_samples)
            part_samples = all_samples[start_idx:end_idx]
            
            # 生成新文件名
            if output_prefix:
                new_filename = f"{output_prefix}_{base_name}_part{part_idx + 1}.jsonl"
            else:
                new_filename = f"{base_name}_part{part_idx + 1}.jsonl"
            
            new_file_path = output_dir / new_filename
            
            # 写入分割后的文件
            with open(new_file_path, 'w', encoding='utf-8') as f:
                for sample in part_samples:
                    f.write(json.dumps(sample, ensure_ascii=False) + '\n')
            
            output_files.append({
                "file": str(new_file_path),
                "samples": len(part_samples)
            })
        
        log(f"✅ 线程 {thread_id} - {jsonl_file.name}: 分割为 {num_parts} 个文件")
        
        return {
            "status": "success",
            "original_file": str(jsonl_file),
            "total_samples": total_samples,
            "output_files": output_files,
            "num_parts": num_parts,
            "thread_id": thread_id
        }
        
    except Exception as e:
        return {
            "status": "error",
            "file": str(jsonl_file),
            "message": str(e),
            "thread_id": thread_id
        }


def collect_jsonl_files(yaml_path: str) -> List[Dict[str, Any]]:
    """收集所有需要处理的JSONL文件"""
    with open(yaml_path, "r") as f:
        dataset_config = yaml.safe_load(f)
    
    jsonl_files = []
    
    # 遍历所有数据集
    for dataset_name, config in dataset_config["Datasets"].items():
        metafile_dir = Path(config["MetaFiles"])
        
        if not metafile_dir.exists():
            log(f"⚠️ MetaFiles目录不存在: {metafile_dir}")
            continue
        
        # 查找所有JSONL文件
        dataset_jsonl_files = list(metafile_dir.glob("*.jsonl"))
        
        for jsonl_file in dataset_jsonl_files:
            jsonl_files.append({
                "dataset_name": dataset_name,
                "jsonl_file": str(jsonl_file),
                "metafile_dir": str(metafile_dir)
            })
    
    return jsonl_files


def safe_delete_files(files_to_delete: List[str], num_workers: int = 4) -> Dict[str, Any]:
    """安全删除文件列表"""
    deleted_count = 0
    failed_deletes = []
    
    log(f"🗑️ 开始删除 {len(files_to_delete)} 个原始文件...")
    
    for file_path in tqdm(files_to_delete, desc="删除原始文件"):
        try:
            if Path(file_path).exists():
                Path(file_path).unlink()
                deleted_count += 1
            else:
                log(f"⚠️ 文件不存在，跳过删除: {file_path}")
        except Exception as e:
            log(f"❌ 删除文件失败 {file_path}: {e}")
            failed_deletes.append(file_path)
    
    return {
        "deleted_count": deleted_count,
        "failed_deletes": failed_deletes
    }


def split_jsonl_files(yaml_path: str, max_samples_per_file: int, output_prefix: str = None, num_workers: int = 8):
    """主函数：分割JSONL文件"""
    yaml_path = Path(yaml_path)
    
    if not yaml_path.exists():
        log(f"❌ YAML配置文件不存在: {yaml_path}")
        return
    
    log(f"🔧 配置信息:")
    log(f"  YAML配置: {yaml_path}")
    log(f"  每文件最大样本数: {max_samples_per_file}")
    log(f"  输出前缀: {output_prefix or '(无)'}")
    log(f"  线程数: {num_workers}")
    
    # 收集所有JSONL文件
    log("📋 收集JSONL文件...")
    all_jsonl_files = collect_jsonl_files(str(yaml_path))
    
    if not all_jsonl_files:
        log("❌ 未找到任何JSONL文件")
        return
    
    log(f"📊 找到 {len(all_jsonl_files)} 个JSONL文件需要处理")
    
    # 准备处理任务
    file_tasks = []
    for file_info in all_jsonl_files:
        file_tasks.append({
            "jsonl_file": file_info["jsonl_file"],
            "max_samples_per_file": max_samples_per_file,
            "output_prefix": output_prefix,
            "dataset_name": file_info["dataset_name"]
        })
    
    # 多线程并发处理
    log(f"🚀 开始多线程分割 (使用 {num_workers} 个线程)...")
    results = split_single_jsonl_file(file_tasks, num_workers=num_workers)
    
    # 统计结果
    success_count = 0
    no_split_count = 0
    error_count = 0
    skipped_count = 0
    files_to_delete = []
    total_new_files = 0
    total_samples = 0
    
    for result in results:
        status = result["status"]
        if status == "success":
            success_count += 1
            files_to_delete.append(result["original_file"])
            total_new_files += result["num_parts"]
            total_samples += result["total_samples"]
        elif status == "no_split_needed":
            no_split_count += 1
            total_samples += result["total_samples"]
        elif status == "error":
            error_count += 1
            log(f"❌ 处理失败: {result['file']} - {result['message']}")
        elif status == "skipped":
            skipped_count += 1
    
    # 删除原始文件
    if files_to_delete:
        delete_result = safe_delete_files(files_to_delete, num_workers)
        log(f"🗑️ 删除结果: 成功 {delete_result['deleted_count']} 个，失败 {len(delete_result['failed_deletes'])} 个")
    
    # 输出最终统计
    log(f"🎉 处理完成!")
    log(f"  ✅ 成功分割: {success_count} 个文件")
    log(f"  📋 无需分割: {no_split_count} 个文件")
    log(f"  ⏭️ 跳过处理: {skipped_count} 个文件")
    log(f"  ❌ 处理失败: {error_count} 个文件")
    log(f"  📁 新增文件: {total_new_files} 个")
    log(f"  📊 总样本数: {total_samples}")


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="JSONL文件分割工具 - 多线程并发版本")
    parser.add_argument("--datasets_yaml", required=True, help="数据集YAML配置文件路径")
    parser.add_argument("--max_samples", type=int, default=1000, help="每个JSONL文件的最大样本数")
    parser.add_argument("--output_prefix", default=None, help="输出文件名前缀（可选）")
    parser.add_argument("--workers", type=int, default=8, help="并发线程数 (默认: 8)")
    
    args = parser.parse_args()
    
    split_jsonl_files(
        yaml_path=args.datasets_yaml,
        max_samples_per_file=args.max_samples,
        output_prefix=args.output_prefix,
        num_workers=args.workers
    )