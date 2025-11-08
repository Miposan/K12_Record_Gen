import os
import re
import sys
import yaml
import pickle as pkl
import random

from glob import glob
from copy import deepcopy
from omegaconf import OmegaConf
from typing import List, Literal, Tuple, Union
import base64
from PIL import Image
from io import BytesIO
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from tqdm import tqdm
import time

from datatool.logger import log
from datatool.utils.parallel import post_allocated_multiprocess, post_allocated_multithread, dynamic_task_pool_multiprocess
from datatool.utils.file_io import load_jsonlines, save_jsonlines_mpi, save_jsonlines, \
    load_video_bytes_base64, load_audio_bytes_base64, load_image_base64_with_type


def _build_tasks_for_one_file(args):
    """纯函数：给定一个 metafile，返回该文件的所有 task 列表"""
    metafile, target_metafile, dataset_name, skip_processed_items = args
    tasks = []
    
    # 1. 已处理 id 集合
    processed_items = set()
    if os.path.exists(target_metafile):
        if skip_processed_items:
            try:
                processed_items = {item["id"] for item in load_jsonlines(target_metafile)}
            except Exception as e:
                log(f"Warn loading processed items from {target_metafile}: {e}")
        else:
            processed_items = set()

    
    # 2. 读该文件全部 item
    try:
        all_items = load_jsonlines(metafile)
    except Exception as e:
        log(f"Error loading {metafile}: {e}")
        return tasks
    
    # 3. 生成 task
    for item in all_items:
        if item["id"] not in processed_items:
            tasks.append([
                item,
                metafile,          # src_path
                target_metafile,   # save_path
                dataset_name
            ])
    return tasks

def remove_file(path):
    try:
        if os.path.exists(path):
            os.remove(path)
            return True
    except Exception as e:
        log(f"删除文件 {path} 失败: {e}", level=log.WARNING)
    return False


def process_metafiles_hook(dataset_yaml_path,
                           hook_func,
                           dst_metafiles_name=None,
                           skip_processed_items=True,
                           write_mode: Literal['item', 'file'] = 'item',
                           num_workers=1,
                           **hook_kwargs):
    """
    根据输入的钩子函数（hook_func），从 `dataset_yaml_path` 中的 MetaFiles 文件中提取目标数据到 `dst_metafiles_name` 文件夹下。

    # 钩子函数示例 -- 提取出多轮对话数据
    def extract_multivqa(item):
        '''
        Args:
            item (dict): 单个 unistore 格式的标准 json 格式数据
        
        Returns:
            bool: True 表示保留数据，False 表示丢弃数据
            item: 保留的数据（仅在第一个返回值为 True 时生效）
        '''
        if len(item["conversations"]) > 2:
            yield item

    Args:
        dataset_yaml_path (str): 源数据桶的 yaml 文件路径，注意文件级别的并行只支持传进来为yaml的形式
        hook_func (function): 钩子函数迭代器，该函数接收一个字典类型的参数，将需要保存的数据以迭代器形式返回
        dst_metafiles_name (str): 新数据存储目录的名称，若为 None，则原地修改原始文件
        skip_processed_items (bool, optional): 是否跳过已经处理过的数据。若为 False，将会删除已处理过的数据。Defaults to True
        write_mode (Literal['item', 'file'], optional): 写入模式，'item'表示逐行处理并写入，'file'表示批量处理整个文件后写入。Defaults to 'item'
        num_workers (int, optional): 多进程的 worker 数量。Defaults to 1
    """
    @post_allocated_multiprocess
    def _process(task, **kwargs):
        dataset_name, src_path, save_path = task
        # 加载已处理过的数据
        if os.path.exists(save_path) and skip_processed_items:
            if write_mode == "item":
                processed_items = set([item["id"] for item in load_jsonlines(save_path)])
                log(f"found {processed_items} items in caches.")
            else:
                # 跳过已处理完的文件
                log(f"skip {save_path} due to processed cache.")
                return None
        else:
            if os.path.exists(save_path):
                os.remove(save_path)
            processed_items = set()
        all_new_data = []
        log(f"Processing {src_path}...")
        for item in load_jsonlines(src_path):
            if item["id"] in processed_items:
                continue
            for new_data in hook_func(
                    deepcopy(item), 
                    dataset_name=dataset_name,
                    src_path=src_path,
                    **hook_kwargs
                ):
                if write_mode == "item":
                    save_jsonlines([new_data], save_path, mode="a")
                else:
                    all_new_data.append(new_data)
        if write_mode != "item" and len(all_new_data) > 0:
            save_jsonlines(all_new_data, save_path, mode="w")
        return None
        
    if not os.path.exists(dataset_yaml_path):
        raise FileNotFoundError(f"Dataset yaml file {dataset_yaml_path} not found.")

    assert write_mode in ['item', 'file'], f"write_mode should be in ['item', 'file'], but got {write_mode}"

    all_tasks = []
    dataset_config = OmegaConf.load(dataset_yaml_path)
    for dataset_name, config in dataset_config["Datasets"].items():
        metafile_dir = config["MetaFiles"]
        if not os.path.exists(metafile_dir):
            log(f"skip metafile_dir not exist: {metafile_dir}")
            continue
        all_metafiles = glob(
            os.path.join(metafile_dir, "**", "*.jsonl"), recursive=True)
        # 增加当前数据集下的所有文件任务
        if dst_metafiles_name == os.path.basename(metafile_dir):
            log(f"{dst_metafiles_name} has existed, please use a new metafile dirname.", level=log.ERROR)
            return
        for metafile in all_metafiles:
            try:
                with open(metafile, 'r', encoding='utf-8') as f:
                    has_valid_line = False
                    for line in f:
                        line = line.strip()
                        if line:
                            has_valid_line = True
                            break
                if not has_valid_line:
                    log(f"skip empty metafile: {metafile}")
                    continue
            except Exception as e:
                log(f"skip invalid metafile: {metafile}, error: {e}")
                continue
            all_tasks.append([
                dataset_name,
                metafile, 
                os.path.join(os.path.dirname(metafile_dir),
                            dst_metafiles_name, os.path.relpath(metafile, metafile_dir))
            ])
    log(f"Found {len(all_tasks)} metafiles in all.")
    _process(all_tasks, num_workers=num_workers)
    log("Done.")



def _process_yaml_items(dataset_yaml_path, hook_func, dst_metafiles_name, skip_processed_items, num_workers, **hook_kwargs):
    """
    处理yaml配置，item级并行
    """
    @post_allocated_multiprocess
    def _process(task, **kwargs):
        item, src_path, save_path, dataset_name = task
        for new_data in hook_func(deepcopy(item), 
                                  dataset_name=dataset_name, 
                                  src_path=src_path,
                                  save_path=save_path,
                                  **hook_kwargs):
            save_jsonlines_mpi([new_data], save_path, mode="a")
        return None
    # 时间点：任务收集阶段
    task_collection_start = time.time()
    all_tasks = [] # 每个 item 一个任务
    target_metafile_list = []
    dataset_config = OmegaConf.load(dataset_yaml_path)
# 🚀 优化1: 预收集所有metafile信息，避免重复计算
    all_metafiles_info = []
    for dataset_name, config in dataset_config["Datasets"].items():
        metafile_dir = config["MetaFiles"]
        all_metafiles = glob(
            os.path.join(metafile_dir, "**", "*.jsonl"), recursive=True)
        
        # 检查目标目录
        if dst_metafiles_name == os.path.basename(metafile_dir):
            log(f"{dst_metafiles_name} has existed, please use a new metafile dirname.", level=log.ERROR)
            return
        
        # 预收集metafile信息
        for metafile in all_metafiles:
            if os.path.exists(metafile):
                target_metafile = metafile.replace(
                    metafile_dir, 
                    os.path.join(os.path.dirname(metafile_dir), dst_metafiles_name)
                )
                all_metafiles_info.append((metafile, target_metafile, dataset_name))
    
    log(f"Found {len(all_metafiles_info)} metafiles to process")
    
    # 当不跳过已处理项时，主进程先行删除已存在的目标文件，统一打日志
    if not skip_processed_items:
        deleted_count = 0
        for _, target_metafile, _ in all_metafiles_info:
            target_dir = os.path.dirname(target_metafile)
            if not os.path.exists(target_dir):
                continue
            if os.path.exists(target_metafile):
                log(f"delete {target_metafile}")
                try:
                    os.remove(target_metafile)
                    deleted_count += 1
                except Exception as e:
                    log(f"failed to delete {target_metafile}: {e}", level=log.WARNING)
        if deleted_count:
            log(f"deleted {deleted_count} existing target metafiles before collection")
    
    # 🚀 优化2: 并行收集任务，充分利用多核CPU
    
    # 并行收集任务
    log("Collecting tasks in parallel ...")
    with ProcessPoolExecutor(max_workers=min(8, len(all_metafiles_info))) as ex:
        # 先把参数拍平
        job_args = [
            (metafile, target_metafile, dataset_name, skip_processed_items)
            for metafile, target_metafile, dataset_name in all_metafiles_info
        ]
        # 提交
        future_to_file = {ex.submit(_build_tasks_for_one_file, arg): arg[0] for arg in job_args}
        
        # 添加进度条显示并行收集进度
        with tqdm(total=len(job_args), desc="Collecting tasks") as pbar:
            for fut in as_completed(future_to_file):
                file_tasks = fut.result()
                all_tasks.extend(file_tasks)
                
                # 同时收集target_metafile_list
                if file_tasks:
                    # 从第一个task中获取target_metafile
                    target_metafile = file_tasks[0][2]  # save_path
                    target_metafile_list.append(target_metafile)
                
                pbar.update(1)
                pbar.set_postfix({"total_tasks": len(all_tasks)})
        
        log(f"Collected {len(all_tasks)} tasks from {len(all_metafiles_info)} files")
    
    task_collection_time = time.time() - task_collection_start
    log(f"[PERF-MAIN] Task collection took: {task_collection_time:.3f}s")
    log(f"Found {len(all_tasks)} items in all metafiles.")

    # 多进程方式获取每条数据的 predicts
    execution_start = time.time()
    log(f"[PERF-MAIN] Starting ProcessPoolExecutor with {num_workers} workers...")
    _process(all_tasks, num_workers=min(num_workers, len(all_tasks)))
    execution_time = time.time() - execution_start
    log(f"[PERF-MAIN] ProcessPool execution took: {execution_time:.3f}s")
    log(f"[PERF-MAIN] Average time per task: {execution_time/len(all_tasks):.3f}s")
    log("Done.")

    # 统计所有生成的数据条数
    total_count = 0
    folder_counts = {}
    for dataset_name, config in dataset_config["Datasets"].items():
        metafile_dir = config["MetaFiles"]
        if dst_metafiles_name is not None:
            save_root = os.path.join(os.path.dirname(metafile_dir), dst_metafiles_name)
            folder_count = 0
            for root, dirs, files in os.walk(save_root):
                for file in files:
                    if file.endswith('.jsonl'):
                        file_path = os.path.join(root, file)
                        try:
                            count = sum(1 for _ in open(file_path, 'r', encoding='utf-8'))
                            folder_count += count
                        except Exception as e:
                            log(f"[统计] 统计 {file_path} 失败: {e}")
            folder_counts[save_root] = folder_count
            total_count += folder_count
    for folder, count in folder_counts.items():
        log(f"[统计] 转换后数据总数: {count} in {folder}")
    log(f"[统计] 所有文件夹总数: {total_count}")

    # 删除临时的锁文件
    log("删除临时的文件锁...")
    # 收集所有需要清理的目录，避免重复处理
    unique_dirs = set()
    for target_metafile in target_metafile_list:
        target_metafile_dir = os.path.dirname(target_metafile)
        unique_dirs.add(target_metafile_dir)
    
    log(f"需要清理锁文件的目录数量: {len(unique_dirs)}")
    
    # 收集所有锁文件
    all_lock_files = []
    for target_metafile_dir in unique_dirs:
        if not os.path.exists(target_metafile_dir):
            log(f"目录不存在，跳过: {target_metafile_dir}")
            continue
        try:
            locks = glob(
                os.path.join(target_metafile_dir, "**", "*.jsonl.lock"), recursive=True
            )
            if locks:
                all_lock_files.extend(locks)
                log(f"找到 {len(locks)} 个锁文件 in {target_metafile_dir}")
            else:
                log(f"不存在锁文件 in {target_metafile_dir}")
                return
        except Exception as e:
            log(f"搜索目录 {target_metafile_dir} 锁文件报错: {e}", level=log.WARNING)

    log(f"待删除锁文件总数: {len(all_lock_files)}")
    
    if len(all_lock_files) == 0:
        return

    # 多线程并发删除文件
    total_removed = 0
    with ThreadPoolExecutor(max_workers=min(4, len(all_lock_files))) as executor:
        futures = {executor.submit(remove_file, f): f for f in all_lock_files}
        for i, future in enumerate(as_completed(futures), 1):
            if future.result():
                total_removed += 1
            if i % 1000 == 0:
                log(f"已删除 {i} 个锁文件...")

    log(f"锁文件清理完成，共删除 {total_removed} 个文件")
    log("Done.")


def process_items_hook_multithreads(dataset_yaml_path,
                                    hook_func,
                                    dst_metafiles_name=None,
                                    skip_processed_items=True,
                                    num_workers=1,
                                    **hook_kwargs):
    """
    根据输入的钩子函数（hook_func），从 `dataset_yaml_path` 中的 MetaFiles 文件中提取目标数据到 `dst_metafiles_name` 文件夹下。
    使用多线程处理，适用于I/O密集型任务。

    # 钩子函数示例 -- 提取出多轮对话数据
    def extract_multivqa(item):
        '''
        Args:
            item (dict): 单个 unistore 格式的标准 json 格式数据
        
        Returns:
            bool: True 表示保留数据，False 表示丢弃数据
            item: 保留的数据（仅在第一个返回值为 True 时生效）
        '''
        if len(item["conversations"]) > 2:
            yield item

    Args:
        dataset_yaml_path (str): 源数据桶的 yaml 文件路径
        hook_func (function): 钩子函数迭代器，该函数接收一个字典类型的参数，将需要保存的数据以迭代器形式返回
        dst_metafiles_name (str): 新数据存储目录的名称，若为 None，则原地修改原始文件
        skip_processed_items (bool, optional): 是否跳过已经处理过的数据。若为 False，将会删除已处理过的数据。Defaults to True
        num_workers (int, optional): 多线程的 worker 数量。Defaults to 1
    """
    def single_process(task):
        item, src_path, save_path, dataset_name = task
        for new_data in hook_func(
                deepcopy(item), 
                src_path=src_path,
                save_path=save_path,
                dataset_name=dataset_name,
                **hook_kwargs
            ):
            if new_data:
                save_jsonlines_mpi([new_data], save_path, mode="a")
        return None
    @post_allocated_multithread
    def _process(task, **kwargs):
        item, src_path, save_path, dataset_name = task
        for new_data in hook_func(
                deepcopy(item), 
                src_path=src_path,
                save_path=save_path,
                dataset_name=dataset_name,
                **hook_kwargs
            ):
            if new_data:
                save_jsonlines_mpi([new_data], save_path, mode="a")
        return None
        
    if not os.path.exists(dataset_yaml_path):
        raise FileNotFoundError(f"Dataset yaml file {dataset_yaml_path} not found.")

    # 时间点：任务收集阶段
    task_collection_start = time.time()
    all_tasks = [] # 每个 item 一个任务
    target_metafile_list = []
    dataset_config = OmegaConf.load(dataset_yaml_path)
    
    # 🚀 优化1: 预收集所有metafile信息，避免重复计算
    all_metafiles_info = []
    for dataset_name, config in dataset_config["Datasets"].items():
        metafile_dir = config["MetaFiles"]
        all_metafiles = glob(
            os.path.join(metafile_dir, "**", "*.jsonl"), recursive=True)
        
        # 检查目标目录
        if dst_metafiles_name == os.path.basename(metafile_dir):
            log(f"{dst_metafiles_name} has existed, please use a new metafile dirname.", level=log.ERROR)
            return
        
        # 预收集metafile信息
        for metafile in all_metafiles:
            if os.path.exists(metafile):
                target_metafile = metafile.replace(
                    metafile_dir, 
                    os.path.join(os.path.dirname(metafile_dir), dst_metafiles_name)
                )
                all_metafiles_info.append((metafile, target_metafile, dataset_name))
    
    log(f"Found {len(all_metafiles_info)} metafiles to process")
    
    # 当不跳过已处理项时，主进程先行删除已存在的目标文件，统一打日志
    if not skip_processed_items:
        deleted_count = 0
        for _, target_metafile, _, _ in all_metafiles_info:
            target_dir = os.path.dirname(target_metafile)
            if not os.path.exists(target_dir):
                continue
            if os.path.exists(target_metafile):
                log(f"delete {target_metafile}")
                try:
                    os.remove(target_metafile)
                    deleted_count += 1
                except Exception as e:
                    log(f"failed to delete {target_metafile}: {e}", level=log.WARNING)
        if deleted_count:
            log(f"deleted {deleted_count} existing target metafiles before collection")
    
    # 🚀 优化2: 并行收集任务，充分利用多核CPU
    
    # 并行收集任务
    log("Collecting tasks in parallel ...")
    with ProcessPoolExecutor(max_workers=min(8, len(all_metafiles_info))) as ex:
        # 先把参数拍平
        job_args = [
            (metafile, target_metafile, dataset_name, skip_processed_items)
            for metafile, target_metafile, dataset_name in all_metafiles_info
        ]
        # 提交
        future_to_file = {ex.submit(_build_tasks_for_one_file, arg): arg[0] for arg in job_args}
        # 汇总
        # all_tasks = []
        target_metafile_list = []
        
        # 添加进度条显示并行收集进度
        with tqdm(total=len(job_args), desc="Collecting tasks") as pbar:
            for fut in as_completed(future_to_file):
                file_tasks = fut.result()
                all_tasks.extend(file_tasks)
                
                # 同时收集target_metafile_list
                if file_tasks:
                    # 从第一个task中获取target_metafile
                    target_metafile = file_tasks[0][2]  # save_path
                    target_metafile_list.append(target_metafile)
                
                pbar.update(1)
                pbar.set_postfix({"total_tasks": len(all_tasks)})
        
        log(f"Collected {len(all_tasks)} tasks from {len(all_metafiles_info)} files")
    
    task_collection_time = time.time() - task_collection_start
    log(f"[PERF-MAIN] Task collection took: {task_collection_time:.3f}s")
    log(f"Found {len(all_tasks)} items in all metafiles.")

    
    # 多线程方式获取每条数据的 predicts
    log(f"[PERF-MAIN] Starting ThreadPoolExecutor with {num_workers} workers...")
    execution_start = time.time()
    _process(all_tasks, num_workers=min(num_workers, len(all_tasks)))

    # with ThreadPoolExecutor(max_workers=num_workers) as pool:
    #     futures = {pool.submit(single_process, sample): sample for sample in all_tasks}
    #     # 添加进度条
    #     with tqdm(total=len(all_tasks), desc="Processing") as pbar:
    #         for _ in as_completed(futures):
    #             pbar.update(1)
    
    execution_time = time.time() - execution_start
    log(f"[PERF-MAIN] ProcessPool execution took: {execution_time:.3f}s")
    log(f"[PERF-MAIN] Average time per task: {execution_time/len(all_tasks):.3f}s")
    
    # 删除临时的锁文件
    log("删除临时的文件锁...")
    # 收集所有需要清理的目录，避免重复处理
    unique_dirs = set()
    for target_metafile in target_metafile_list:
        target_metafile_dir = os.path.dirname(target_metafile)
        unique_dirs.add(target_metafile_dir)
    
    log(f"需要清理锁文件的目录数量: {len(unique_dirs)}")
    
    # 收集所有锁文件
    all_lock_files = []
    for target_metafile_dir in unique_dirs:
        if not os.path.exists(target_metafile_dir):
            log(f"目录不存在，跳过: {target_metafile_dir}")
            continue
        try:
            locks = glob(
                os.path.join(target_metafile_dir, "**", "*.jsonl.lock"), recursive=True
            )
            if locks:
                all_lock_files.extend(locks)
                log(f"找到 {len(locks)} 个锁文件 in {target_metafile_dir}")
        except Exception as e:
            log(f"搜索目录 {target_metafile_dir} 锁文件报错: {e}", level=log.WARNING)

    log(f"待删除锁文件总数: {len(all_lock_files)}")

    # 多线程并发删除文件
    total_removed = 0
    if len(all_lock_files) == 0:
        return
    with ThreadPoolExecutor(max_workers=min(16, len(all_lock_files))) as executor:
        futures = {executor.submit(remove_file, f): f for f in all_lock_files}
        for i, future in enumerate(as_completed(futures), 1):
            if future.result():
                total_removed += 1
            if i % 1000 == 0:
                log(f"已删除 {i} 个锁文件...")

    log(f"锁文件清理完成，共删除 {total_removed} 个文件")
    log("Done.")



def process_data_hook(dataset_path,
                       hook_func,
                       dst_metafiles_name=None,
                       skip_processed_items=True,
                       num_workers=1,
                       parallel_level="item",
                       parallel_type="process",
                       **hook_kwargs):
    """
    通用入口，支持多种文件格式和并行粒度，支持多进程和多线程两种并行方式
    
    Args:
        dataset_path: 数据集路径
        hook_func: 钩子函数
        dst_metafiles_name: 目标文件夹名称
        skip_processed_items: 是否跳过已处理过的数据，默认True
        num_workers: 并行进程/线程数
        parallel_level: "item"（默认，单条数据并行）或"file"（文件级并行）
        parallel_type: "process"（多进程，默认）或"thread"（多线程）
        hook_kwargs: 钩子函数参数
    
    使用场景说明:
    - 多进程(process): 适用于CPU密集型任务，如复杂的数据变换、模型推理等
    - 多线程(thread): 适用于I/O密集型任务，如文件读写、网络请求、简单的数据处理等
    
    并行级别说明:
    - item级并行: 对每个数据项进行并行处理，适合数据量大的场景
    - file级并行: 对每个文件进行并行处理，适合文件数量多但单文件数据量适中的场景
    """
    SUPPORTED_FORMATS = {'.yaml': _process_yaml_items, '.yml': _process_yaml_items}
    if not os.path.exists(dataset_path):
        raise FileNotFoundError(f"Dataset yaml/jsonl file {dataset_path} not found.")
    ext = os.path.splitext(dataset_path)[-1].lower()
    if ext not in SUPPORTED_FORMATS:
        raise ValueError(f"不支持的文件格式: {ext}")
    
    if parallel_level == "item":
        if parallel_type == "process":
            # 使用多进程处理
            process_func = SUPPORTED_FORMATS[ext]
            process_func(dataset_path, hook_func, dst_metafiles_name, skip_processed_items, num_workers, **hook_kwargs)
        elif parallel_type == "thread":
            # 使用多线程处理 - 仅支持yaml格式
            #if not dataset_path.endswith(('.yaml', '.yml')):
            #    raise ValueError("多线程模式目前仅支持yaml文件")
            process_items_hook_multithreads(dataset_path, hook_func, dst_metafiles_name, skip_processed_items, num_workers, **hook_kwargs)
        else:
            raise ValueError(f"不支持的并行类型: {parallel_type}，支持的类型: ['process', 'thread']")
    elif parallel_level == "file":
        # file级并行必须传进来的是yaml文件，且只支持多进程
        if not dataset_path.endswith(('.yaml', '.yml')):
            raise ValueError("文件级并行必须传进来的是yaml文件")
        if parallel_type != "process":
            log(f"文件级并行强制使用多进程模式，忽略parallel_type={parallel_type}", level=log.WARNING)
        process_metafiles_hook(dataset_path, hook_func, dst_metafiles_name, skip_processed_items, 
                              write_mode='file', num_workers=num_workers, **hook_kwargs)
    else:
        raise NotImplementedError(f"不支持 {parallel_level}级别并行")


def get_image_base64(image_path, new_width=None, new_height=None):
  """
  function: 本地图片路径转化成url
  image_path: str 图片路径
  """
  # 读取图片
  with open(image_path, "rb") as img:
    # 调整图片尺寸
    if new_width and new_height:
      with Image.open(image_path, "r") as img:
          img = img.resize((new_width, new_height))
          img = img.convert("RGB")
          # 将图片保存到字节流中，而不是文件中
          buffered = BytesIO()
          img.save(buffered, format="JPEG")

      # 获取Base64编码
      base64_encoded = base64.b64encode(buffered.getvalue()).decode('utf-8')
    else:
      base64_encoded = base64.b64encode(img.read()).decode("utf-8")

  # 构建data URL
  base64_url = f"data:image/jpeg;base64,{base64_encoded}"
  return base64_url


# 提取视频
def get_video_base64(video_path):
    with open(video_path, 'rb') as video_file:
        video_base = base64.b64encode(video_file.read()).decode('utf-8')
    base64_url = f"data:image/jpeg;base64,{video_base}"
    return base64_url

def get_audio_base64(audio_path):
    # 先空实现
    return None

def replace_media_path(path, source_dir, target_dir):
    if source_dir and target_dir and path.startswith(source_dir):
        return path.replace(source_dir, target_dir, 1)
    return path

def load_message_from_data(
        meta_item, 
        load_media: bool = True, 
        system_prompt: str = None,
        merge_answer: bool = False,
        source_dir: str = None,
        target_dir: str = None,
        media_to_base64 : bool = True
    ) -> Union[List, Tuple[List, str]]:
    """
    处理多模态对话数据，支持<image>、<video>、<audio>占位符替换为base64内容
    支持source_dir/target_dir路径替换（考虑到media文件老是迁移，所以需要支持路径替换）
    """
    import uuid
    messages = []
    images = meta_item.get('images', [])
    videos = meta_item.get('videos', [])
    audios = meta_item.get('audios', [])

    image_idx, video_idx, audio_idx = 0, 0, 0

    conversations = meta_item['messages']
    if system_prompt is not None:
        messages.append({"role": "system", "content": [{"type": "text", "text": system_prompt}]})
    for idx, conv in enumerate(conversations):
        role = conv['role']
        content = []
        conv_text = conv['content']

        if role == 'user':
            last_idx = 0
            for match in re.finditer(r'<(image|video|audio)>', conv_text):
                media_type = match.group(1)
                start, end = match.span()
                if start > last_idx:
                    content.append({"type": "text", "text": conv_text[last_idx:start]})
                if media_type == "image" and image_idx < len(images):
                    img_path = replace_media_path(images[image_idx], source_dir, target_dir)
                    if load_media:
                        if media_to_base64:
                            img64, img_type = load_image_base64_with_type(img_path)
                            content.append({"type": "image_url", "image_url": {"url": f"data:{img_type};base64,{img64}"}})
                        else:
                            content.append({"type": "image_url", "image_url": {"url": img_path}})
                    image_idx += 1
                elif media_type == "video" and video_idx < len(videos):
                    vid_path = replace_media_path(videos[video_idx], source_dir, target_dir)
                    if load_media:
                        if media_to_base64:
                            vid64 = load_video_bytes_base64(vid_path)
                            content.append({"type": "video_url", "video_url": {"url": f"data:video/mp4;base64,{vid64}"}})
                        else:
                            content.append({"type": "video_url", "video_url": {"url": vid_path}})
                    video_idx += 1
                elif media_type == "audio" and audio_idx < len(audios):
                    aud_path = replace_media_path(audios[audio_idx], source_dir, target_dir)
                    if load_media:
                        if media_to_base64:
                            aud64 = load_audio_bytes_base64(aud_path)
                            content.append({"type": "input_audio", "audio_url": {"url": f"data:audio/ogg;base64,{aud64}"}})
                        else:
                            content.append({"type": "input_audio", "input_audio": {"url": aud_path}})
                    audio_idx += 1
                last_idx = end
            if last_idx < len(conv_text):
                content.append({"type": "text", "text": conv_text[last_idx:]})
            messages.append({"role": role, "content": content})
        elif role == 'system':
            content = [{"type": "text", "text": conv_text}]
            messages.append({"role": role, "content": content})
        elif role == 'assistant':
            if idx == len(conversations) - 1:
                answer = conv_text
            else:
                content = [{"type": "text", "text": conv_text}]
                messages.append({"role": role, "content": content})
        else:
            raise ValueError(f"Unknown role: {role}")
    if merge_answer:
        messages.append({"role": "assistant", "content": [{"type": "text", "text": answer}]})
        return messages
    else:
        return messages, answer

def load_media_item(path: str, media_type: str, load_media: bool, media_to_base64: bool):
    """
    返回多媒体内容 dict
    """
    if not load_media:
        return None
    
    if media_to_base64:
        if media_type == "image":
            data, img_type = load_image_base64_with_type(path)
            return {"type": "image_url", "image_url": {"url": f"data:{img_type};base64,{data}"}} 
        elif media_type == "video":
            data = load_video_bytes_base64(path)
            return {"type": "video_url", "video_url": {"url": f"data:video/mp4;base64,{data}"}} 
        elif media_type == "audio":
            data = load_audio_bytes_base64(path)
            return {"type": "input_audio", "audio_url": {"url": f"data:audio/ogg;base64,{data}"}} 
    else:
        # 不转base64，直接返回路径
        if media_type == "image":
            return {"type": "image_url", "image_url": {"url": path}} 
        elif media_type == "video":
            return {"type": "video_url", "video_url": {"url": path}} 
        elif media_type == "audio":
            return {"type": "input_audio", "audio_url": {"url": path}} 


def remove_media_tags(text):
    return re.sub(r'<(image|video|audio)>', '', text)



def build_conversations(prompt: str,
                        response: str,
                        history: list[str, str],
                        train_history: bool):
    conversations = []
    for ques, answ in history:
        if_train = train_history
        conversations.append({"role": "user", "text": ques})
        conversations.append({"role": "assistant", "text": answ, "if_train": if_train})

    conversations.append({"role": "user", "text": prompt})
    conversations.append({"role": "assistant", "text": response, "if_train": True})

    return conversations


def test_hook(item, **kwargs):
    message, answer = load_message_from_data(item)
    print(message)
    yield item


def process_turn_hook(dataset_path,
                      turn_func,
                      prepare_initial_tasks_func,
                      dst_metafiles_name=None,
                      skip_processed_items=True,
                      num_workers=1,
                      **hook_kwargs):
    """
    多轮对话专用处理函数，支持轮次级别的动态并行
    
    Args:
        dataset_path: 数据集路径（yaml或jsonl）
        turn_func: 单轮处理函数
        prepare_initial_tasks_func: 准备初始任务函数
        dst_metafiles_name: 目标文件夹名称
        skip_processed_items: 是否跳过已处理数据
        num_workers: worker数量
    """

    if not os.path.exists(dataset_path):
        raise FileNotFoundError(f"Dataset file {dataset_path} not found.")
    
    ext = os.path.splitext(dataset_path)[-1].lower()
    
    @dynamic_task_pool_multiprocess
    def _turn_wrapper(task, **kwargs):
        """包装turn_func"""
        return turn_func(task, **kwargs)
    
    initial_tasks = []
    
    if ext in ['.yaml', '.yml']:
        dataset_config = OmegaConf.load(dataset_path)
        
        for dataset_name, config in dataset_config["Datasets"].items():
            metafile_dir = config["MetaFiles"]
            if not os.path.exists(metafile_dir):
                log(f"Skip non-existent metafile_dir: {metafile_dir}")
                continue
                
            all_metafiles = glob(os.path.join(metafile_dir, "**", "*.jsonl"), recursive=True)
            
            if dst_metafiles_name == os.path.basename(metafile_dir):
                log(f"{dst_metafiles_name} conflicts with source, please use a different name.", level=log.ERROR)
                return
            
            for metafile in all_metafiles:
                try:
                    with open(metafile, 'r', encoding='utf-8') as f:
                        has_valid_line = False
                        for line in f:
                            if line.strip():
                                has_valid_line = True
                                break
                    if not has_valid_line:
                        log(f"Skip empty metafile: {metafile}")
                        continue
                except Exception as e:
                    log(f"Skip invalid metafile: {metafile}, error: {e}")
                    continue
                
                target_metafile = metafile.replace(
                    metafile_dir,
                    os.path.join(os.path.dirname(metafile_dir), dst_metafiles_name)
                )
                
                processed_items = set()
                if os.path.exists(target_metafile):
                    if skip_processed_items:
                        try:
                            processed_items = {item["id"] for item in load_jsonlines(target_metafile)}
                            log(f"Found {len(processed_items)} processed items in {target_metafile}")
                        except Exception as e:
                            log(f"Error loading processed items from {target_metafile}: {e}")
                    else:
                        # 🔧 当不跳过已处理项时，删除已存在的目标文件
                        log(f"[skip_processed_items=False] Deleting existing target file: {target_metafile}")
                        try:
                            os.remove(target_metafile)
                            log(f"✅ Deleted: {target_metafile}")
                        except Exception as e:
                            log(f"⚠️ Failed to delete {target_metafile}: {e}", level=log.WARNING)
                
                for item in load_jsonlines(metafile):
                    if item["id"] not in processed_items:
                        try:
                            initial_task = prepare_initial_tasks_func(
                                item,
                                src_path=metafile,
                                save_path=target_metafile,
                                config=config,
                                dataset_name=dataset_name,
                                **hook_kwargs
                            )
                            initial_tasks.append(initial_task)
                        except Exception as e:
                            log(f"Error preparing task for item {item.get('id', 'unknown')}: {e}", level=log.WARNING)
    else:
        raise ValueError(f"Unsupported file format: {ext}")
    
    log(f"Prepared {len(initial_tasks)} initial tasks from {dataset_path}")
    
    if not initial_tasks:
        log("No tasks to process.")
        return
    
    results = _turn_wrapper(initial_tasks, num_workers=num_workers, **hook_kwargs)
    
    # 统计成功和失败的数量
    # 按item_id分组，找出每个item的最后一轮结果
    item_results = {}
    for result in results:
        item_id = result.get('item_id')
        turn = result.get('turn', 0)
        
        # 保留每个item的最大轮次（最后一轮）
        if item_id not in item_results or turn > item_results[item_id]['turn']:
            item_results[item_id] = result
    
    # 统计最后一轮的成功/失败
    successful_items = set()
    failed_items = set()
    for item_id, last_result in item_results.items():
        if last_result.get('result') is not None:
            successful_items.add(item_id)
        else:
            failed_items.add(item_id)
    
    log("=" * 80)
    log(f"所有轮次处理完成！")
    log(f"总任务数: {len(initial_tasks)}")
    log(f"实际处理的items: {len(item_results)}")
    log(f"成功的items: {len(successful_items)}")
    log(f"失败的items: {len(failed_items)}")
    log(f"未处理的items: {len(initial_tasks) - len(item_results)}")
    if failed_items and len(failed_items) <= 20:
        log(f"失败的item IDs: {sorted(list(failed_items))[:20]}")
    log("=" * 80)
    
    # 删除临时锁文件
    log("Cleaning up lock files...")
    if ext in ['.yaml', '.yml']:
        dataset_config = OmegaConf.load(dataset_path)
        for dataset_name, config in dataset_config["Datasets"].items():
            metafile_dir = config["MetaFiles"]
            target_dir = os.path.join(os.path.dirname(metafile_dir), dst_metafiles_name)
            if os.path.exists(target_dir):
                lock_files = glob(os.path.join(target_dir, "**", "*.lock"), recursive=True)
                for lock_file in lock_files:
                    try:
                        if os.path.exists(lock_file):
                            os.remove(lock_file)
                    except Exception as e:
                        log(f"Failed to remove lock file {lock_file}: {e}", level=log.WARNING)
                log(f"Removed {len(lock_files)} lock files from {target_dir}")
    
    log("Done.")