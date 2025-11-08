import os
import functools
import multiprocessing
import threading
from concurrent.futures import ProcessPoolExecutor
from queue import Empty, Queue
from functools import partial
import time
from tqdm import tqdm

from datatool.logger import log

def split_data_for_workers(data_list, num_workers: int=1):
    """根据 num_workers 将 data_list 平均分配给每个 worker

    Args:
        data_list (list): 数据列表，每个元素是一条独立数据
        num_workers (int, optional): 进程数. Defaults to 1.

    Returns:
        distributed_data: list of list, 每个元素是一个 worker 的数据列表
    """
    distributed_data = [[] for _ in range(num_workers)]
    for i, c_data in enumerate(data_list):
        distributed_data[i % num_workers].append(c_data)
    return distributed_data

def pre_allocated_multiprocess(func):
    """多进程装饰器（预分配数据，计划制）

    Example:
        @pre_allocated_multiprocess
        def func(data, **kwargs):
            '''
            Args:
                data (list): distributed data to be processed
                kwargs (dict): diy params, including 'process_id'
            '''
            # get diy params
            para1 = kwargs.get("para1", None)
            process_id = kwargs.get("process_id")
            # do something
            return [...]  # Return the processed results
        # Use
        results = func(data, num_workers, **kwargs)
    """
    @functools.wraps(func)
    def wrapper(all_data, num_workers=1, **kwargs):
        """Wrapper
        Args:
            all_data (list): all data to be processed 
            num_workers (int): workers num
        Returns:
            all_results (list): 各个进程返回的结果汇总
        """
        # split src data
        # num_workers = min(num_workers, multiprocessing.cpu_count())
        log(f"Using {num_workers} processes...")
        all_data_list = split_data_for_workers(all_data, num_workers)
        
        # Create a manager for shared objects
        manager = multiprocessing.Manager()
        result_queue = manager.Queue()
        counter_queue = manager.Queue()
        
        # create mp
        processes = []
        for pid in range(num_workers):
            new_kwargs = kwargs.copy()
            new_kwargs["process_id"] = pid
            p = multiprocessing.Process(
                target=lambda q, c, data, kw: process_worker(q, c, data, kw, func), 
                args=(result_queue, counter_queue, all_data_list[pid], new_kwargs))
            processes.append(p)
            p.start()
        
        # wait for all processes to finish
        for p in processes:
            p.join()
        
        # collect results
        all_results = []
        while not result_queue.empty():
            _result = result_queue.get()
            if isinstance(_result, list) or isinstance(_result, tuple):
                all_results.extend(_result)
            else:
                all_results.append(_result)
        
        return all_results
    
    def process_worker(result_queue, counter_queue, data, kwargs, func):
        """处理数据的工作函数"""
        try:
            results = func(data, **kwargs)
            result_queue.put(results)
            
            # 计算处理的数据项数量
            if isinstance(results, list) or isinstance(results, tuple):
                counter_queue.put(len(results))
            else:
                counter_queue.put(1)
                
        except Exception as e:
            log(f"Process {kwargs.get('process_id')} Error: {e}")
            
    return wrapper

def post_allocated_multiprocess(func):
    @functools.wraps(func)
    def wrapper(all_data, num_workers=1, **kwargs):
        log(f"Using {num_workers} processes...")
        
        # 使用原生的 multiprocessing.Queue 而不是 manager.Queue
        data_queue = multiprocessing.Queue()
        result_queue = multiprocessing.Queue()
        
        # 使用单独的进程来填充队列，避免阻塞主进程
        def queue_filler(data_list, queue):
            for data in data_list:
                queue.put(data)
            # 放入结束标记
            for _ in range(num_workers):
                queue.put(None)
        
        # 启动填充进程
        filler_process = multiprocessing.Process(
            target=queue_filler, 
            args=(all_data, data_queue)
        )
        filler_process.start()
        
        # 工作进程函数
        def worker(queue, result_queue, **kwargs):
            while True:
                try:
                    data = queue.get(timeout=1)
                    if data is None:  # 结束标记
                        break
                    result = func(data, **kwargs)
                    result_queue.put(result)
                except Exception as e:
                    log(f"Process {kwargs.get('process_id')} Error: {e}")
                    break
            # log(f"Process {kwargs.get('process_id')} finished!!!")
        
        # 立即启动所有工作进程
        processes = []
        for process_id in range(num_workers):
            new_kwargs = kwargs.copy()
            new_kwargs["process_id"] = process_id
            p = multiprocessing.Process(
                target=worker, args=(data_queue, result_queue), kwargs=new_kwargs)
            p.start()
            processes.append(p)
        
        # 收集结果
        all_results = []
        with tqdm(total=len(all_data), desc="Processing data") as pbar:
            for _ in range(len(all_data)):
                result = result_queue.get()
                all_results.append(result)
                pbar.update(1)
        
        # 等待所有进程完成
        filler_process.join()
        for p in processes:
            p.join()
        
        return all_results
    
    return wrapper

def post_allocated_multithread(func):
    @functools.wraps(func)
    def wrapper(all_data, num_workers=1, **kwargs):
        log(f"Using {num_workers} threads...")
        
        if not all_data:
            return []
        
        data_queue = Queue()
        result_queue = Queue()

        # 填充者线程
        def queue_filler(data_list, queue, n_workers):
            for item in data_list:
                queue.put(item)
            for _ in range(n_workers):
                queue.put(None)

        filler_thread = threading.Thread(
            target=queue_filler, args=(all_data, data_queue, num_workers), daemon=True
        )
        filler_thread.start()

        # 工作者线程
        def worker(data_q, result_q, **kw):
            while True:
                data = data_q.get()
                if data is None:
                    data_q.task_done()  # 重要：为None标记task_done
                    break
                try:
                    result = func(data, **kw)
                    result_q.put(result)
                except Exception as e:
                    log(f"Thread {kw.get('process_id')} Error: {e}")
                    result_q.put(None)  # 确保结果队列不会卡住
                finally:
                    data_q.task_done()

        threads = []
        for tid in range(num_workers):
            new_kwargs = {**kwargs, "process_id": tid}
            t = threading.Thread(target=worker, args=(data_queue, result_queue), kwargs=new_kwargs, daemon=True)
            t.start()
            threads.append(t)

        all_results = []
        with tqdm(total=len(all_data), desc="Processing data") as pbar:
            for _ in range(len(all_data)):
                result = result_queue.get()
                if result is not None:  # 过滤掉错误的结果
                    all_results.append(result)
                pbar.update(1)

        # 等待填充线程结束
        filler_thread.join(timeout=5)
        
        # 不需要再join data_queue，因为worker已经处理完了所有数据
        # 等待所有worker线程结束
        for t in threads:
            t.join(timeout=5)

        return all_results

    return wrapper


def dynamic_task_pool_multiprocess(func):
    """
    动态任务池多进程装饰器（支持子任务动态生成）
    
    适用场景：
    - 任务有依赖关系（如多轮对话，后一轮依赖前一轮结果）
    - 不同任务的子任务数量不同（如不同item需要不同轮数）
    - 需要动态负载均衡
    """
    @functools.wraps(func)
    def wrapper(initial_tasks, num_workers=1, **kwargs):
        log(f"Using {num_workers} processes with dynamic task pool...")
        
        if not initial_tasks:
            log("No initial tasks to process.")
            return []
        
        manager = multiprocessing.Manager()
        task_queue = manager.Queue()
        result_queue = manager.Queue()
        # 注意：使用 multiprocessing.Value 而不是 manager.Value，因为需要 get_lock() 方法
        active_workers = multiprocessing.Value('i', num_workers)
        total_completed = multiprocessing.Value('i', 0)
        
        # 填充初始任务
        for task in initial_tasks:
            task_queue.put(task)
        
        log(f"Initial tasks count: {len(initial_tasks)}")
        
        def worker(worker_id, task_q, result_q, active_count, completed_count, **kw):
            """Worker进程"""
            local_completed = 0
            while True:
                try:
                    task = task_q.get(timeout=3)
                    if task is None:
                        break
                    
                    kw_copy = kw.copy()
                    kw_copy['process_id'] = worker_id
                    
                    try:
                        result = func(task, **kw_copy)
                        
                        if result and 'result' in result:
                            result_q.put({
                                'task_id': task.get('task_id'),
                                'item_id': task.get('item_id'),
                                'turn': task.get('turn'),
                                'result': result.get('result')
                            })
                        
                        if result and 'next_tasks' in result and result['next_tasks']:
                            for next_task in result['next_tasks']:
                                task_q.put(next_task)
                        
                        local_completed += 1
                        
                    except Exception as e:
                        log(f"Worker {worker_id} error: {e}", level=log.ERROR)
                        import traceback
                        log(traceback.format_exc(), level=log.ERROR)
                    
                except Empty:
                    if task_q.qsize() == 0:
                        time.sleep(1.0)
                        if task_q.qsize() == 0:
                            time.sleep(0.5)
                            if task_q.qsize() == 0:
                                break
                except Exception as e:
                    log(f"Worker {worker_id} unexpected error: {e}", level=log.ERROR)
                    break
            
            with completed_count.get_lock():
                completed_count.value += local_completed
            
            with active_count.get_lock():
                active_count.value -= 1
                log(f"Worker {worker_id} finished, processed {local_completed} tasks. Active workers: {active_count.value}")
        
        # 启动workers
        processes = []
        for worker_id in range(num_workers):
            p = multiprocessing.Process(
                target=worker,
                args=(worker_id, task_queue, result_queue, active_workers, total_completed),
                kwargs=kwargs
            )
            p.start()
            processes.append(p)
        
        # 监控进度
        def progress_monitor():
            last_count = 0
            with tqdm(desc="Processing turns", unit="turn") as pbar:
                while active_workers.value > 0:
                    current_count = total_completed.value
                    if current_count > last_count:
                        pbar.update(current_count - last_count)
                        last_count = current_count
                    time.sleep(0.5)
                current_count = total_completed.value
                if current_count > last_count:
                    pbar.update(current_count - last_count)
        
        monitor_thread = threading.Thread(target=progress_monitor, daemon=True)
        monitor_thread.start()
        
        # 等待所有进程结束
        for p in processes:
            p.join()
        
        monitor_thread.join(timeout=2)
        
        # 收集所有结果
        all_results = []
        while not result_queue.empty():
            try:
                result = result_queue.get_nowait()
                all_results.append(result)
            except Empty:
                break
        
        log(f"All workers finished. Total completed: {total_completed.value}, Results collected: {len(all_results)}")
        
        return all_results
    
    return wrapper