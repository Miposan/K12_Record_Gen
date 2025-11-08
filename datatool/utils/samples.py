import os
import re
import json
import random

from glob import glob
from tqdm import tqdm

def stat_sample_nums(meta_dir):
    """递归统计 meta_dir 目录下所有 jsonl 文件中的样本数量
    Args:
        meta_dir: 输入的 meta_dir 路径
        
    Returns:
        int: 总的样本数量
    """
    meta_files = glob(os.path.join(meta_dir, "**", "*.jsonl"), recursive=True)
    sample_nums = 0
    for file in meta_files:
        with open(file, 'r', encoding='utf-8') as f:
            sample_nums += sum(1 for _ in f)
    return sample_nums

def reservoir_sampling_from_jsonls(file_paths, sample_size=20000):
    """
    从多个 JSONL 文件中随机选取样本，使用 Reservoir Sampling 算法。
    
    :param file_paths: 包含多个 JSONL 文件路径的列表
    :param sample_size: 需要选取的样本数量，默认为 10000
    :return: 返回选取的样本列表
    """
    sampled_data, unsampled_data = [], []

    all_idx = 0
    # 逐个读取文件
    for file_path in file_paths:
        with open(file_path, 'r', encoding='utf-8') as f:
            for line in f:
                all_idx += 1
                data = json.loads(line.strip())
                # 采样阶段：前 sample_size 条数据直接放入样本中
                if len(sampled_data) < sample_size:
                    sampled_data.append(data)
                else:
                    # 以后每读取一条数据，按概率随机替换样本中的数据
                    # 产生一个随机索引
                    rand_index = random.randint(0, all_idx)
                    if rand_index < sample_size:
                        unsampled_data.append(sampled_data[rand_index])
                        sampled_data[rand_index] = data
                    else:
                        unsampled_data.append(data)
    return sampled_data, unsampled_data

def find_all_media_tags(s, pattern=r'<\|ZP_MM_PLH=(\w+)\|>'):
    """找到所有媒体标记及其位置
    Args:
        s (str): 输入字符串
        pattern (str): 匹配模式
    Returns:
        result (list): [(media_id, start_pos, end_pos), ...]
    """
    matches = re.finditer(pattern, s)
    result = []
    for match in matches:
        start = match.start()
        end = match.end()
        result.append((match.group(1), start, end))
    return result