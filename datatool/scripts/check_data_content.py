"""
功能说明：
检查数据集当中的内容

    - 检查多媒体资源文件是否存在/是否有效
    - 多轮think数据格式检查（可选，通过 --check_multi_turn_think 启用）
        - 
    - Long CoT 格式检查（可选，通过 --check_long_cot 启用）
        - think/answer 格式检查
        --think_format指定格式进行检查
        1.think_answer: <think>xxxx</think><answer>xxxx\\boxed{yy}</answer>
        2.think: <think>xxxx</think>xxxx\\boxed{yyy}
        3.no_think: <think></think>xxxx

"""

import os
import re
import logging

from copy import deepcopy
from omegaconf import OmegaConf

from PIL import Image
import cv2
import soundfile as sf

from datatool.logger import log
from datatool.arguments import get_args
from datatool.utils.data import load_message_from_data, process_data_hook
from datatool.utils.file_io import save_jsonlines
from functools import partial


def check_media_data(item):
    """
    检查item中的videos、images、audios字段（如存在且为列表），判断每个文件路径是否存在，不存在的文件全部返回。
    返回：
        ok: bool, True表示全部存在，False表示有不存在的
        missing_files: list, 不存在的文件路径列表
        corrupted_files: list,损坏的文件路径列表
    """
    media_fields = ["videos", "images", "audios"]
    missing_files = []
    corrupted_files = []
    mismatch_files = []

    # 查找 messages 中的占位符
    messages = item.get("messages", [])
    placeholders = {
        "videos": [msg["content"] for msg in messages if msg.get("role") == "user" and "<video>" in msg["content"]],
        "images": [msg["content"] for msg in messages if msg.get("role") == "user" and "<image>" in msg["content"]],
        "audios": [msg["content"] for msg in messages if msg.get("role") == "user" and "<audio>" in msg["content"]]
    }
    for field in media_fields:
        files = item.get(field, None)
        expected_placeholders = len(placeholders.get(field, []))
        if len(files) != expected_placeholders:
            mismatch_files.append({
                "type": field,
                "expected": expected_placeholders,
                "actual": len(files),
                "files": files
            })
        if files and isinstance(files, list):
            for f in files:
                if not os.path.exists(f):
                    missing_files.append({"type": field, "file": f})
                elif not check_file_valid(f,field):
                    corrupted_files.append({"type": field, "file": f})
                    
    if missing_files or corrupted_files:
        return False, missing_files, corrupted_files, mismatch_files
    return True, None, None, None

def check_file_valid(file_path, field):
    """ 
    检查单个文件是否有效。
    根据 field 自动选择检测方式（图片 / 视频 / 音频）。
    返回：
        bool: True 表示文件有效，False 表示文件损坏或无法读取。
    """
    try:
        if field == "images":
            with Image.open(file_path) as img:
                img.verify()
        elif field == "videos":
            cap = cv2.VideoCapture(file_path)
            if not cap.isOpened():
                return False
            ret, _ = cap.read()
            cap.release()
            if not ret:
                return False
        elif field == "audios":
            sf.info(file_path)
    except Exception:
        return False

    return True

def check_data_format(item, format):
    """
    检查单条数据中 assistant 回复的 think/answer 格式。
    参数:
        item: dict, 单条样本
        format: str, 指定检查格式 (think_answer / think / no_think)
    返回:
        ok: bool, True 表示全部符合格式
        error_messages: list, 不符合格式的内容列表
    """
    messages = item.get("messages", [])
    if not messages:
        return True, None

    # 找出所有 assistant 回复
    assistant_msgs = [m["content"] for m in messages if m.get("role") == "assistant"]
    if not assistant_msgs:
        return True, None

    # 不同 think 格式的正则模式
    think_patterns = {
        "think_answer": re.compile(
            r"^<think>[\s\S]+?</think>\s*<answer>[\s\S]*?\\boxed\{.*?\}[\s\S]*?</answer>$"
        ),
        "think": re.compile(
            r"^<think>[\s\S]+?</think>[\s\S]*?\\boxed\{.*?\}[\s\S]*$"
        ),
        "no_think": re.compile(
            r"^<think>\s*</think>[\s\S]+$"
        )
    }

    if format not in think_patterns:
        raise ValueError(f"未知 think_format: {format}")

    pattern = think_patterns[format]
    invalid_msgs = []

    for msg in assistant_msgs:
        if not pattern.match(msg.strip()):
            invalid_msgs.append(msg)

    if invalid_msgs:
        return False

    return True
    
def check_multi_turn_think_format(item):
    """
    仅检查 conversations 列表：
    - 第一个消息必须是 user
    - 最后一个消息必须是 assistant
    其他内容不做检查。
    
    Returns:
        tuple[bool, str | None]: (是否通过检查, 错误原因)
    """
    conversations = item.get('conversations', [])
    
    if not conversations or len(conversations) < 2:
        return False, "multi_turn_think_error: conversations 至少需要两个消息（user 和 assistant）"
    
    first_role = conversations[0].get('role')
    last_role = conversations[-1].get('role')
    
    if first_role != 'user':
        return False, f"multi_turn_think_error: 第一个消息的 role 必须是 user，但实际为 {first_role}"
    
    if last_role != 'assistant':
        return False, f"multi_turn_think_error: 最后一个消息的 role 必须是 assistant，但实际为 {last_role}"
    
    return True, None


# ========== 主检查函数 ==========

def check_data_content(item, dataset_name=None, src_path=None, error_log_path=None, **kwargs):  
    id = item.get("id", None)
    # 1. 多媒体文件存在性/有效性检查
    ok, missing_files, corrupted_files, mismatch_files = check_media_data(item)
    if not ok:
        if missing_files:
            record_error(item, src_path, id, "多媒体文件不存在", error_log_path, dataset_name=dataset_name, error_files=missing_files)
        if corrupted_files:
            record_error(item, src_path, id, "多媒体文件损坏", error_log_path, dataset_name=dataset_name, error_files=corrupted_files)
        if mismatch_files:
            record_error(item, src_path, id, "多媒体文件数量与占位符数量不一致", error_log_path, dataset_name=dataset_name, error_files=mismatch_files)
    
    #2. think格式检查
    if args.check_long_cot:
        ok = check_data_format(item,args.think_format)
        if not ok:
            record_error(item, src_path, id, "Long COT think格式不正确", 
                        error_log_path, dataset_name=dataset_name,)
        # return []
        
    #3. 多轮conversation检查
    if args.check_multi_turn_think:
        ok, fault = check_data_format(item)
        if not ok:
            record_error(item, src_path, id, fault, 
                        error_log_path, dataset_name=dataset_name,)
    return []


def record_error(item, src_path, id, reason, error_log_path, dataset_name=None, answer=None, error_files=None):
    # 记录不合格样本
    error_info = {
        "dataset": dataset_name,
        "id": id,
        "reason": reason,
        "file_path": src_path,
        "messages": item.get("messages", None),
        # "media_map": item.get("media_map", None),
        # "media_size": item.get("media_size", None),
    }
    if error_files is not None:
        error_info["error_files"] = error_files
    # if answer is not None:
    #     error_info["answer"] = answer
    save_jsonlines([error_info], error_log_path, mode="a")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--num_workers", type=int, default=32,
                      help="Number of worker processes for parallel data processing. Default is 8.")
    parser.add_argument("--datasets_path", type=str, required=True,)
    parser.add_argument("--error_log_path", type=str, default=None,
                      help="Path to save error log jsonl file.")
    parser.add_argument("--check_multi_turn_think", action='store_true',
                      help="Path to save error log jsonl file.")
    parser.add_argument("--check_long_cot", action='store_true',
                      help="Path to save error log jsonl file.")
    parser.add_argument("--think_format", type=str, default="think", options=["think_answer","think","no_think"],
                      help="Path to save error log jsonl file.")
    
    args = get_args(parser)


    os.makedirs(os.path.dirname(args.error_log_path), exist_ok=True)

    # 如果已存在，先清空
    if os.path.exists(args.error_log_path):
        with open(args.error_log_path, "w", encoding="utf-8") as f:
            pass
    
    if not args.check_long_cot:
        if args.think_format:
            log(f"警告: --think_format 参数只在启用 --check_long_cot 时有效，将被忽略", level=logging.WARNING)

    check_func = partial(check_data_content, error_log_path=args.error_log_path)

    process_data_hook(
        args.datasets_path,
        check_func,
        skip_processed_items=False,
        dst_metafiles_name="MetaFiles@Recheck_Tmp",
        num_workers=args.num_workers,
        parallel_type="thread"
    )

    # 检查是否有错误
    if os.path.exists(args.error_log_path):
        with open(args.error_log_path, "r", encoding="utf-8") as f:
            lines = [line for line in f if line.strip()]
        if len(lines) == 0:
            log("没有错误，所有数据均通过检查！")
        else:
            log(f"共发现 {len(lines)} 条不合格数据，详情见 {args.error_log_path}")
    else:
        log("没有错误，所有数据均通过检查！")