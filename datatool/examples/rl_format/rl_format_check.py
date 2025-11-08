"""
功能说明：
本脚本用于批量检查RL（强化学习）格式数据的质量，自动筛查并记录不合格样本。

主要功能：
2. 检查answer字段是否符合 <think>...</think> 任意内容 <answer>...</answer> 的格式，且 <answer> 内容不能为空。
3. 检查answer内容是否包含 \boxed{...} 或 <|begin_of_box|>...<|sep|>...<|end_of_box|>，并校验分隔符数量等格式要求。
4. 支持多进程高效处理大规模数据。
5. 对所有不合格样本，记录数据集名称、id、错误原因、文件路径、conversation等关键信息到指定jsonl日志文件。
6. 检查结束后，若所有数据均通过，会在终端/日志中提示"没有错误，所有数据均通过检查！"。

输出说明：
- 所有不合格样本会被记录到 error_log_path 指定的jsonl文件，每条包含数据集名、id、错误原因、文件路径等信息。
- 若无任何错误，终端会输出"没有错误，所有数据均通过检查！"。

"""

import os
import re
from copy import deepcopy
from omegaconf import OmegaConf
from datatool.logger import log
from datatool.arguments import get_args
from datatool.utils.data import load_message_from_data, process_data_hook
from datatool.utils.file_io import save_jsonlines
from functools import partial


def check_thinking_answer_format(answer):
    """
    检查answer是否为<think>yyy</think>xxx<answer>xxxx</answer>格式，且xxxx非空
    允许<think>...</think>和<answer>...</answer>之间有任意内容
    """
    m = re.match(r'<think>(.*?)</think>.*?<answer>(.*?)</answer>', answer, re.DOTALL)
    if not m:
        return False, "answer不符合<think></think>...<answer></answer>格式"
    think, ans = m.group(1), m.group(2)
    if ans.strip() == "":
        return False, "<answer>内容为空"
    return True, None

def check_answer_content(answer):
    """
    检查answer内容是否有\boxed{}或<|begin_of_box|>xxx<|sep|>xxx<|end_of_box|>，分隔符数量合理
    """
    # 1. 有\boxed{...}
    if re.search(r'\\boxed\{.*?\}', answer, re.DOTALL):
        return True, None
    # 2. 有<|begin_of_box|>xxx<|sep|>xxx<|end_of_box|>
    m = re.search(r'<\|begin_of_box\|>(.*?)<\|end_of_box\|>', answer, re.DOTALL)
    if m:
        content = m.group(1)
        sep_count = content.count('<|sep|>')
        # 至少有一个<|sep|>
        if sep_count < 1:
            return False, "<|begin_of_box|><|end_of_box|>之间没有<|sep|>"
        # 检查答案数量和sep数量
        parts = content.split('<|sep|>')
        if len(parts) != sep_count + 1:
            return False, "<|sep|>数量与答案数量不符"
        return True, None
    return False, "answer没有\\boxed{}或<|begin_of_box|>...<|sep|>...<|end_of_box|>格式"

# ========== 主检查函数 ==========

def check_rl_data_format(item, dataset_name=None, src_path=None, error_log_path=None, **kwargs):
    id = item.get("id", None)
    # 2. answer格式检查
    try:
        # 用load_message_from_training_data导入
        _, answer = load_message_from_data(item, load_media=False)
    except Exception as e:
        record_error(item, src_path, id, f"load_message_from_data异常: {e}", error_log_path, dataset_name=dataset_name)
        return []
    ok, reason = check_thinking_answer_format(answer)
    if not ok:
        record_error(item, src_path, id, reason, error_log_path, dataset_name=dataset_name, answer=answer)
        return []
    # 3. answer内容格式检查
    ok, reason = check_answer_content(answer)
    if not ok:
        record_error(item, src_path, id, reason, error_log_path, dataset_name=dataset_name, answer=answer)
        return []
    # 合格样本不做记录
    return []

def record_error(item, src_path, id, reason, error_log_path, dataset_name=None, answer=None):
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
    # if answer is not None:
    #     error_info["answer"] = answer
    save_jsonlines([error_info], error_log_path, mode="a")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--num_workers", type=int, default=96,
                      help="Number of worker processes for parallel data processing. Default is 8.")
    parser.add_argument("--datasets_path", type=str, required=True)
    parser.add_argument("--error_log_path", type=str, default="datatool/logs/rl_data_check_errors.jsonl",
                      help="Path to save error log jsonl file.")
    args = get_args(parser)
    dataset_config = OmegaConf.load(args.datasets_path)
    log(f"Load dataset yaml from {args.datasets_path}")

    os.makedirs(os.path.dirname(args.error_log_path), exist_ok=True)

    # 如果已存在，先清空
    if os.path.exists(args.error_log_path):
        with open(args.error_log_path, "w", encoding="utf-8") as f:
            pass

    check_func = partial(check_rl_data_format, error_log_path=args.error_log_path)

    process_data_hook(
        args.datasets_path,
        check_func,
        dst_metafiles_name="tmp_test_MetaFiles",
        num_workers=args.num_workers
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