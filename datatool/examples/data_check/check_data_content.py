"""
功能说明：
检查数据集当中的内容

- 检查多媒体资源文件是否存在

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


def check_media_data(item):
    """
    检查item中的videos、images、audios字段（如存在且为列表），判断每个文件路径是否存在，不存在的文件全部返回。
    返回：
        ok: bool, True表示全部存在，False表示有不存在的
        missing_files: list, 不存在的文件路径列表
    """
    media_fields = ["videos", "images", "audios"]
    missing_files = []
    for field in media_fields:
        files = item.get(field, None)
        if files and isinstance(files, list):
            for f in files:
                if not os.path.exists(f):
                    missing_files.append({"type": field, "file": f})
    if missing_files:
        return False, missing_files
    return True, None


# ========== 主检查函数 ==========

def check_data_content(item, dataset_name=None, src_path=None, error_log_path=None, **kwargs):
    id = item.get("id", None)
    # 1. 多媒体文件存在性检查
    ok, missing_files = check_media_data(item)
    if not ok:
        record_error(item, src_path, id, "多媒体文件不存在", error_log_path, dataset_name=dataset_name, missing_files=missing_files)
        return []
    yield item

def record_error(item, src_path, id, reason, error_log_path, dataset_name=None, answer=None, missing_files=None):
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
    if missing_files is not None:
        error_info["missing_files"] = missing_files
    # if answer is not None:
    #     error_info["answer"] = answer
    save_jsonlines([error_info], error_log_path, mode="a")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--num_workers", type=int, default=32,
                      help="Number of worker processes for parallel data processing. Default is 8.")
    parser.add_argument("--datasets_path", type=str, default="/workspace/home/chenjiali/cjl-test-1/cjl-pck/data-process-toolkits/tmp_1.yaml")
    parser.add_argument("--error_log_path", type=str, default="./error_log.jsonl",
                      help="Path to save error log jsonl file.")
    args = get_args(parser)
    dataset_config = OmegaConf.load(args.datasets_path)
    log(f"Load dataset yaml from {args.datasets_path}")

    os.makedirs(os.path.dirname(args.error_log_path), exist_ok=True)

    # 如果已存在，先清空
    if os.path.exists(args.error_log_path):
        with open(args.error_log_path, "w", encoding="utf-8") as f:
            pass

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