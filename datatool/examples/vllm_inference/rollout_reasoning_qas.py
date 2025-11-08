"""
功能：每个问题，rollout 出多个答案，并且对每次 rollout 的结果都判断回答对不对

输出：
在原数据的 metadata 中新增字段：
{
    "rollouts": [
        {
            "answer": "回答",
            "is_true": True / False / None
        },
        ...
    ],
    "hard_level": float  # 难度系数，范围[0.0, 1.0]，1表示最难
}
"""
import os
import re
import uuid
import pickle as pkl

from glob import glob
from tqdm import tqdm
from copy import deepcopy
from omegaconf import OmegaConf

from datatool.logger import log
from datatool.config import gconfig
from datatool.arguments import get_args
from datatool.apis import get_api_model
from datatool.utils.data import process_data_hook, load_message_from_data, remove_media_tags


def model_verify(question, answer, model_answer):
    request_prompt = "如果问题存在多个答案，存在其中一个答案回答正确，或者漏掉回答，则认为回答错误，输出False。"
    response = api_judge_model.generate(messages=[
        {"role": "system", "content": gconfig.verify_short_qa_system_prompt},
        {"role": "user", "content": "问题：" + question + "\n原始回答：" + answer + "\n直接答案：" + model_answer}
    ])
    response = response.strip()
    if "True" in response:
        return True
    elif "False" in response:
        return False
    else:
        log("Invalid response: " + response, level=log.WARNING)
        return None

def calculate_hard_level(rollouts, method="pass_at_k"):
    """计算问题的难度系数
    
    Args:
        rollouts (list): rollout的结果列表，每个元素包含is_true字段
        method (str): 计算方法，目前支持：
            - "pass_at_k": 基于pass@k的比例计算
            - 可以在这里添加其他计算方法
    
    Returns:
        float: 难度系数，范围[0.0, 1.0]，1表示最难
    """
    if method == "pass_at_k":
        # 统计正确答案的比例
        true_count = sum(1 for r in rollouts if r["is_true"] is True)
        total_valid = sum(1 for r in rollouts if r["is_true"] is not None)
        
        if total_valid == 0:
            return 0.5  # 如果没有有效的验证结果，返回中等难度
        
        # 计算正确率并转换为难度系数
        correct_ratio = true_count / total_valid
        hard_level = 1.0 - correct_ratio  # 正确率越低，难度越高
        
        return hard_level
    else:
        raise ValueError(f"Unsupported method: {method}")


def rollout_answers(item, **kwargs):
    try:
        sep_token = '<|sep|>'
        vllm_system_prompt = f"请通过推理得到最后的答案，输出格式要求：过程格式不限，但如果题目有最终答案，如果这条题目只有一个问题的，则请在最后用latex格式的\\boxed{{}}标注出来最终的答案。\
            如果这条题目存在多个小题的，请在最后用`{sep_token}` 作为答案分隔，并加上 `<|begin_of_box|>` 和 `<|end_of_box|>`，并且这些答案不要用\\boxed{{}}包裹了，比如：<|begin_of_box|>第一个问题答案{sep_token}第二个问题答案<|end_of_box|>\
            如果是一个问题存在两个答案的，A或B，可以直接用一个\\boxed{{}}包裹，比如：\\boxed{{答案1或答案2}}"
        messages, _ = load_message_from_data(item,
                                             load_media=True)
        # TODO 不知道为什么直接传{'role': 'system', 'content': '请通过推理得到最后的答案，输出格式要求：过程格式不限...'}会报错的？？？
        # 加多个输出要求
        messages[-1]["content"].append({"type": "text", "text": vllm_system_prompt})
        rollouts = []
        if args.rollout_times > 1:
            response_list = api_model.generate(messages)  # 一次性rollout多个出来
        else:
            response_list = [api_model.generate(messages)]
        for rollout_idx, response in enumerate(response_list):
            std_answer = extract_boxed_answer(item["messages"][-1]["content"], api_extract_model)
            model_ans = extract_boxed_answer(response, api_extract_model) if response != api_model.fail_msg else ""
            if response != api_model.fail_msg:
                is_true_answer = model_verify(
                    remove_media_tags(item["messages"][-2]["content"]),
                    std_answer,
                    model_ans
                )
            else:
                is_true_answer = None
            rollouts.append({
                "response": response,
                "answer": model_ans,
                "is_true": is_true_answer
            })
        item["metadata"] = item.get("metadata", {})
        item["metadata"]["rollouts"] = rollouts
        # 计算难度系数
        item["metadata"]["hard_level"] = calculate_hard_level(rollouts)
        yield deepcopy(item)
    except Exception as e:
        log(f"Error processing item: {item.get('id', str(item))}, error: {e}", level=log.WARNING)
        raise

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--num_workers", type=int, default=96,
                        help="Number of worker processes for parallel data processing. Default is 1.")
    parser.add_argument("--datasets_path", type=str, default=None,
                        help="Path to the dataset configuration YAML file.")
    parser.add_argument("--save_meta_name", type=str, default=None, 
                        help="Directory name for saving metadata files. Default is None.")
    parser.add_argument("--rollout_times", type=int, default=8, 
                        help="Number of rollout times. Default is 8.")
    args = get_args(parser)
    args.n = args.rollout_times
    api_model = get_api_model(**vars(args))
    api_extract_model = get_api_model(api_type="one_api", model_key="deepseek-v3-0324")
    # api_judge_model = api_extract_model
    api_judge_model = api_extract_model  # 这里就用回同一个模型, 可以自行选别的模型
    # 加载数据配置
    dataset_config = OmegaConf.load(args.datasets_path)
    log(f"Load dataset yaml from {args.datasets_path}")
    # 处理
    process_data_hook(args.datasets_path,
                           rollout_answers,
                           dst_metafiles_name=args.save_meta_name,
                           num_workers=args.num_workers)
