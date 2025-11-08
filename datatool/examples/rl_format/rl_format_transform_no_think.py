"""
功能说明：
本脚本用于将RL任务数据中的assistant答案，转换为指定的标准格式。转换后，答案部分会被抽取并格式化，保留完整的思考过程，并根据题型自动处理答案标签和分隔方式。
输出的assistant为：<think></think><answer>xxxxxx</answer>  (<think></think>里面内容滞空). 最后答案的格式为: \\boxed{xxxxxx}（单个答案） 或者 <|begin_of_box|>xxxxxx{sep}xxxxx<|end_of_box|>（多个答案）
主要功能：
- 读取原始RL任务数据，自动识别题目和原始assistant答案。
- 按照预设规则，将答案和思考过程格式化到<answer>标签内，自动处理多子问题/多答案的分隔与包裹方式。
- 支持自动评分（难度、质量、思考深度，预留接口）。
- 输出数据时，在metadata中新增/更新以下字段：
    {
        "original_answer": "原始答案",
        "RL_gt": "格式化后的答案",
        "is_rule_based": true/false,
        "hard_level": 分数,
        "quality": 分数,
        "thinking_depth": 分数,
        "think": "思考过程",
        "answer": "最终答案"
    }
"""

import os
import re
from copy import deepcopy
from omegaconf import OmegaConf
from collections import defaultdict
from datatool.logger import log
from datatool.arguments import get_args
from datatool.utils.data import process_data_hook, load_message_from_data
from functools import partial
from datatool.utils.format_check import extract_boxed_answer, extract_think_and_answer

# RL格式转换prompt
SYSTEM_PROMPT_TEMPLATE = '''
## 任务
请充当模型答案格式转换助手，我会提供一道任务题和原始思考过程答案，你的任务是将原始答案转换成指定的格式，**保留所有思考过程**，并严格按照以下要求处理答案部分：

- **所有思考过程都要保留，并全部放在 `<answer></answer>` 标签内。**
- **每当出现最终答案时，请用 `\\boxed{{答案}}` 的格式包裹该答案。**
- **如果题目包含多个独立子问题（如有多个空格需要填写，或题干明确分为多问），请将每个子问题的答案不再使用 `\\boxed{{}}` 包裹，而是用 `{sep}` 作为分隔符，并且最外层加上 `<|begin_of_box|>` 和 `<|end_of_box|>`。**
- **如果只是一个问题但有多个答案（如"列举/选择多个内容"），请将所有答案都放在同一个 `\\boxed{{}}` 里，不要用 `{sep}` 分隔。**
- **请注意，最终答案不一定只在最后一句出现，可能在思考过程中间就有。出现最终答案就用 `\\boxed{{}}` 包裹。前面包裹过的答案，后面请不要再重复包裹！！！**
- **严格遵循 `<answer></answer>` 的固定格式！！（即开始 tag 和结束 tag）**
- **您的回答必须只包含转换后的结果。**
## 示例：

例1（单问题）:
```
<answer>首先我们需要根据题干给出的条件分析角AOB的度数。已知条件是........经过计算可得，∠AOB 的度数为 \\boxed{{68°}}。</answer>
```

例2（单问题）:
```
<answer>题目要求写出煮沸的产物。分析水煮沸的过程，水分子获得能量后会变成水蒸气，同时也会有部分水分子直接逸出。综上，煮沸的产物有水分子和水蒸气，所以答案是 \\boxed{{水分子和水蒸气}}</answer>
```

例3（多个问题）:
```
<answer>三棱柱的定义是根据几何知识，三棱柱是......所以这是一个三棱柱，而这个三棱柱的体积根据......计算可得答案是15cm^3所以这道题目的答案是<|begin_of_box|>三棱柱{sep}15cm^3<|end_of_box|></answer>
```

例4（单问题）:
```
<answer>题目问下列哪些属于哺乳动物？猫、狗、蛇。首先分析猫和狗的生物学分类，它们都属于哺乳动物，而蛇属于爬行动物。综上，猫和狗属于哺乳动物，所以答案是 \\boxed{{猫或狗}}</answer>
```

**特别注意：**
- 只有当题目本身包含多个独立子问题（如有多个空格需要填写，或题干明确分为多问）时，才需要用 `{sep}` 作为答案分隔，并加上 `<|begin_of_box|>` 和 `<|end_of_box|>`，并且这些答案不要用\\boxed{{}}包裹了！！！。
- 只有当题目本身包含多个独立子问题时候，答案请在比较靠后的位置再统一使用<|begin_of_box|>第一个问题答案{sep}第二个问题答案<|end_of_box|>来进行输出。这种题目的答案，不要用\\boxed{{}}包裹了！！！
- 如果题目只是让你列举、选择或填写多个内容（如"xxx 或 xxx 或 xxx"），请直接将所有内容写在同一个 `\\boxed{{}}` 里，不要使用 `{sep}` 分隔。
- 如果这道题目只有一个问题，出现最终答案要用 `\\boxed{{}}` 包裹，且这些答案不一定只在最后一句出现。前面包裹过的答案，后面请不要再重复包裹！！！
- 思考的过程记得需要保留下来，放进去<answer></answer>里面

---
# 题目：
# {question}
原始答案与思考过程：
{original_answer}
'''

# TODO 可以自定义
TASK_CATEGORY = "reasoning"
TASK_STAGE = "RL"

def hard_level_score(think, answer):
    # TODO
    return None

def quality_score(think, answer):
    # TODO
    return None

def thinking_depth_score(think, answer):
    # TODO
    return None

def get_scores(think, answer):
    """计算各项评分"""
    hard_level = hard_level_score(think, answer)
    quality = quality_score(think, answer)
    thinking_depth = thinking_depth_score(think, answer)
    return hard_level, quality, thinking_depth


def transform_rl_format(item, **kwargs):
    """转换单条数据的格式"""
    update_conversation = kwargs.get("update_conversation", True)
    api_model = kwargs.get("api_model")
    answer_sep_token = kwargs.get("answer_sep_token", "[|]")

    # 加载数据
    messages, assistant_text = load_message_from_data(item,
                                                        system_prompt=None,
                                                        merge_answer=False,
                                                        load_media=False)
    if not assistant_text:
        return

    # 提取题目内容（假设第一个user为题目）
    question = None
    for msg in messages:
        if msg.get("role") == "user":
            content = msg["content"]
            if isinstance(content, list):
                for c in content:
                    if c.get("type") == "text":
                        question = c["text"]
                        break
                if question:
                    break
            elif isinstance(content, str):
                question = content
                break
    if not question:
        question = ""

    if "下列说法" in question or "以下说法" in question:
        return
    # 构建prompt
    system_prompt = SYSTEM_PROMPT_TEMPLATE.format(sep=answer_sep_token, question=question, original_answer=assistant_text)

    try:
        rl_format_response = api_model.generate([{"role": "user", "content": system_prompt}])
        if rl_format_response == api_model.fail_msg:
            return
        # 只保留<answer>...</answer>之间的内容，去除前后多余字符
        match = re.search(r"<answer>.*?</answer>", rl_format_response, re.DOTALL)
        if match:
            rl_format_response = match.group(0)
        else:
            # 如果没有匹配到，直接返回
            return
        # 提取think和answer
        think, answer = extract_think_and_answer(rl_format_response)
        if not answer:
            return
        if not think:
            think = None
        # 计算评分
        hard_level, quality, thinking_depth = get_scores(think, answer)
        # 更新metadata
        item["metadata"] = item.get("metadata", {})
        item["metadata"].update({
            "original_answer": assistant_text,
            "RL_gt": extract_boxed_answer(answer),
            "is_rule_based": True,
            "hard_level": hard_level,
            "quality": quality,
            "category": TASK_CATEGORY,
            "stage": TASK_STAGE,
            "thinking_depth": thinking_depth,
            "think": think,
            "answer": answer
        })
        # 是否更新原始conversation
        for conv in item["conversations"]:
            if conv.get("role") == "assistant":
                conv["text"] = "<think></think>" + rl_format_response
                break
        yield item
    except Exception as e:
        log(f"处理失败: {e}")
        return

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser()
    parser.add_argument("--num_workers", type=int, default=128,
                      help="Number of worker processes for parallel data processing. Default is 8.")
    parser.add_argument("--datasets_path", type=str, default=None,
                      help="Path to the dataset path file.")
    parser.add_argument("--save_meta_name", type=str, default="MetaFiles@RLFormat",
                      help="Directory name for saving metadata files.")
    parser.add_argument("--answer_sep_token", type=str, default="<|sep|>", help="Special token for splitting multiple answers.")
    
    args = get_args(parser)
    
    # 获取API模型
    from datatool.apis import get_api_model
    api_model = get_api_model(**vars(args))
    
    # 加载数据配置
    dataset_config = OmegaConf.load(args.datasets_path)
    log(f"Load dataset yaml from {args.datasets_path}")
    
    
    # 这里用partial包装
    transform_rl_format_hook = partial(
        transform_rl_format,
        api_model=api_model,
        answer_sep_token=args.answer_sep_token,
    )

    process_data_hook(
        args.datasets_path,
        transform_rl_format_hook,
        dst_metafiles_name=args.save_meta_name,
        num_workers=args.num_workers
    )

