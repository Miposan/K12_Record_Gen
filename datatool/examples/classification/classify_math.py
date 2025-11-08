"""
对题目数据进行数学题和非数学题进行分类。

输出:
在原始数据的metadata当中增加新的字段
{
    "is_math": 0/1
}
0: 非数学题
1: 数学题
"""

import os
from copy import deepcopy
from omegaconf import OmegaConf
from datatool.logger import log
from datatool.arguments import get_args
from datatool.apis import get_api_model
from datatool.utils.data import process_data_hook, load_message_from_data

SYSTEM_PROMPT = """你是一个分类专家，以下是一些题目，请判断是否是数学题:
- **0**: 非数学题.
- **1**: 数学题. 

关于一些和物理知识（比如物理实验）还有化学生物相关的题目，请不要判断为数学题。物理的计算题可以判断为数学题

**Example**:
Question: 若关于x的一元二次方程x²+6x+c=0配方后得到(x+3)²=2c，则c的值为？
Classification: 1

Question: 以 $x^2 + 4y^2 = 12$ 的焦点为焦点，过直线 $l: x - y + 9 = 0$ 上的一点 $M$ 作椭圆，使椭圆的长轴最短。求椭圆的方程为_____
Classification: 1

Question: 下图为某地近五年水资源利用情况图，请分析变化趋势。
Classification: 0

Question: 如图14-3-1所示，冲击钻在工作过程中，其能量转化关系是_____.
Classification: 0

Question: 加入相同浓度的 $\\text{NaOH}$ 溶液各 40.00 mL，加热至 120℃左右，使氨气全部逸出（$(\\text{NH}_4)_2\\text{SO}_4$ 和 $\\text{NH}_4\\text{HSO}_4$ 固体的分解温度均高于 200℃），测得有关实验数据如下（标准状况）：\n实验过程中有关反应的离子方程式_________。
Classification: 0

**注意:只输出1或0，不需要任何解释或其他内容，否则你会受到严重的惩罚**

Now, classify the following question:
"""

def judge_math(item, **kwargs):
    """判断当前题目是否为数学类型题目"""
    if item["metadata"].get("type", None) == "计算题":
        item["metadata"]["is_math"] = 1
        yield deepcopy(item)
        return
    try:
        # 注意load_media=False，图像信息是不会get到的
        messages, _ = load_message_from_data(
            item,
            load_media=False,
            system_prompt=SYSTEM_PROMPT
        )
        
        try:
            result = api_model.generate(messages=messages)
            is_math = 0
            if result != api_model.fail_msg and result in ["0", "1"]:
                is_math = int(result.strip())
        except Exception as e:
            if any(x in str(e).lower() for x in ["不安全", "敏感内容", "unsafe", "sensitive"]):
                log(f"Skipped due to sensitive content: {e}")
            else:
                log(f"Error: {e}")
            is_math = 0

        item = deepcopy(item)
        item["metadata"] = item.get("metadata", {}) or {}
        item["metadata"]["is_math"] = is_math
        yield item
        
    except BaseException as e:
        import traceback
        traceback.print_exc()
        log(f"Catch Error when processing item {item.get('uuid', '')}: {e}")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--datasets_path", type=str, default=None,
                        help="Path to the dataset configuration dataset file.")
    parser.add_argument("--save_meta_name", type=str, default="MetaFiles@IsMath", 
                        help="Directory name for saving metadata files.")
    parser.add_argument("--num_workers", type=int, default=128,
                        help="Number of worker processes for parallel data processing.")
    args = get_args(parser)
    api_model = get_api_model(**vars(args))
    dataset_config = OmegaConf.load(args.datasets_path)
    log(f"Load dataset yaml from {args.datasets_path}")
    
    process_data_hook(args.datasets_path,
                judge_math,
                dst_metafiles_name=args.save_meta_name,
                num_workers=args.num_workers)