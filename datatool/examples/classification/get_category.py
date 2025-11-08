"""
功能：给训练数据打分类标签，参考tools/configs/single_image/category_def.json

输出：
在原数据的 metadata 中新增字段：
{
    "category_from_model": "类别-子类别",
    "category_from_human": dataset_config中指定的类别或None
}
"""
import os
import re
import uuid

from datatool.logger import log
from datatool.config import gconfig
from datatool.arguments import get_args
from datatool.apis import get_api_model
from datatool.utils.data import process_data_hook, load_message_from_data

def classify_data(item, **kwargs):
    """对数据进行分类"""
    
    dataset_category = kwargs.get("dataset_category")
    
    if dataset_category is not None and "-" in dataset_category:
        category = dataset_category
    else:
        messages, _ = load_message_from_data(
            item, 
            load_media=True,
            system_prompt=gconfig.get_category_prompt()
        )
        if messages is None:
            category = "其他-其他"
        else:
            category = api_model.generate(messages)
            if category == api_model.fail_msg or \
                category not in set(gconfig.get_category_list()):
                log(f"Invalid category: {category} @ {item['id']}", level=log.WARNING)
                category = "其他-其他"
    
    item["metadata"] = item.get("metadata", {})
    item["metadata"].update({
        "category_from_model": category,
        "category_from_human": dataset_category
    })
    yield item

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--num_workers", type=int, default=1,
                        help="Number of worker processes for parallel data processing. Default is 1.")
    parser.add_argument("--datasets_path", type=str, default=None,
                        help="Path to the dataset file.")
    parser.add_argument("--save_meta_name", type=str, default="53_Category_50K_V1",
                        help="Directory name for saving metadata files. Default is 'CategoryMetaFiles'.")
    args = get_args(parser)
    api_model = get_api_model(**vars(args))
    
    # 处理
    process_data_hook(
        args.datasets_path or gconfig.instruction_data_yaml,
        classify_data,
        dst_metafiles_name=args.save_meta_name,
        num_workers=args.num_workers
    )
