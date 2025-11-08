import os
import re

from datatool.logger import log
from datatool.config import gconfig
from datatool.arguments import get_args
from datatool.apis import get_api_model
from datatool.utils.data import process_data_hook, load_message_from_data, remove_media_tags

def test_hook(item, **kwargs):
    try:
        messages = item.get("messages",[])
        user_msg = next((m["content"] for m in messages if m["role"] == "user"), None)
        new_item = {
            "id": item['id'],
            "messages": [
                {
                    "role": "user",
                    "content": user_msg
                },
                {
                    "role": "assistant",
                    "content": ""
                }
            ],
            # "videos": [videos],
            # "metadata": item['metadata'],
        }
        
        messages , answer = load_message_from_data(new_item)
        result = api_model.generate(messages=messages)
        if result == api_model.fail_msg:
            return
            # item["metadata"] = item.get("metadata", {}) or {}
        item["prediction"] = result
        yield item
    except BaseException as e:
        import traceback
        traceback.print_exc()
        log(f"Catch Error when processing item {item.get('uuid', '')}: {e}")

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--num_workers", type=int, default=96,
                        help="Number of worker processes for parallel data processing. Default is 1.")
    parser.add_argument("--datasets_path", type=str, default=None,
                        help="Path to the dataset configuration YAML file.")
    parser.add_argument("--save_meta_name", type=str, default=None, 
                        help="Directory name for saving metadata files. Default is None.")

       
    args = get_args(parser)
    args.datasets_path = "/home/gary/AI-Project/yjy/test/datatooltest/test.jsonl"
    args.save_meta_name = "/home/gary/AI-Project/yjy/test/datatooltest/test"
    args.max_tokens = 2048

    args.api_type = "sglang"
    args.model_key = "Qwen2.5-VL-3B-Instruct"
    args.url = "127.0.0.1"
    args.ports = "30000"
    api_model = get_api_model(**vars(args))
    
    process_data_hook(args.datasets_path,
                           test_hook,
                           dst_metafiles_name=args.save_meta_name,
                           num_workers=args.num_workers)