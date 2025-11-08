import os
import json
import requests
from datatool.logger import log
from pathlib import Path
from openai import OpenAI
from datatool.apis.base_api import BaseAPI

class OneAPI(BaseAPI):
    """微软代理的海外 API 接口(内部API)，详情见：https://zhipu-ai.feishu.cn/wiki/VtcWwErD1i2RJnknCe3cbUPMnjf"""    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.api_key = os.getenv("ONE_API_KEY", None)
        if self.api_key is None:
            raise ValueError("ONE_API_KEY is not set")
        self.client = OpenAI(api_key=self.api_key, base_url="https://api-gateway.glm.ai/v1")

    def generate_inner(self, messages):
        try:
            response = self.client.chat.completions.create(
                messages=messages,
                model=self.model_key,
                temperature=self.temperature,
                top_p=self.top_p,
                stream=False,
                max_tokens=self.max_tokens,
                n=self.n
            )
            response = response.choices[0].message
            answer = response.content.strip()
            # if self.model_key == "claude-3-7-sonnet-20250219-thinking" or self.model_key == "doubao-1.5-thinking-pro-vision-250415":
            if self.generate_thinking_process:
                if hasattr(response, "reasoning_content") and "<think>" not in answer and "</think>" not in answer and "<answer>" not in answer and "</answer>" not in answer:
                    log(f"Response has reasoning_content and <think></think><answer></answer> not in answer", level=log.WARNING)
                    answer = f"<think>{response.reasoning_content.strip()}</think><answer>{answer}</answer>"
                elif hasattr(response, "reasoning_content") and "<answer>" in answer and "</answer>" in answer:
                    log(f"Response has reasoning_content and <answer></answer> in answer", level=log.WARNING)
                    answer = f"<think>{response.reasoning_content.strip()}</think>{answer}"                    
                else:
                    log(f"Response has no reasoning_content or <think></think> has already in answer", level=log.WARNING)
            return 0, answer
        except Exception as e:
            log(f'Error {e}')
            return -1, ""

if __name__ == "__main__":
    from datatool.utils.file_io import encode_image_base64
    api = OneAPI(model_key="claude-3-7-sonnet-20250219-thinking",
                 generate_thinking_process=True,
                 max_tokens=8192)
    image_path = "./合成图片.png"
    print(api.generate(messages=[
        {
           "role": "system",
           "content": "请你根据用户输入的问题，在 thoughts 中详细理解用户的输入意图和阐述自己的思考过程。此外，满足以下几点要求：\n1. 如果问题是有确定性答案的，请将最终的结果提取出来放在 latex 的\\boxed{} 中。\n2. 请保持跟用户问题使用的语言一致。"
        },
        {
            "role": "user", 
            "content": [
                {
                    "type": "image_url",
                    "image_url": {
                        "url": f"data:image/jpeg;base64,{encode_image_base64(image_path)}"
                    }
                },
                {
                    "type": "text", "text": '请描述这张图片内容。'
                },
            ]
        }
    ]))