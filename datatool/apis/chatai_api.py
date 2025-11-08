import os
import json
import requests
from datatool.logger import log
from pathlib import Path
from openai import OpenAI
from datatool.apis.base_api import BaseAPI

class ChatAIAPI(BaseAPI):   
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.api_key = os.getenv("CHATAI_API_KEY", None)
        if self.api_key is None:
            raise ValueError("CHATAI_API_KEY is not set")
        self.client = OpenAI(api_key=self.api_key, base_url="https://www.chataiapi.com/v1")

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
