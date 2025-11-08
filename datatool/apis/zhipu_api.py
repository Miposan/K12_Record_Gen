import os

from zhipuai import ZhipuAI
from datatool.apis.base_api import BaseAPI

class ZhipuAPI(BaseAPI): 
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.api_key = os.getenv("ZHIPU_API_KEY", None)
        if self.api_key is None:
            raise ValueError("ZHIPU_API_KEY is not set")
            
        print(f"[ZhipuAPI] Using API key: {self.api_key[:6]}******")

        self.client = ZhipuAI(api_key=self.api_key)
    
    def generate_inner(self, messages):
        try:
            response = self.client.chat.completions.create(
                messages=messages,
                model=self.model_key,
                temperature=self.temperature,
                top_p=self.top_p,
                stream=False,
                max_tokens=self.max_tokens
            )
            response = response.choices[0].message.content.strip()
            return 0, response
        except Exception as e:
            print(f'Error {e}')
            return -1, ""