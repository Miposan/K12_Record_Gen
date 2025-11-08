import os

from openai import OpenAI
from datatool.apis.base_api import BaseAPI

# TODOs

class OtherAPI(BaseAPI):    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.api_key = os.getenv("OTHER_API_KEY", None)
        if self.api_key is None:
            raise ValueError("OTHER_API_KEY is not set")
        self.client = OpenAI(api_key=self.api_key, base_url=self.url)

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

if __name__ == "__main__":
    api = OtherAPI(model_key="gpt-4o-2024-08-06")
    print(api.generate(messages=[
        {
            "role": "user", 
            "content": "hello"
        }
    ]))