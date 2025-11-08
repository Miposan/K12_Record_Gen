import os
import random
from openai import OpenAI
import requests

from datatool.apis.base_api import BaseAPI


class VLLMAPI(BaseAPI):
    """"本地部署的vllm API"""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.port_num = 0
        print(self.ports)
        API_POOL = [int(x.strip()) for x in self.ports.split(",")]
        available_ports = []
        # 判断有多少个端口可用 
        for port in API_POOL:
            url = f"http://{self.url}:{port}/v1/models"
            try:
                resp = requests.get(url, timeout=100)
                if resp.status_code == 200 and "object" in resp.json():
                    print(f"✅ vLLM running on port {port}")
                    available_ports.append(port)
                    self.port_num += 1
                else:
                    print(f"⚠️  Port {port} responded but not as vLLM")
            except Exception as e:
                print(f"❌ No response on port {port}")

        self.api_pool = available_ports
        self.api_key = os.getenv("VLLM_API_KEY", None)
        if self.api_key is None:
            raise ValueError("VLLM_API_KEY is not set")
        self.clients = []

        for api_port in self.api_pool:
            api_base = f"http://{self.url}:{api_port}/v1"
            client = OpenAI(
                base_url = api_base,
                api_key = self.api_key,
                )
            self.clients.append(client)
        self.model_key = client.models.list().data[0].id

    def generate_inner(self, messages):
        n = random.randint(0, len(self.clients) - 1)
        try:
            response = self.clients[n].chat.completions.create(
                messages=messages,
                model=self.model_key if self.model_key else "default-lora",
                temperature=self.temperature,
                top_p=self.top_p,
                stream=False,
                max_tokens=self.max_tokens,
                timeout=5000,
                n=self.n
            )
            if self.n > 1:
                response = [choice.message.content for choice in response.choices]
            else:
                response = response.choices[0].message.content.strip()
            return 0, response
        except Exception as e:
            print(f'Error {e}')
            return -1, ""   