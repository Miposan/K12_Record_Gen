import os
import random
import requests
from openai import OpenAI  # sglang 内置兼容 OpenAI API 格式
from datatool.apis.base_api import BaseAPI


class SGLangAPI(BaseAPI):
    """本地部署的 SGLang API 封装，支持多端口自动检测与负载均衡"""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.port_num = 0
        print(f"Configured ports: {self.ports}")

        API_POOL = [int(x.strip()) for x in self.ports.split(",")]
        available_ports = []

        # --- 检测哪些端口上运行了 SGLang ---
        for port in API_POOL:
            url = f"http://{self.url}:{port}/v1/models"
            try:
                resp = requests.get(url, timeout=100)
                if resp.status_code == 200 and "object" in resp.json():
                    print(f"✅ SGLang running on port {port}")
                    available_ports.append(port)
                    self.port_num += 1
                else:
                    print(f"⚠️ Port {port} responded but not a valid SGLang API")
            except Exception as e:
                print(f"❌ No response on port {port} — {e}")

        if not available_ports:
            raise RuntimeError("No available SGLang ports found.")

        self.api_pool = available_ports
        self.api_key = os.getenv("SGLANG_API_KEY", None)
        print(self.api_key)
        if self.api_key is None:
            raise ValueError("SGLANG_API_KEY is not set")

        # --- 初始化多个客户端 ---
        self.clients = []
        for api_port in self.api_pool:
            api_base = f"http://{self.url}:{api_port}/v1"
            client = OpenAI(
                api_key=self.api_key,
                base_url=api_base,
            )
            if client is None:
                print("Client creation failed.")
            else:
                print(f"Client successfully created: {type(client)}")
            self.clients.append(client)


    def _get_model_id(self):
        """获取第一个模型的 ID"""
        try:
            client = self.clients[0]
            models = client.models.list()
            if models.data:
                return models.data[0].id
        except Exception as e:
            print(f"⚠️ Failed to get model list: {e}")
        return "default"

    def generate_inner(self, messages):
        """执行一次 SGLang 推理请求"""
        n = random.randint(0, len(self.clients) - 1)
        client = self.clients[n]
        try:
            response = client.chat.completions.create(
                messages=messages,
                model=self.model_key,
                temperature=self.temperature,
                top_p=self.top_p,
                max_tokens=self.max_tokens,
                stream=False,
                n=self.n,
                timeout=5000
            )

            if self.n > 1:
                response = [choice.message.content for choice in response.choices]
            else:
                response = response.choices[0].message.content.strip()

            return 0, response
        except Exception as e:
            print(f"Error during SGLang inference: {e}")
            return -1, ""
