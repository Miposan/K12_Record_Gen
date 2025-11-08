import os
import random
import requests
import sys
sys.path.append("/workspace/home/chenjiali/cjl-test-1/cjl-pck/vlm-train-prod")
from swift.llm import InferRequest, InferClient, RequestConfig
from datatool.apis.base_api import BaseAPI
from datatool.utils.wrappers import timer
import time
from datatool.logger import log

class VLLM_SWIFTAPI(BaseAPI):
    """本地部署的 Swift vLLM API"""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.port_num = 0
        API_POOL = [int(x.strip()) for x in self.ports.split(",")]
        available_ports = []

        # 检测可用端口
        for port in API_POOL:
            try:
                engine = InferClient(host=self.url, port=port)
                if engine.models:
                    print(f"✅ Swift vLLM running on port {port}, models: {engine.models}")
                    available_ports.append(port)
                    self.port_num += 1
                else:
                    print(f"⚠️  Port {port} responded but no models found")
            except Exception:
                print(f"❌ No response on port {port}")

        if not available_ports:
            raise ValueError("No available Swift vLLM API ports found")

        self.api_pool = available_ports
        self.clients = [InferClient(host=self.url, port=port) for port in self.api_pool]
        self.model_key = None  # Swift 的 InferClient 不需要显式 model key
        self.request_config = RequestConfig(
            max_tokens=self.max_tokens,
            temperature=self.temperature,
            top_p=self.top_p,
            top_k=self.top_k,
            repetition_penalty=self.repetition_penalty,
            n=self.n,
        )
    
    def generate_inner(self, messages, images=[], videos=[]):
        num = random.randint(0, len(self.clients) - 1)
        client = self.clients[num]
        try:
            infer_request = InferRequest(messages=messages, images=images, videos=videos)
            response_list = client.infer([infer_request], self.request_config)
            if self.n > 1:
                response = [choice.message.content.strip() for choice in response_list[0].choices]
            else:
                response = response_list[0].choices[0].message.content.strip()
            
            # response = response_list[0].choices[0].message.content.strip()
            return 0, response
        except Exception as e:
            print(f"Error: {e}")
            return -1, ""
        
    @timer(prefix="generate")
    def generate(self, messages, images=[], videos=[]):
        for i in range(self.max_retry):
            if i>0:
                T = random.random() * self.retry_wait * 2
                time.sleep(T)
            try:
                ret_code, answer = self.generate_inner(messages=messages, images=images, videos=videos)
                if ret_code == 0:
                    return answer
                else:
                    print(f"Request error, retry ({i}/{self.max_retry})")
            except Exception as e:
                print(f"Request error, retry ({i}/{self.max_retry}): {e}")

        return self.fail_msg
