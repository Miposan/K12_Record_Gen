import io
import base64
import requests
import numpy as np

from datatool.apis.base_api import BaseAPI
from datatool.logger import log

# TODO 需要修改未实现

class ClipEmbeddingAPI(BaseAPI):
    def __init__(self,
                 request_data_mode: str = 'tar_path',
                 **kwargs):
        super().__init__(**kwargs)
        self.inner_url = {
            # messages = {
            #     "tarpath": tar_path,
            #     "offset": offset,
            #     "media_name": media_name
            # }
            "tar_path": f"{self.url}/encode_image_from_tar",
            # messages = {
            #     "image_path": image_path
            # }
            "image_path": f"{self.url}/encode_image",
            # messages = {
            #     "image_base64": base64_str
            # }
            "base64": f"{self.url}/encode_image",
            # messages = {
            #     "text": text
            # }
            "text": f"{self.url}/encode_text"
        }[request_data_mode]
        self.request_data_mode = request_data_mode
    
    def is_alive(self):
        """测试 api 是否存活"""
        cache_url = self.inner_url
        self.inner_url = f"{self.url}/encode_text"
        ret = self.generate(messages={
            "text": "hello"
        })
        self.inner_url = cache_url
        if isinstance(ret, np.ndarray):
            log(f"is alive, embedding shape: {ret.shape}")
            return True
        return False

    def generate_inner(self, messages):
        headers = {
            "Content-Type": "application/json"
        }
        if self.request_data_mode == 'text':
            response = requests.post(self.inner_url, data=messages)
        else:
            response = requests.post(self.inner_url, headers=headers, json=messages)
        if response.status_code == 200:
            result = response.json()
            embedding_base64 = result['data']['embedding']
            # Decode the base64 string back to numpy array
            embedding_bytes = base64.b64decode(embedding_base64)
            embedding_array = np.load(io.BytesIO(embedding_bytes))
            return 0, embedding_array
        else:
            log(f"ClipEmbeddingAPI Request error, response: {response}")
            return -1, self.fail_msg
        

if __name__ == "__main__":
    clip_emb = ClipEmbeddingAPI(url="http://172.21.144.100:5001")
    clip_emb.is_alive()
    print(clip_emb.generate({
        "offset": 0,
        "media_name": "4bfa6204-5ea2-4155-b428-2a1ec7a05346.png"
    }))