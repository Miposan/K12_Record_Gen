import json
import requests

from datatool.logger import log
from datatool.apis.base_api import BaseAPI

class VLLM_ZHIPUAPI(BaseAPI):
    """在`推理服务`部署的 VLLM API"""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        assert self.url is not None, f'VLLM models must specify url such as http://172.18.64.41:5002/v1/chat/completions'

    def is_alive(self):
        """测试 api 是否存活"""
        ret = self.generate(messages=[
            {
                "role": "user", 
                "content": [
                    {"type": "text", "text": "hello"}
                ]
            }
        ])
        log(f"is_alive: {ret}")
        if ret == self.fail_msg:
            return False
        return True

    def generate_inner(self, messages):
        headers = {
            'Content-Type': 'application/json'
        }
        payload = {
            "model": self.model_key,
            "tools": [],
            "messages": messages,
            "stream": False,
            "do_sample": self.do_sample,
            "top_p": self.top_p,
            "top_k": self.top_k,
            "temperature": self.temperature,
            "repetition_penalty": self.repetition_penalty,
            "decoder_input_details": self.decoder_input_details,
            "n": self.n
        }

        if self.n == 1 and self.best_of is not None:
            payload["best_of"] = self.best_of
        
        payload = {k:v for k,v in payload.items() if v is not None}
        
        try:
            response = requests.post(self.url, headers=headers, data=json.dumps(payload), verify=False, timeout=self.timeout)
            if response.status_code == 200:
                if self.n > 1:
                    output = [choice['message']['content'] for choice in response.json()['choices']]
                else:
                    output = response.json()['choices'][0]['message']['content']
                
                return 0, output
            else:
                log(f"Received bad status code: {response.status_code}")
                return -1, ""
        except Exception as e:
            log(messages)
            log(f'Error {e}')
            return -1, ""

if __name__ == "__main__":
    api = VLLM_ZHIPUAPI(
        url="http://172.20.76.122:5002/v1/chat/completions"
    )
    user_prompt = "A conversation between User and Assistant. The user provides one or more images and/or a question, and the Assistant interprets the visual and textual input to solve it. The Assistant first understands the intent, analyzes relevant details, and reasons through the solution. The reasoning process and final answer are enclosed within <think> </think> and <answer> </answer> tags, respectively, i.e., <think> reasoning based on images and text </think> <answer> final answer </answer>. The user's question is: according to our records we received four reviews from you during the month of march totaling how many words of copy"
    log(api.generate(messages=[
        {
            "role": "user", 
            "content": [
                {
                    "type": "image_url",
                    "image_url": {
                        "url": "file:/workspace/mmdoctor/datasets_platform/SingleImage/MMBench_TEST_CN_V11/medias/243.jpg"
                    }
                },
                {"type": "text", "text": "<|begin_of_image|><|image|><|end_of_image|>Hint: 下面的文章描述了一个实验。阅读文章，然后按照以下说明进行操作。\n\n艾琳将...e correct answer from the options above. \n"}
            ]
        }
    ]))
    # log(api.is_alive())