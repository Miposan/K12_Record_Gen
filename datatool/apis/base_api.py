import time
import random as rd

from datatool.utils.wrappers import timer

class BaseAPI():
    fail_msg = "<RequestError></RequestError>"
    
    def __init__(
        self,
        url: str | None = None,
        model_key: str | None = None,
        num_threads: int = 1,
        max_retry: int = 3,
        retry_wait: int = 1,
        retry_sleep_secs: float = 0.5,
        timeout: float = 300,
        do_sample: bool = False,
        stream: bool = False,
        max_tokens: int = 1024,
        temperature: float = 0.8,
        top_p: float = 0.00001,
        top_k: float = 1,
        best_of: int = 1,
        n: int = 1,
        ports: str | None = None,
        repetition_penalty: float = 1.0,
        frequency_penalty: float | None = None,
        truncate: int | None = None,
        tools: list | None = None,
        decoder_input_details: bool | None = None,
        generate_thinking_process: bool = False,
        **_,
    ) -> None:
        """api configs"""
        self.model_key = model_key
        self.url = url
        self.num_threads = num_threads
        self.max_retry = max_retry
        self.retry_wait = retry_wait
        self.retry_sleep_secs = retry_sleep_secs
        self.timeout = timeout
        
        """ decoding parameters """
        self.do_sample = do_sample
        self.max_tokens = max_tokens
        self.temperature = temperature
        self.stream = stream
        self.top_p = top_p
        self.top_k = top_k
        self.best_of = best_of
        self.n = n
        self.ports = ports
        self.repetition_penalty = repetition_penalty
        self.frequency_penalty = frequency_penalty
        self.truncate = truncate
        self.tools = tools
        self.decoder_input_details = decoder_input_details
        self.generate_thinking_process = generate_thinking_process

    def is_alive(self):
        """测试 api 是否存活"""
        ret = self.generate(messages=[
            {
                "role": "user", 
                "content": "hello"
            }
        ])
        print(f"is_alive: {ret}")
        if ret == self.fail_msg:
            return False
        return True
        
    @NotImplementedError
    def generate_inner(self, messages):
        """get responses by api
        Return:
            ret_code (int): status code returned by api
                - 0: success
                - other: error
            answer (str): response
        """
        pass

    @timer(prefix="generate")
    def generate(self, messages):
        for i in range(self.max_retry):
            if i>0:
                T = rd.random() * self.retry_wait * 2
                time.sleep(T)
            try:
                ret_code, answer = self.generate_inner(messages=messages)
                if ret_code == 0:
                    return answer
                else:
                    print(f"Request error, retry ({i}/{self.max_retry})")
            except Exception as e:
                print(f"Request error, retry ({i}/{self.max_retry}): {e}")

        return self.fail_msg

    def __call__(self, messages):
        return self.generate(messages)