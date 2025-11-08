import argparse

from .base_api import BaseAPI
from .openrouter_api import OpenrouterAPI
from .chatanywhere_api import ChatAnywhereAPI
from .zhipu_api import ZhipuAPI
from .other_api import OtherAPI
from .vllm_api import VLLMAPI
from .one_api import OneAPI
from .vllm_zhipu_api import VLLM_ZHIPUAPI
from .chatai_api import ChatAIAPI
from .silicon_api import SILICONAPI
from .wcode_api import WCodeAPI
# from .vllm_swift_api import VLLM_SWIFTAPI


def get_api_model(api_type, model_key, **kwargs):
    api_model = None
    if api_type == "openrouter":
        api_model = OpenrouterAPI(model_key=model_key, **kwargs)
    elif api_type == "chatanywhere":
        api_model = ChatAnywhereAPI(model_key=model_key, **kwargs)
    elif api_type == "zhipu":
        api_model = ZhipuAPI(model_key=model_key, **kwargs)
    elif api_type == "vllm":
        api_model = VLLMAPI(model_key=model_key, **kwargs)
    elif api_type == "one_api":
        api_model = OneAPI(model_key=model_key, **kwargs)
    elif api_type == "chatai_api":
        api_model = ChatAIAPI(model_key=model_key, **kwargs)
    elif api_type == "zhipu_vllm":
        api_model = VLLM_ZHIPUAPI(model_key=model_key, **kwargs)
    elif api_type == "silicon_api":
        api_model = SILICONAPI(model_key=model_key, **kwargs)
    elif api_type == "wcode_api":
        api_model = WCodeAPI(model_key=model_key, **kwargs)
    elif api_type == "swift_vllm":
        from .vllm_swift_api import VLLM_SWIFTAPI
        api_model = VLLM_SWIFTAPI(model_key=model_key, **kwargs)
    elif api_type == "sglang":
        from .sglang_api import SGLangAPI
        api_model = SGLangAPI(model_key=model_key, **kwargs)
    else:
        api_model = OtherAPI(model_key=model_key, **kwargs)
    
    # 测试 API 连接是否存活
    test_res = api_model.is_alive()
    if test_res:
        print(f"{api_type}@{model_key} test success!")
    else:
        print(f"{api_type}@{model_key} test failed!")
        return None
    return api_model