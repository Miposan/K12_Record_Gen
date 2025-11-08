import argparse

def add_api_args(parser):
    """增加 API 调用参数"""
    group = parser.add_argument_group("API 调用参数")
    group.add_argument('--url', type=str, default=None, help='API URL')
    group.add_argument("--api_type", type=str, default="one_api",
                       choices=[ "openrouter", "one_api", "chatanywhere", "zhipu", "vllm", "zhipu_vllm"]) # 目前只支持这几种形式，有需要的可以继续扩充
    group.add_argument('--model_key', type=str, default="doubao-1.5-pro-32k-250115", help='Model key')  # 模型的名称
    group.add_argument('--num_threads', type=int, default=1, help='Number of threads')
    group.add_argument('--max_retry', type=int, default=3, help='Maximum number of retries')
    group.add_argument('--retry_wait', type=int, default=1, help='Retry wait time')
    group.add_argument('--retry_sleep_secs', type=float, default=0.5, help='Retry sleep seconds')
    group.add_argument('--timeout', type=float, default=300, help='Request timeout')
    group.add_argument('--do_sample', action='store_true', help='Enable sampling')
    group.add_argument('--stream', action='store_true', help='Enable streaming')
    group.add_argument('--max_tokens', type=int, default=1024, help='Maximum number of tokens')
    group.add_argument('--temperature', type=float, default=0.8, help='Temperature for sampling')
    group.add_argument('--top_p', type=float, default=0.01, help='Top-p sampling')
    group.add_argument('--top_k', type=float, default=1, help='Top-k sampling')
    group.add_argument('--best_of', type=int, default=1, help='Best of')
    group.add_argument('--repetition_penalty', type=float, default=1.0, help='Repetition penalty')
    group.add_argument('--frequency_penalty', type=float, default=None, help='Frequency penalty')
    group.add_argument('--truncate', type=int, default=None, help='Truncate length')
    group.add_argument('--tools', type=list, default=None, help='List of tools')
    group.add_argument('--decoder_input_details', type=bool, default=None, help='Decoder input details')
    group.add_argument('--n', type=int, default=1, help='Rollout return output')
    group.add_argument('--ports', type=str, default=None, help='Ports')
    group.add_argument('--port', type=str, default=None, help='Single port for zhipu inference service')
    return parser

def add_config_args(parser):
    """增加 Config 配置参数"""
    group = parser.add_argument_group("Config 配置参数")
    return parser

def get_args(parser=None):
    """解析所有参数"""
    if parser is None:
        parser = argparse.ArgumentParser(description='post-training-toolkits')
    else:
        assert isinstance(parser, argparse.ArgumentParser)
    parser = add_api_args(parser)
    parser = add_config_args(parser)
    args = parser.parse_args()
    return args