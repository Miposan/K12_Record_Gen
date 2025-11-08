python -m sglang.launch_server \
  --model-path /home/devdata/pre-trained/Qwen-Family/Qwen2.5-VL-3B-Instruct/Qwen/Qwen2___5-VL-3B-Instruct \
  --context-length 4096 \
  --pp-size 1 \
  --mem-fraction-static 0.85 \
  --disable-cuda-graph \
  --host 0.0.0.0 \
  --port 30000
