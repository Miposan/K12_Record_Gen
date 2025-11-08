# datatool scipts

数据处理通用脚本，方便用户处理数据。

## 脚本说明

1. yaml格式生成(适配datahub数据查看)
```bash
python3 datatool/scripts/generate_datahubs_yaml.py --data_dir /workspace/common/cjl/datav202502/InstructionTuningByCategory@20250311 --save_path ./datahubs/InstructionTuningByCategory@20250311@NonRuleBased.yaml --metafile_name MetaFiles@non_rule_based
```

2. 数据集迁移
- 压缩数据集
scripts/zip_dataset.py，主要为迁移服务器做准备
这个脚本用于将分布在不同目录的数据集统一打包压缩，支持多线程并发处理和智能分卷压缩。生成的压缩包可以方便地在不同服务器间传输和部署。
```bash
python scripts/zip_dataset.py \
    --datasets_yaml configs/my_dataset.yaml \
    --save_dir /tmp/exports \
    --workers 16 \
    --need_deduplicate Ture #重复多媒体文件去重
```

- 数据集解压和路径重构
scripts/unzip_repath_dataset.py
这个脚本用于解压由 zip_dataset.py 生成的数据集压缩包，并将其中的相对路径转换为目标服务器上的绝对路径。支持单个zip文件和分卷zip文件的解压。
```bash
  python scripts/unzip_repath_dataset.py \
      --zip_source_dir /home/data/zips \
      --target_dir /data/extracted \
      --workers 12
```

3. 数据内容检查
- (1)检查多媒体资源文件是否都存在，以及多媒体文件是否有效
- (2)检查think_format 只允许使用 "think" / "no_think" / "think_answer" 三种传入格式规定，否则会 raise ValueError, 只在Long CoT当中才会检查（通过 --check_long_cot 启用，--think_format规定检查传入格式）

```
think_answer: <think>xxxx</think><answer>xxxx\\boxed{yy}</answer>
think: <think>xxxx</think>xxxx\\boxed{yyy}
no_think: <think></think>xxxx
```

- (3)多轮对话格式检查 通过--check_multi_turn_think启用

4. jsonl文件切分
- 分割所有JSONL文件，每个文件最多xxx个样本（当一个jsonl文件数据量过大时，可以分割成多个小份文件）
```bash
      python scripts/split_jsonl.py \
        --datasets_yaml configs/my_dataset.yaml \
        --max_samples xxx \
        --workers 8
```

5. 数据聚合dataaggregation
- scripts/data_aggregation/merge_into_single_metafiles/merge_dataset.py：将MetaFiles整合为一个新
- scripts/data_aggregation/merge_dataset.py：将source.yaml中的数据集复制到目标目录
- scripts/data_aggregation/reconstruct_dataset.py：重建和整合数据集，输入一个包含多个数据集的目录路径，自动遍历各数据集的MetaFiles目录