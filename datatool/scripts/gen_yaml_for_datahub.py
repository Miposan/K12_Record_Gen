"""
功能：生成数据合集的 yaml 配置文件，可用于 datahub 平台进行数据 preview
输入：数据集根目录路径
输出：将 yaml 配置文件保存在指定路径下

示例：
python3 datatool/scripts/generate_datahubs_yaml.py --data_dir /workspace/common/cjl/datav202502/InstructionTuningByCategory@20250311 --save_path ./single_image/datahubs/InstructionTuningByCategory@20250311@NonRuleBased.yaml --metafile_name MetaFiles@non_rule_based
"""
import os
import yaml
import argparse
from pathlib import Path

from glob import glob
from tqdm import tqdm

from datatool.utils.samples import stat_sample_nums
from datatool.config import gconfig

parser = argparse.ArgumentParser()

parser.add_argument("--data_dir", type=str, help="数据根目录路径")
parser.add_argument("--save_path", type=str, default=None, help="结果保存路径")
parser.add_argument("--metafile_name", type=str, default="MetaFiles", help="meta目录名")

args = parser.parse_args()

if args.save_path is None:
    args.save_path = gconfig.config_dir / f"datahubs/{os.path.basename(args.data_dir)}@{args.metafile_name}.yaml"

metafile_dirs = sorted(glob(os.path.join(args.data_dir, "**", args.metafile_name), recursive=True))

total_sample_nums = 0
final_config = {"DataDir": args.data_dir, "Datasets": {}}
for meta_dir in tqdm(metafile_dirs):
    relative_path = os.path.dirname(os.path.relpath(meta_dir, args.data_dir))
    if not relative_path:
        relative_path = args.data_dir.split('/')[-1]
    c_sample_nums = stat_sample_nums(meta_dir)
    total_sample_nums += c_sample_nums
    c_config = {
        # "TarFiles": os.path.join(os.path.dirname(meta_dir), args.tarfile_name),
        "MetaFiles": meta_dir,
        "sample_nums": c_sample_nums
    }
    final_config['Datasets'][relative_path] = c_config
final_config['TotalSampleNums'] = total_sample_nums

yaml_string = f"# dataset config for data center\n"
yaml_string += yaml.dump(final_config, allow_unicode=True, sort_keys=False)

# Save the processed YAML string to a file
save_path = Path(args.save_path)
save_path.parent.mkdir(parents=True, exist_ok=True)
with open(args.save_path, "w", encoding="utf-8") as file:
    file.write(yaml_string)