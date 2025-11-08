import os
import yaml

from pathlib import Path

__all__ = ["gconfig"]

def process_value_inconfig(value, config_dir):
    """递归处理配置中的路径
    
    Args:
        value: 配置值，可能是字符串、字典或其他类型
        config_dir: 配置文件所在目录
    """
    if isinstance(value, dict):
        # 递归处理字典中的每个值
        return {k: process_value_inconfig(v, config_dir) for k, v in value.items()}
    elif isinstance(value, str):
        # 处理相对路径
        if value.startswith("./"):
            return Path(config_dir / value)
        # 处理绝对路径
        elif value.startswith("/"):
            return Path(value)
    return value

class _GlobalConfig():

    def __init__(self):
        _datatype = os.getenv("DATA_TYPE", "single_image")
        if os.getenv("CONFIG_DIR", None):
            _config_dir = Path(os.getenv("CONFIG_DIR")) / _datatype
        else:
            _config_dir = Path(__file__).parent / f"configs/{_datatype}"
        self._config = {
            "datatype": _datatype,
            "config_dir": _config_dir,
            "stage": os.getenv("DATA_STAGE", "@stage1"),
            "log_dir": os.getenv('LOG_DIR', Path(__file__).parent.parent / "logs")
        }
        # load config.yaml
        _config_path = _config_dir / "config.yaml"
        if _config_path.exists():
            with open(_config_path, "r") as f:
                _config = yaml.safe_load(f)
            for k,v in _config.items():
                # 处理 stage 的个性化配置
                if k.startswith("@stage"):
                    if k == self._config["stage"]:
                        for stage_k, stage_v in v.items():
                            self._config[stage_k] = process_value_inconfig(stage_v, _config_dir)
                else:
                    self._config[k] = process_value_inconfig(v, _config_dir)
        
    def __getattr__(self, name: str):
        """ 支持 gconfig.name """
        return self._config.get(name, None)

    def __getitem__(self, name: str):
        """ 支持 gconfig["name"] """
        return self._config.get(name, None)

    def __setitem__(self, name: str, value):
        """ 支持 gconfig["name"] = value """
        self._config[name] = value

    def get_category_def(self):
        from datatool.utils.file_io import load_json
        category_def = load_json(self._config["category_def"])
        return category_def

    def get_category_list(self):
        """获取 category 列表
        """
        category_list = list()
        category_def = self.get_category_def()
        for level1, level1_value in category_def.items():
            for level2, _ in level1_value.items():
                category_list.append(f"{level1}-{level2}")
        return category_list

    def get_category_prompt(self):
        """获取 category 分类器的 system prompt
        """
        category_def = self.get_category_def()
        category_desc_text = ""
        for level1, level1_value in category_def.items():
            category_desc_text += f"#### **{level1}**\n"
            for level2, level2_value in level1_value.items():
                category_desc_text += f"- **{level2}**：{level2_value['desc']}\n"
            category_desc_text += "\n"
        new_system_prompt = self.category_system_prompt.format(categorys=category_desc_text)
        return new_system_prompt

gconfig = _GlobalConfig()

if __name__ == "__main__":
    print(gconfig.get_category_list())
    print(gconfig.config_dir)
    print(gconfig.instruction_data_yaml)
    print(gconfig.get_category_prompt())
    print(gconfig["stage1"]["instruction_data_yaml"])
    print(gconfig.null)
    print(gconfig["InstructionTuningByCategory@20250311"]["filtered"])