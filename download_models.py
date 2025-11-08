#!/usr/bin/env python3
"""
通用Hugging Face模型下载脚本
支持镜像站下载，断点续传，多线程加速
"""

import os
import sys
import requests
import json
from pathlib import Path
from typing import List, Dict, Optional
from concurrent.futures import ProcessPoolExecutor, as_completed
import multiprocessing as mp
import time
from tqdm import tqdm
import argparse

class HFModelDownloader:
    def __init__(self, mirror: str = "hf-mirror.com"):
        self.mirror = mirror
        self.base_url = f"https://{mirror}"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        
    def get_model_info(self, model_id: str) -> Dict:
        """获取模型信息"""
        api_url = f"{self.base_url}/api/models/{model_id}"
        try:
            response = self.session.get(api_url, timeout=30)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            print(f"获取模型信息失败: {e}")
            return {}
    
    def get_model_files(self, model_id: str) -> List[Dict]:
        """获取模型文件列表"""
        api_url = f"{self.base_url}/api/models/{model_id}/tree/main"
        try:
            response = self.session.get(api_url, timeout=30)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            print(f"获取文件列表失败: {e}")
            return []
    
    def download_file(self, model_id: str, filename: str, local_dir: str, 
                     chunk_size: int = 8*1024*1024, max_retries: int = 3) -> bool:
        """下载单个文件"""
        # 为每个进程创建独立的session对象
        session = requests.Session()
        session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        
        local_path = Path(local_dir) / filename
        local_path.parent.mkdir(parents=True, exist_ok=True)
        
        # 构建下载URL
        url = f"{self.base_url}/{model_id}/resolve/main/{filename}"
        
        # 智能断点续传检查
        resume_pos = 0
        use_resume = False
        
        if local_path.exists():
            file_size = local_path.stat().st_size
            
            # 检查文件是否完整（通过HEAD请求获取远程文件大小）
            try:
                head_response = session.head(url, timeout=10)
                remote_size = int(head_response.headers.get('content-length', 0))
                
                if file_size >= remote_size:
                    # 文件已完整下载
                    print(f"✅ {filename} 已存在且完整，跳过下载")
                    return True
                elif file_size > 1024 * 1024:  # 只对大文件使用断点续传
                    resume_pos = file_size
                    use_resume = True
                    print(f"🔄 {filename} 断点续传: {file_size}/{remote_size} bytes")
            except Exception as e:
                # HEAD请求失败，删除文件重新下载
                print(f"⚠️ {filename} HEAD请求失败: {e}，重新下载")
                try:
                    local_path.unlink()
                except:
                    pass
        
        headers = {}
        if use_resume and resume_pos > 0:
            headers['Range'] = f'bytes={resume_pos}-'
        
        for attempt in range(max_retries):
            try:
                response = session.get(url, headers=headers, stream=True, timeout=60)
                response.raise_for_status()
                
                # 处理断点续传
                
                total_size = int(response.headers.get('content-length', 0)) + resume_pos
                
                with open(local_path, 'ab' if resume_pos > 0 else 'wb') as f:
                    # 简化进度显示，只显示文件名和大小
                    desc = f"{filename[:30]}..." if len(filename) > 30 else filename
                    with tqdm(total=total_size, initial=resume_pos, unit='B', 
                             unit_scale=True, desc=desc, leave=False, 
                             bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]') as pbar:
                        for chunk in response.iter_content(chunk_size=chunk_size):
                            if chunk:
                                f.write(chunk)
                                pbar.update(len(chunk))
                
                print(f"✅ {filename} 下载完成")
                return True
                
            except Exception as e:
                print(f"❌ {filename} 下载失败 (尝试 {attempt + 1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)  # 指数退避
                else:
                    # 最后一次尝试失败，删除可能损坏的文件
                    try:
                        local_path.unlink()
                    except:
                        pass
        
        # 关闭session
        session.close()
        return False
    
    def download_model(self, model_id: str, local_dir: str = None, 
                      max_workers: int = 4, include_patterns: List[str] = None,
                      exclude_patterns: List[str] = None) -> bool:
        """下载整个模型"""
        # 提取模型名称（去掉组织名）
        model_name = model_id.split('/')[-1]
        
        if local_dir is None:
            local_dir = f"./models/{model_name}"
        else:
            # 如果指定了local_dir，在下面创建模型名称子目录
            local_dir = f"{local_dir}/{model_name}"
        
        local_path = Path(local_dir)
        local_path.mkdir(parents=True, exist_ok=True)
        
        print(f"🚀 开始下载模型: {model_id}")
        print(f"📁 保存到: {local_path.absolute()}")
        
        # 获取文件列表
        files = self.get_model_files(model_id)
        if not files:
            print("❌ 无法获取文件列表")
            return False
        
        # 过滤文件
        download_files = []
        for file_info in files:
            filename = file_info.get('path', '')
            if not filename or file_info.get('type') == 'directory':
                continue
                
            # 应用包含/排除模式
            if include_patterns and not any(pattern in filename for pattern in include_patterns):
                continue
            if exclude_patterns and any(pattern in filename for pattern in exclude_patterns):
                continue
            
            download_files.append(filename)
        
        print(f"📄 需要下载 {len(download_files)} 个文件")
        
        # 多进程下载
        # 限制最大进程数，避免创建过多进程
        actual_workers = min(max_workers, len(download_files), mp.cpu_count())
        print(f"🔄 使用 {actual_workers} 个进程并行下载")
        
        # 创建进度跟踪
        completed_files = []
        failed_files = []
        
        with ProcessPoolExecutor(max_workers=actual_workers) as executor:
            future_to_file = {
                executor.submit(self.download_file, model_id, filename, str(local_path)): filename
                for filename in download_files
            }
            
            # 使用更清晰的进度显示
            with tqdm(total=len(download_files), desc="总体进度", unit="文件") as pbar:
                for future in as_completed(future_to_file, timeout=3600):  # 1小时超时
                    filename = future_to_file[future]
                    try:
                        result = future.result(timeout=300)  # 5分钟单个文件超时
                        if result:
                            completed_files.append(filename)
                            pbar.set_postfix({"成功": len(completed_files), "失败": len(failed_files)})
                        else:
                            failed_files.append(filename)
                            pbar.set_postfix({"成功": len(completed_files), "失败": len(failed_files)})
                        pbar.update(1)
                    except Exception as e:
                        failed_files.append(filename)
                        print(f"\n❌ {filename} 下载异常: {e}")
                        pbar.set_postfix({"成功": len(completed_files), "失败": len(failed_files)})
                        pbar.update(1)
        
        success_count = len(completed_files)
        print(f"🎉 下载完成! 成功: {success_count}/{len(download_files)}")
        
        # 如果有失败的文件，显示它们
        if failed_files:
            print(f"❌ 以下文件下载失败:")
            for filename in failed_files:
                print(f"   - {filename}")
        
        return success_count == len(download_files)

def main():
    parser = argparse.ArgumentParser(description="通用Hugging Face模型下载脚本")
    parser.add_argument("--model_id", help="模型ID，如: Qwen/Qwen2.5-VL-7B-Instruct")
    parser.add_argument("--local-dir", help="本地保存目录")
    parser.add_argument("--mirror", default="hf-mirror.com", help="镜像站点")
    parser.add_argument("--max-workers", type=int, default=4, help="最大并发数")
    parser.add_argument("--include", nargs="*", help="包含的文件模式")
    parser.add_argument("--exclude", nargs="*", help="排除的文件模式")
    
    args = parser.parse_args()
    
    downloader = HFModelDownloader(mirror=args.mirror)
    
    model_id = args.model_id
    
    success = downloader.download_model(
        model_id=model_id,
        local_dir=args.local_dir,
        max_workers=args.max_workers,
        include_patterns=args.include,
        exclude_patterns=args.exclude
    )
    
    if success:
        print("🎉 模型下载成功!")
    else:
        print("❌ 模型下载失败!")
        sys.exit(1)

if __name__ == "__main__":
    main()

# python download_models.py --model_id Qwen/Qwen2.5-VL-7B-Instruct --local-dir /home/cike/pre-trained --max-workers 4

# python download_models.py --model_id OpenGVLab/InternVL3_5-8B --local-dir /home/cike/pre-trained --max-workers 4


# python download_models.py --model_id lmms-lab/LLaVA-OneVision-1.5-8B-Instruct --local-dir /home/cike/pre-trained --max-workers 3