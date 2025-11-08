import io
import os
import json
import math
import uuid
import base64
import hashlib
import tarfile
import jsonlines
import pickle as pkl
import numpy as np
import pandas as pd
import dask.dataframe as dd
import webdataset as wds
from io import BytesIO
from urllib.parse import urlparse
import requests

from tqdm import tqdm
from glob import glob
from PIL import Image
from filelock import FileLock
from datatool.utils.constant import IMAGE_MIME_TYPES
from datatool.logger import log

def save_json(data, save_path, mode="w"):
    with open(save_path, mode) as fp:
        json.dump(data, fp, ensure_ascii=False)

def load_json(file_path):
    with open(file_path) as fp:
        return json.load(fp)

def save_jsonlines(data, save_path, mode="w"):
    if len(data) == 0:
        log(f"Warning: data is empty, skip saving for {save_path}")
        return
    os.makedirs(os.path.dirname(save_path), exist_ok=True)
    with jsonlines.open(save_path, mode) as fp:
        if isinstance(data, list):
            for c_data in data:
                fp.write(c_data)
        elif isinstance(data, dict):
            fp.write(data)
        else:
            raise TypeError("data must be list or dict")

def save_metafiles(data, output_dir, chunk_size=1000):
    for i in range(0, len(data), chunk_size):
        chunk = data[i:i + chunk_size]
        file_index = i // chunk_size
        output_file = os.path.join(output_dir, f"meta_{file_index:06d}.jsonl")
        save_jsonlines(chunk, output_file)

def save_jsonlines_mpi(new_data, file_path, mode="a"):
    # 创建文件锁，锁文件名可以是原文件名加上 `.lock`
    lock = FileLock(f"{file_path}.lock")

    # 使用文件锁，确保只有一个进程可以访问该文件
    with lock:
        save_jsonlines(new_data, file_path, mode)

def load_jsonlines(src_pathes):
    if isinstance(src_pathes, str):
        src_pathes = [src_pathes]
    data = []
    for src_path in src_pathes:
        try:
            with jsonlines.open(src_path) as fp:
                c_data = list(fp)
        except Exception as e:
            c_data = []
            with open(src_path) as fp:
                for line in fp.readlines():
                    line = line.strip()
                    if not line:  # 跳过空行
                        continue
                    try:
                        c_data.append(json.loads(line))
                    except Exception:
                        continue  # 跳过无法解析的行
        if len(c_data) == 0:
            continue  # 跳过空文件
        data.extend(c_data)
    return data


def load_pickle(file_path):
    try:
        with open(file_path, 'rb') as f:
            data = pkl.load(f)   
    except Exception as e:
        log(f"Error: {e}")
        data = None
    return data

def save_pickle(data, file_path, mode="w"):
    assert mode in ["w", "a"]
    if mode == "a" and os.path.exists(file_path):
        old_data = load_pickle(file_path)
        if old_data is None:
            old_data = [] if isinstance(data, list) else {}
        assert type(data) == type(old_data), f"The type of data in mode `a` must be the same as the old data. But find {type(data)} != {type(old_data)}"
        if isinstance(data, list):
            old_data.extend(data)
        elif isinstance(data, dict):
            old_data.update(data)
        else:
            raise TypeError("pickle append mode support: list or dict")
        data = old_data
    with open(file_path, mode="wb") as fp:
        pkl.dump(data, fp)

def save_pickle_mpi(new_data, file_path, mode="a"):
    # 创建文件锁，锁文件名可以是原文件名加上 `.lock`
    lock = FileLock(f"{file_path}.lock")

    # 使用文件锁，确保只有一个进程可以访问该文件
    with lock:
        save_pickle(new_data, file_path, mode=mode)


def is_url(path: str) -> bool:
    try:
        result = urlparse(path)
        return result.scheme in ("http", "https")
    except Exception:
        return False

def load_image_bytes(image_path):
    with open(image_path, "rb") as f:
        return f.read()

def save_image_bytes(img_bytes, dst_image_path):
    with open(dst_image_path, "wb") as fp:
        fp.write(img_bytes)

def load_media_size(image, reverse=True):
    """获取图像的大小

    Args:
        image (str or bytes or PIL.Image): 图像路径或图像字节或PIL.Image对象
        reverse (bool, optional): 是否颠倒顺序，变成先高再宽. Defaults to True.
        
    Returns:
        image.size: (width, height) if reverse else (height, width)
    """
    if isinstance(image, str):
        image = Image.open(image)
    elif isinstance(image, bytes):
        image = Image.open(io.BytesIO(image))
    elif isinstance(image, Image.Image):
        pass
    else:
        raise TypeError("image must be str or bytes or PIL.Image")
    assert len(image.size) == 2, f"length of {image.size} != 2"
    if reverse:
        # 颠倒顺序，变成先高再宽
        return (image.size[1], image.size[0])
    else:
        return image.size


def load_image_base64(image, new_width=None, new_height=None):
    """
    支持输入bytes、base64字符串、文件路径，并可选缩放图片。
    """
    # 先获取图片的二进制数据
    if isinstance(image, bytes):
        image_data = image
    elif isinstance(image, str) and image.startswith("data:image/"):
        image_data = base64.b64decode(image.split(",")[1])
    elif isinstance(image, str) and os.path.isfile(image):
        image_data = load_image_bytes(image)
    else:
        raise ValueError("Invalid image path or data.")

    # 如果需要缩放
    if new_width is not None or new_height is not None:
        with Image.open(io.BytesIO(image_data)) as img:
            orig_width, orig_height = img.size
            # 只指定一个时，等比例缩放
            if new_width is not None and new_height is None:
                ratio = new_width / orig_width
                new_height = int(orig_height * ratio)
            elif new_height is not None and new_width is None:
                ratio = new_height / orig_height
                new_width = int(orig_width * ratio)
            # 两个都指定则直接用
            img = img.resize((new_width, new_height), Image.LANCZOS)
            buf = io.BytesIO()
            img.save(buf, format=img.format if img.format else 'PNG')
            image_data = buf.getvalue()

    return base64.b64encode(image_data).decode('utf-8')


def load_parquet_file(file_path_list):
    image_dfs = []
    for file_path in file_path_list:
        image_df = dd.read_parquet(file_path, engine="pyarrow").compute()
        image_dfs.append(image_df)
    image_df = pd.concat(image_dfs)
    return image_df


def get_image_md5(image):
    """
    获取图像的 MD5 哈希值。

    Args:
        image (str or bytes): 图像的路径或图像的字节数据。

    Returns:
        str: 图像的 MD5 哈希值。
    """
    if isinstance(image, str):
        img_bytes = load_image_bytes(image)
    elif isinstance(image, bytes):
        img_bytes = image
    else:
        raise TypeError("image must be str or bytes")
    
    img_md5 = hashlib.md5(img_bytes).hexdigest()
    return img_md5


def detect_image_type(encoded_image):
    """根据 Bytes 检测图像类型

    Args:
        image_data (bytes): 图像的字节数据
        image_name (str, optional): 图像的名称. Defaults to None.
    """
    decoded_data = base64.b64decode(encoded_image)
    header = decoded_data[:8]  # 检查前8字节
    if header.startswith(b'\xFF\xD8\xFF'):
        return "jpg"
    elif header.startswith(b'\x89PNG\r\n\x1a\n'):
        return "png"
    elif header.startswith(b'GIF8'):
        return "gif"
    elif header.startswith(b'BM'):
        return "bmp"
    elif header.startswith(b'RIFF') and b'WEBP' in header:
        return "webp"
    elif header.startswith(b'\x49\x49\x2A\x00') or header.startswith(b'\x4D\x4D\x00\x2A'):
        return "tiff"
    else:
        return "unknown"


def load_image_base64_with_type(image_path, new_width=None, new_height=None):
    if is_url(image_path):
        resp = requests.get(image_path)
        resp.raise_for_status()
        img_bytes = resp.content
        encoded_img = load_image_base64(img_bytes, new_width, new_height)
        img_type = detect_image_type(encoded_img)
        return encoded_img, IMAGE_MIME_TYPES.get(img_type, 'image/jpeg')
    else:
        encoded_img = load_image_base64(image_path, new_width, new_height)
        img_type = detect_image_type(encoded_img)
        return encoded_img, IMAGE_MIME_TYPES.get(img_type, 'image/jpeg')


def load_video_bytes_base64(video_path):
    """将视频文件转换为base64编码"""
    if is_url(video_path):  
        with requests.get(video_path) as response:
            response.raise_for_status()
            return base64.b64encode(response.content).decode("utf-8")
    else:
        with open(video_path, 'rb') as video_file:
            return base64.b64encode(video_file.read()).decode('utf-8')



def load_audio_bytes_base64(audio_path):
    """将音频文件转换为base64编码"""
    if is_url(audio_path):
        with requests.get(audio_path) as response:
            response.raise_for_status()
            return base64.b64encode(response.content).decode("utf-8")
    else:
        with open(audio_path, 'rb') as audio_file:
            return base64.b64encode(audio_file.read()).decode('utf-8')