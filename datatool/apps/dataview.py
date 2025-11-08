"""
功能：浏览 unistore 标准格式数据

启动命令：
# 需要自己检查开发机是否打开了 8080 端口，用其他端口也行
# pip install streamlit
streamlit run pttools/apps/dataview.py --server.port 8080

# 在浏览器中打开 http://{服务器登录 ip 地址}:{端口号}/
"""
import base64
import json
import os
import random
import pickle
import collections
import struct
import traceback
import mmap
import pickle as pkl
import streamlit as st

from glob import glob
from omegaconf import OmegaConf
from datetime import datetime
from typing import List, Dict, Union

# Set page config
st.set_page_config(
    page_title="Document Viewer",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Custom CSS for styling
st.markdown("""
    <style>
    /* Light mode (default) variables */
    :root {
        --background-color: white;
        --border-color: #eee;
        --text-color: #333;
        --placeholder-bg: #f0f0f0;
        --placeholder-text: #666;
    }

    .document-card {
        padding: 1.5rem;
        border-radius: 0.5rem;
        background-color: #F5F5F5;
        color: var(--text-color);
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.2);
        margin: 0.5rem 1.5rem 3rem 1.5rem;
        border: 1px solid var(--border-color);
    }
    
    /* Ensure text inside document card uses the correct color */
    .document-card p, 
    .document-card span {
        color: var(--text-color);
    }
    
    .image-placeholder {
        cursor: pointer;
        transition: opacity 0.3s;
        background-color: var(--placeholder-bg) !important;
        color: var(--placeholder-text) !important;
        border: 1px solid var(--border-color) !important;
    }
    
    .image-container .image-info {
        color: var(--placeholder-text);
    }
    
    /* Update modal background for better contrast */
    .modal {
        background-color: rgba(0,0,0,0.95);
    }
    
    .modal {
        display: none;
        position: fixed;
        z-index: 1000;
        left: 0;
        top: 0;
        width: 100%;
        height: 100%;
        background-color: rgba(0,0,0,0.9);
    }
    .modal-content {
        margin: auto;
        display: block;
        position: absolute;
        top: 50%;
        left: 50%;
        transform: translate(-50%, -50%);
        max-width: 100%;
        max-height: 100%;
    }
    .close {
        position: absolute;
        right: 20px;
        top: 10px;
        color: #f1f1f1;
        font-size: 40px;
        font-weight: bold;
        cursor: pointer;
    }
    /* Adjust button alignment */
    div.stButton > button {
        margin-top: 4px;  /* Align with title */
    }
    /* Adjust spacing between title and content */
    .element-container:has(div.stMarkdown) {
        margin-top: -1rem;  /* Reduce space between title and card */
    }
    .toast {
        position: fixed;
        top: 20px;
        right: 20px;
        padding: 16px 24px;
        background-color: #4CAF50;
        color: white;
        border-radius: 4px;
        z-index: 1000;
        animation: slideIn 0.3s, fadeOut 0.5s 2.5s;
        opacity: 0;
    }
    
    @keyframes slideIn {
        from {
            transform: translateX(100%);
            opacity: 0;
        }
        to {
            transform: translateX(0);
            opacity: 1;
        }
    }
    
    @keyframes fadeOut {
        from {opacity: 1;}
        to {opacity: 0;}
    }
    </style>
    
    <div id="imageModal" class="modal">
        <span class="close">&times;</span>
        <div class="modal-content" id="modalContent"></div>
    </div>
    
    <script>
    // Modal functionality
    document.addEventListener('click', function(e) {
        if (e.target.classList.contains('image-placeholder')) {
            const modal = document.getElementById('imageModal');
            const modalContent = document.getElementById('modalContent');
            modalContent.innerHTML = `
                <div style="width:${e.target.dataset.originalWidth}px; 
                     height:${e.target.dataset.originalHeight}px; 
                     background-color:#f0f0f0; 
                     display:flex; 
                     align-items:center; 
                     justify-content:center;">
                    Original Image Size (${e.target.dataset.originalWidth}x${e.target.dataset.originalHeight})
                </div>`;
            modal.style.display = "block";
        }
    });
    
    // Close modal when clicking the X or outside the modal
    document.querySelector('.close').onclick = function() {
        document.getElementById('imageModal').style.display = "none";
    }
    
    window.onclick = function(e) {
        const modal = document.getElementById('imageModal');
        if (e.target == modal) {
            modal.style.display = "none";
        }
    }
    
    // Add this function to your existing JavaScript
    function showToast(message) {
        const toast = document.createElement('div');
        toast.className = 'toast';
        toast.textContent = message;
        document.body.appendChild(toast);
        
        // Remove the toast after animation
        setTimeout(() => {
            document.body.removeChild(toast);
        }, 3000);
    }
    
    function showFullSize(img, originalWidth, originalHeight) {
        const modal = document.getElementById('imageModal');
        const modalContent = document.getElementById('modalContent');
        modalContent.innerHTML = `<img src="${img.src}" style="max-width:90vw; max-height:90vh;">`;
        modal.style.display = "block";
    }
    
    // Add theme detection
    function updateTheme() {
        const isDark = window.matchMedia && window.matchMedia('(prefers-color-scheme: dark)').matches;
        document.body.setAttribute('data-theme', isDark ? 'dark' : 'light');
    }
    
    // Listen for theme changes
    window.matchMedia('(prefers-color-scheme: dark)').addListener(updateTheme);
    
    // Initial theme setup
    updateTheme();
    </script>
    """, unsafe_allow_html=True)

TarHeader = collections.namedtuple(
    "TarHeader",
    [
        "name",
        "mode",
        "uid",
        "gid",
        "size",
        "mtime",
        "chksum",
        "typeflag",
        "linkname",
        "magic",
        "version",
        "uname",
        "gname",
        "devmajor",
        "devminor",
        "prefix",
    ],
)


def parse_tar_header(header_bytes):
    """解析 tar 格式的文件头信息
    Args:
        header_bytes (bytes): header bytes, less than 500
    Returns:
        tar header info
    """
    assert len(header_bytes) <= 500
    header = struct.unpack("!100s8s8s8s12s12s8s1s100s6s2s32s32s8s8s155s", header_bytes)
    return TarHeader(*header)


def extract_data_from_tarfile(tar_path, offset):
    """根据偏移量从tar流中获取数据
    Args:
        tar_path (str): tar path
        offset (int): offset
    Returns:
        name, 
        data bytes
    """
    try:
        with open(tar_path, "rb") as stream:
            stream = mmap.mmap(stream.fileno(), 0, access=mmap.ACCESS_READ)
            header = parse_tar_header(stream[offset: offset + 500])
            name = header.name.decode("utf-8").strip("\x00")
            start = offset + 512
            end = start + int(header.size.decode("utf-8")[:-1], 8)
            return name, stream[start: end]
    except:
        print(f"Failed: {tar_path}, offset: {offset}")
        print(traceback.format_exc())


def format_date(date_str: str) -> str:
    """Format date string to a readable format."""
    try:
        date_obj = datetime.strptime(date_str, "%Y-%m-%d")
        return date_obj.strftime("%B %d, %Y")
    except:
        return date_str

def count_sample_nums(meta_dir):
    jsonl_files = [os.path.join(root, file) for root, _, files in os.walk(meta_dir) for file in files if file.endswith('.jsonl')]

    _sum = 0
    for file in jsonl_files:
        with open(file, 'r', encoding='utf-8') as f:
            _sum += sum(1 for _ in f)
    return _sum

def count_image_nums(tar_dir):
    index_files = [os.path.join(root, file) for root, _, files in os.walk(tar_dir) for file in files if file.endswith('.index')]

    image_set = set()
    for file in index_files:
        with open(file, 'rb') as f:
            tar_idx = pickle.load(f)
            for _, (_, img_hash) in tar_idx.items():
                image_set.add(img_hash)
    return len(image_set)
    

def sample_documents_from_dataset(metafile_path: str, tarfile_path: str, num_docs: int = 100) -> List[dict]:
    # Create a placeholder for logs in the sidebar
    log_placeholder = st.sidebar.empty()
    
    def update_log(message):
        log_placeholder.info(message)

    update_log("Reading tarfile indices...")
    index_path = os.path.join(tarfile_path, ".index", "index.pkl")
    with open(index_path, 'rb') as f:
        tar_idx = pickle.load(f)

    update_log("Scanning for JSONL files...")
    jsonl_files = [os.path.join(root, file) for root, _, files in os.walk(metafile_path) for file in files if file.endswith('.jsonl')]

    documents, tarmap, imgmap = [], {}, {}
    while num_docs > 0:
        random_file = random.choice(jsonl_files)
        update_log(f"Processing file: {os.path.basename(random_file)}")
        
        with open(random_file, 'r') as f:
            update_log("Counting total lines...")
            total_lines = sum(1 for _ in f)
            
            random_position = random.randint(0, total_lines - 1)
            update_log(f"Moving to position {random_position}/{total_lines}")
            f.seek(0)
            for _ in range(random_position):
                next(f)
    
            count = 0
            try:
                while count < num_docs:
                    update_log(f"Processing document {count + 1}/{num_docs}")
                    line = next(f)
                    doc = json.loads(line)
                    doc['metafile_path'] = random_file
                    doc['id_in_file'] = random_position + count
                    doc['tarfile_path'] = tarfile_path
                    doc['image_bytes'] = {}

                    update_log(f"Loading {len(doc['media_map'])} images...")
                    for k, v in doc['media_map'].items():
                        tarid, imageid = v
                        # get tarfile url
                        tarurl = tarmap.get(tarid, None)
                        if not tarurl:
                            tarurl = os.path.join(tarfile_path, tar_idx[tarid])
                            tarmap[tarid] = tarurl
                        # get image offset
                        imgdict = imgmap.get(tarid, None)
                        if not imgdict:
                            with open(tarurl.replace(".tar", ".index"), 'rb') as tar_f:
                                imgdict = pickle.load(tar_f)
                            imgmap[tarid] = imgdict
                        offset = imgdict[imageid][0]
                        # Store both image bytes and image ID
                        doc['image_bytes'][k] = {
                            'bytes': extract_data_from_tarfile(tarurl, offset)[1],
                            'imageid': imageid
                        }

                    documents.append(doc)
                    count += 1
            except (StopIteration, json.JSONDecodeError):
                update_log("Reached end of file or encountered invalid JSON")
                break
            finally:
                num_docs = num_docs - count
                if num_docs > 0:
                    update_log(f"Remaining documents to process: {num_docs}")

    update_log("Document processing complete!")
    return documents

def adjust_image_size(width: int, height: int, max_size: int = 512) -> tuple[int, int]:
    """Adjust image dimensions if they exceed max_size while maintaining aspect ratio."""
    if width <= max_size and height <= max_size:
        return width, height
    
    # Calculate the scaling factor based on the larger dimension
    scale = max_size / max(width, height)
    
    # Apply the same scale to both dimensions to maintain aspect ratio
    new_width = int(width * scale)
    new_height = int(height * scale)
    
    return new_width, new_height

def render_document_card(doc: dict):
    """Render a single document card."""
    with st.container():
        uuid = doc.get("uuid", "No UUID")
        
        # Escape UUID for the HTML tag
        safe_uuid = uuid.replace("<", "&lt;").replace(">", "&gt;")
        st.subheader(f"{safe_uuid}", anchor=False)
        
        # Escape metadata for the caption
        safe_metadata = f"line {doc['id_in_file']} in file: {doc['metafile_path']}".replace("<", "&lt;").replace(">", "&gt;")
        st.caption(safe_metadata)
        
        html_content = ['<div class="document-card">']
        
        for assistant_msg in doc["conversations"]:
            html_content.append(f"<p style='color: red;'><|{assistant_msg['role']}|></p>")

            content = assistant_msg["text"]
            
            for img_key, size in doc["media_size"].items():
                placeholder = f"<|ZP_MM_PLH={img_key}|>"
                if placeholder in content:
                    parts = content.split(placeholder)
                    
                    # Escape special characters in the text before the image
                    if parts[0]:
                        special_chars = ['#', '$', '*', '_', '`', '~', '|']
                        escaped_text = parts[0]
                        for char in special_chars:
                            escaped_text = escaped_text.replace(char, f"\\{char}")
                        html_content.append(f"<p>{escaped_text}</p>")
                    
                    # Get image data from the document
                    img_data = doc["image_bytes"].get(img_key)
                    if img_data:
                        # Convert bytes to base64
                        img_b64 = base64.b64encode(img_data['bytes']).decode()
                        adjusted_width, adjusted_height = adjust_image_size(size[1], size[0])
                        
                        html_content.append(
                            f'<div class="image-container" style="margin:10px 0;">'
                            f'<a href="data:image/jpeg;base64,{img_b64}" target="_blank">'
                            f'<img src="data:image/jpeg;base64,{img_b64}" '
                            f'width="{adjusted_width}" height="{adjusted_height}" '
                            f'style="cursor:pointer" '
                            f'title="Image ID: {img_data["imageid"]} ({size[0]}x{size[1]})" />'
                            f'</a>'
                            f'<div class="image-info" style="font-size: 0.8em;">'
                            f'Image ID: {img_data["imageid"]} • Original size: {size[0]}x{size[1]}'
                            f'</div>'
                            f'</div>'
                        )
                    else:
                        # Fallback to placeholder if image bytes are not available
                        html_content.append(
                            f'<div class="image-placeholder" '
                            f'style="width:{adjusted_width}px; height:{adjusted_height}px; '
                            f'background-color:#f0f0f0; display:flex; align-items:center; '
                            f'justify-content:center; margin:10px 0; border:1px solid #ddd;" '
                            f'data-original-width="{size[0]}" '
                            f'data-original-height="{size[1]}">'
                            f'<span>Image not available ({size[0]}x{size[1]})</span>'
                            f'</div>'
                        )
                    
                    content = parts[1] if len(parts) > 1 else ""
            
            # Escape any remaining content after all images
            if content:
                special_chars = ['#', '$', '*', '_', '`', '~', '|']
                for char in special_chars:
                    content = content.replace(char, f"\\{char}")
                html_content.append(f"<p>{content}</p>")
        
        # Close the card div
        html_content.append('</div>')
        
        # Render card content
        st.markdown(''.join(html_content), unsafe_allow_html=True)

def main():
    st.title("Dataset Random Walk")
    st.markdown("👀 Enter dataset path or select a predefined dataset to begin.")
    st.markdown("*Popup images may be blocked by your browser. If you see a blank page after clicking an image, right-click the image and select 'Open in new tab'.*")

    with st.sidebar:
        st.title("Dataset Selector")

        dataset_source = st.radio(
            "Dataset Source",
            ["Custom Path", "Predefined Config Yaml"]
        )
        
        if dataset_source == "Custom Path":
            # if dataset_source:
            meta_file_path = st.text_input(
                "MetaFile path",
                value="/path/to/your/MetaFiles"
            )
            tar_file_path = st.text_input(
                "TarFile path",
                value="/path/to/your/TarFiles"
            )
        else:
            yaml_file_path = st.text_input(
                "Yaml path",
                value="/path/to/your/data.yaml"
            )
            # 只有在输入路径后才尝试加载配置
            if yaml_file_path != "/path/to/your/data.yaml":
                try:
                    data_config = OmegaConf.load(yaml_file_path)["Datasets"]
                    categories = list(data_config.keys())

                    selected_category = st.selectbox("Select Category", categories)
                    
                    if selected_category:
                        meta_file_path = data_config[selected_category]["MetaFiles"]
                        tar_file_path = data_config[selected_category]["TarFiles"]
                except Exception as e:
                    st.error(f"Error loading YAML file: {e}")
                
        # Add number input without max limit
        num_samples = st.number_input(
            "Number of documents to show",
            min_value=1,
            value=10,
            step=1,
            help="Select how many documents to display on the page"
        )
        # Combine load and resample into a single button for custom paths
        resample = st.button("Load/Resample Documents", type="primary")
        if not resample:
            st.stop()  # Stop execution here if paths are not confirmed
    
    sample_nums = count_sample_nums(meta_file_path)
    image_nums = count_image_nums(tar_file_path)
    st.markdown(f"**Total number of documents / images: {sample_nums} / {image_nums} = {sample_nums / image_nums:.2f} documents per image**")
    st.markdown("---")

    # Load JSONL data using the appropriate path
    documents = sample_documents_from_dataset(meta_file_path, tar_file_path, num_samples)
    print(f"running: {datetime.now()}")
    # import ipdb; ipdb.set_trace()

    # Render all documents
    for doc in documents:
        render_document_card(doc)

if __name__ == "__main__":
    main()