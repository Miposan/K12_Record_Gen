import re
from datatool.utils.constant import EXTRACT_ANSWER_PROMPT, HTML_TAGS
from deprecated import deprecated


def is_contains_chinese(strs):
    for _char in strs:
        if '\u4e00' <= _char <= '\u9fa5':
            return True
    return False

def match_any_in_paragraph(content, pattern=r'```(markdown)?\s*(.*?)\s*```'):
    matches = re.findall(pattern, content, re.DOTALL)
    if matches:
        return matches[0][1].strip()
    return content.strip()


def thinking_answer_format_check(answer):
    """判断回答是否符合格式：<think>**</think>**<answer>**\boxed{**}**</answer>"""
    pattern = r'^<think>.*?</think>.*?<answer>.*?\\boxed\{.*?\}.*?</answer>$'
    return re.match(pattern, answer, re.DOTALL) is not None


def extract_think_and_answer(rl_format_answer, think_format="think_answer"):
    """
    从格式化后的答案中提取 <think>，<answer> 内容
    think_format 只允许使用 "think" / "no_think" / "think_answer" 三种传入格式规定，否则会 raise ValueError

    以下给出三种格式的样例，使用 `THINK_PLH` & `ANSWER_PLH` 表示 think 和 answer 的内容
    think_answer: <think>{THINK_PLH}</think><answer>{ANSWER_PLH}</answer>
    no_think: <think></think>{ANSWER_PLH} (think tag 之间需为空)
    think: <think>{THINK_PLH}</think>{ANSWER_PLH}
    """
    # Compatible with the previous “think answer” format
    if think_format == "think_answer":
        think_match = re.search(r"<think>(.*?)</think>", rl_format_answer, re.DOTALL)
        answer_match = re.search(r"<answer>(.*?)</answer>", rl_format_answer, re.DOTALL)
        think = think_match.group(1).strip() if think_match else None
        answer = answer_match.group(1).strip() if answer_match else None
    elif think_format == "no_think":
        no_think_match = re.search(r"<think>(.*?)</think>(.*)", rl_format_answer, re.DOTALL)
        think = no_think_match.group(1).strip() if no_think_match else None
        answer = no_think_match.group(2).strip() if no_think_match else None
        assert not think, f"think should be empty in `no_think` mode, but extract `{think}` in `{rl_format_answer}`"
    elif think_format == "think":
        think_match = re.search(r"<think>(.*?)</think>(.*)", rl_format_answer, re.DOTALL)
        think = think_match.group(1).strip() if think_match else None
        answer = think_match.group(2).strip() if think_match else None
    else:
        raise ValueError(f"invalid think_format: {think_format}")
    
    return think, answer

@deprecated("use extract_boxed_content instead")
def find_boxed_content(text):
    """
    Extract content from \boxed{} with proper brace matching.
    
    Args:
        text (str): The text containing \boxed{} expressions
        
    Returns:
        list: List of extracted contents from \boxed{} expressions
    """
    if text is None:
        return None
    results = []
    i = 0
    while i < len(text):
        if text[i:i+7] == "\\boxed{":
            i += 7  # Move past "\boxed{"
            content = ""
            brace_count = 1  # We've already encountered one opening brace
            
            while i < len(text) and brace_count > 0:
                if text[i] == '{':
                    brace_count += 1
                elif text[i] == '}':
                    brace_count -= 1
                
                if brace_count > 0:  # Only add to content if we're still inside the main braces
                    content += text[i]
                i += 1
            
            if brace_count == 0:  # Successfully matched all braces
                results.append(content)
        else:
            i += 1
    if len(results) == 1:
        # If there's only one boxed content, return it
        return results[0].strip()
    else:
        return None

def find_html_tags(text):
    """找到所有非表格相关的标准html标签，返回标签列表"""
    # 排除table相关
    exclude = {'table', 'tr', 'td', 'th'}
    tags_pattern = '|'.join([tag for tag in HTML_TAGS if tag not in exclude])
    # 匹配开头或闭合标签
    pattern = rf'</?({tags_pattern})\b[^>]*>'
    return re.findall(pattern, text, flags=re.IGNORECASE)

def remove_html_tags(text):
    """去除所有非表格相关的标准html标签，返回纯文本"""
    exclude = {'table', 'tr', 'td', 'th'}
    tags_pattern = '|'.join([tag for tag in HTML_TAGS if tag not in exclude])
    pattern = rf'</?(?:{tags_pattern})\b[^>]*>'
    return re.sub(pattern, '', text, flags=re.IGNORECASE)

def find_html_tables(text):
    # 非贪婪匹配所有 table 块
    tables = re.findall(r'<table.*?</table>', text, re.DOTALL | re.IGNORECASE)
    return tables

@deprecated('use extract_boxed_content instead')
def extract_boxed_content_old(text):
    # 用正则抽取 boxed 内容
    pattern = r'\\boxed\{'
    matches = []
    start = 0
    while True:
        match = re.search(pattern, text[start:])
        if not match:
            break
        box_start = start + match.end()
        brace_count = 1
        i = box_start
        while i < len(text):
            if text[i] == '{':
                brace_count += 1
            elif text[i] == '}':
                brace_count -= 1
                if brace_count == 0:
                    matches.append(text[box_start:i])
                    start = i + 1
                    break
            i += 1
        else:
            break  # 没有配对的右括号
    return matches


def extract_boxed_content(text):
    match = re.search(r'<\|begin_of_box\|>(.*?)<\|end_of_box\|>', text, re.DOTALL)
    if match:
        return [s.strip() for s in match.group(1).split("<|sep|>")]
    return None


# 封装抽取最终答案的函数
def extract_boxed_answer(text, api_extract_model=None):
    # 先尝试 begin_of_box 格式
    box_answers = extract_boxed_content(text)
    if box_answers:
        if len(box_answers) > 1:
            return "<|sep|>".join(box_answers)
        else:
            return box_answers[0]
    # 再尝试 boxed 格式
    boxed = extract_boxed_content_old(text)
    if boxed:
        if len(boxed) != 1:
            return ""  # 按道理来说这样子是不符合规范的，应该直接返回空！
        else:
            return boxed[0]
    # 否则用 LLM 抽取
    if api_extract_model is not None:
        prompt = EXTRACT_ANSWER_PROMPT.format(answer=text)
        result = api_extract_model.generate([{"role": "user", "content": prompt}])
        for line in result.splitlines():
            line = line.strip()
            if line:
                return line
    return ""