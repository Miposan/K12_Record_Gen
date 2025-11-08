# 图片类型
IMAGE_MIME_TYPES = {
    'jpg': 'image/jpeg',
    'jpeg': 'image/jpeg',
    'png': 'image/png',
    'gif': 'image/gif',
    'webp': 'image/webp',
    'bmp': 'image/bmp',
    'tiff': 'image/tiff'
}

# 常见HTML标签列表（可根据需要扩展）
HTML_TAGS = [
    'a', 'abbr', 'address', 'area', 'article', 'aside', 'audio', 'b', 'base', 'bdi', 'bdo', 'blockquote', 'body',
    'br', 'button', 'canvas', 'caption', 'cite', 'code', 'col', 'colgroup', 'data', 'datalist', 'dd', 'del', 'details',
    'dfn', 'dialog', 'div', 'dl', 'dt', 'em', 'embed', 'fieldset', 'figcaption', 'figure', 'footer', 'form', 'h1', 'h2',
    'h3', 'h4', 'h5', 'h6', 'head', 'header', 'hr', 'html', 'i', 'iframe', 'img', 'input', 'ins', 'kbd', 'label', 'legend',
    'li', 'link', 'main', 'map', 'mark', 'meta', 'meter', 'nav', 'noscript', 'object', 'ol', 'optgroup', 'option', 'output',
    'p', 'param', 'picture', 'pre', 'progress', 'q', 'rp', 'rt', 'ruby', 's', 'samp', 'script', 'section', 'select', 'small',
    'source', 'span', 'strong', 'style', 'sub', 'summary', 'sup', 'svg', 'table', 'tbody', 'td', 'template', 'textarea',
    'tfoot', 'th', 'thead', 'time', 'title', 'tr', 'track', 'u', 'ul', 'var', 'video', 'wbr'
]


# 答案抽取 prompt
EXTRACT_ANSWER_PROMPT = '''
## 任务
请从以下数学解答中抽取最终答案，只输出答案数值，不要添加任何解释或其他文字。

## 例子

### 例1
Answer: 
```
【解答】
  解：∵将△OAB绕点O逆时针旋转100°得到△OA_{{1}}B_{{1}} ，
  ∴∠A_{{1}}OA=100°，
  ∵∠AOB=30°，
  ∴∠A_{{1}}OB=∠A_{{1}}OA-∠AOB=100°-30°=70°．
  故答案为：70．
  由旋转的性质可得∠A_{{1}}OA=100°，即可求解．
  本题考查了旋转的性质，熟练运用旋转的性质是本题的关键．【答案】
  70
```
Result: 70

### 例2
Answer: 
```
【解答】
  解：∵∠A与∠BCD是同弧所对的圆周角与圆心角，∠BCD=120°，
  ∴∠A=60°，
  ∴∠A+∠C=180°，
  ∴∠C=120°，
  ∴∠BOD=2∠C=240°．
  根据圆周角定理求解．
  本题利用了圆周角定理和圆内接四边形的性质求解．【答案】
  240
```
Result: 240

## 给出你的答案
Answer: 
```
{answer}
```
Result: (只给出最终答案，不要添加无关的描述，否则你将受到严重惩罚！)
'''


# 答案抽取 prompt
EXTRACT_OPTION_ANSWER_PROMPT = '''
## 任务
请从以下数学解答中抽取最终答案，只输出答案选项，不要添加任何解释或其他文字。

## 例子

### 例1
Answer: 
```
【解答】
  解：∵将△OAB绕点O逆时针旋转100°得到△OA_{{1}}B_{{1}} ，
  ∴∠A_{{1}}OA=100°，
  ∵∠AOB=30°，
  ∴∠A_{{1}}OB=∠A_{{1}}OA-∠AOB=100°-30°=70°．
  故答案为：70．
  由旋转的性质可得∠A_{{1}}OA=100°，即可求解．
  本题考查了旋转的性质，熟练运用旋转的性质是本题的关键．【答案】70 因此选择C
```
Result: C

### 例2
Answer: 
```
【解答】
  解：∵∠A与∠BCD是同弧所对的圆周角与圆心角，∠BCD=120°，
  ∴∠A=60°，
  ∴∠A+∠C=180°，
  ∴∠C=120°，
  ∴∠BOD=2∠C=240°．
  根据圆周角定理求解．
  本题利用了圆周角定理和圆内接四边形的性质求解．【答案】240 因此选择B
```
Result: B

## 给出你的答案
Answer: 
```
{answer}
```
Result: (只给出最终选项，不要添加无关的描述，否则你将受到严重惩罚！)
'''




