import re
import random

def add_short_prompt(question, datatype, language="en"):
    templates = PROMPTS if language == "en" else PROMPTS_ZH
    template = random.choice(templates[datatype])
    if template[0] == "head":
        new_query = template[1] + "\n" + question.strip()
    else:
        new_query = question + "\n" + template[1]
    return new_query

def has_short_prompt(question, datatype="all"):
    if datatype == "all":
        pattern = SHORT_PROMPT_PATTERN
    elif datatype == "Phrase":
        pattern = PHRASE_PROMPT_PATTERN
    elif datatype == "multichoice":
        pattern = OPTION_PROMPT_PATTERN
    elif datatype == "YorN":
        pattern = JUDGEMENT_PROMPT_PATTERN
    return bool(pattern.search(question))

def del_short_prompt(question):
    pattern = re.compile(
        '|'.join(re.escape(prompt) + r'(\n)?' for prompt in ALL_SHORT_PROMPTS)
    )
    return pattern.sub("", question).strip()

def del_cot_prompt(question):
    pattern = re.compile(
        '|'.join(re.escape(prompt) + r'(\n)?' for prompt in COT_PROMPTS)
    )
    return pattern.sub("", question).strip()

def del_ocr_prompt(question):
    pattern = re.compile(
        '|'.join(re.escape(prompt) + r'(\n)?' for prompt in OCR_PROMPTS)
    )
    return pattern.sub("", question).strip()

def add_image_caption_prompt(language="en"):
    prompts_cap = {
        "en": [
            "Please provide a caption for the given image.",
            "Describe the image.",
            "Give me a summary of this image.",
            "What do you see in the picture?",
            "Tell me about this picture.",
            "Explain the image to me.",
            "Break down what's in the photo.",
            "What is depicted in the picture?",
            "Illustrate the content of the image.",
            "Convey the essence of the image.",
            "Elaborate on the picture.",
            "Can you detail the image?",
            "Provide an overview of this picture.",
            "Walk me through this image.",
            "What does the image show?",
            "Characterize the picture for me.",
            "Render a description of the image.",
            "Can you clarify what's in the image?",
            "Discuss the elements of the picture.",
            "Provide insight into this image.",
            "What's going on in this photo?"
        ],
        "zh": [
            '这幅作品描绘了：',
            '描述这张图片：',
            '从这张图片中，我们可以看到：',
            '这张图片中最引人注目的是什么？',
            '如果要在这张图片上添加一个标语，您会写什么？',
            '描述图片内容的关键词：',
            '画面传达了以下信息：',
            '这张图展示了：',
            '这张图片展示了什么？',
            '描述这张图片中的场景：',
            '这张图片的主要焦点是：',
            '适合这张图片的标题是：',
            '这张图片可以被描述为：',
            '图片中的元素包括：',
            '这张图片想表达什么信息？',
            '请用一句话来概括这张图片的主题。',
            '图片呈现了以下场景：',
            '以简短的语言概括这张图片：',
            '这张照片的故事是：',
            '从这幅画中，我们可以发现：',
            '对这幅图像进行简要说明：',
        ]
    }[language]
    return random.choice(prompts_cap)

PROMPTS = {
    "multichoice": [
        ("tail", "Answer with the given letter directly."),
        ("tail", "Answer with the option letter from the given choices directly."),
        ("tail", "Please respond with only the letter of the correct answer."),
        ("head", "Hint: Please answer the question and provide the correct option letter, e.g., A, B, C, D, at the end."),
        ("tail", "Respond using only the letter of the selected option."),
        ("tail", "Choose the correct letter and provide it as your answer."),
        ("tail", "Answer by selecting the letter of the correct choice."),
        ("tail", "Provide your answer in the form of the corresponding option letter."),
        ("tail", "Please respond with just the letter that corresponds to the correct answer."),
        ("tail", "Select the correct answer and respond with its letter."),
        ("tail", "Answer by simply stating the letter corresponding to the correct option."),
        ("tail", "Answer using the letter that corresponds to the correct choice."),
        ("tail", "Give only the letter of the correct option as your answer."),
        ("tail", "Reply with the letter corresponding to the best answer."),
        ("tail", "Answer with the option's letter from the given choices directly."),
        ("head", "Choose the correct option letter (e.g. A, B, C, D, ...) to answer."),
        ("head", "Please respond using one of the given options (e.g. A, B, C, D, ...)."),
        ("head", "Hint: Select the letter corresponding to the right choice and give it as your answer."),
        ("head", "Please answer by providing only the option letter (e.g. A, B, C, D, ...)."),
        ("head", "Hint: The correct response should be a single letter, e.g., A, B, C, D."),
        ("head", "Answer the question by choosing the letter corresponding to the correct answer."),
        ("head", "Answer the question using a single word or phrase."),
        ("tail", "Only reply with the correct choice"),
        ("tail", "Only respond with the correct option letter"),
        ("tail", "Only provide the letter"),
        ("tail", "Respond with just the letter"),
        ("tail", "Just reply with the letter"),
        ("tail", "Respond only with the letter"),
        ("tail", "reply with the letter corresponding to the correct answer"),
        ("tail", "Only reply with the letter"),
        ("tail", "Only provide the corresponding letter"),
        ("tail", "Respond solely with the respective letter"),
        ("tail", "Reply with the letter only"),
        ("tail", "Give only the letter answer"),
        ("tail", "Only state its letter"),
        ("tail", "Just reply with its letter"),
        ("tail", "Please state only the letter"),
        ("tail", "Only reply with its letter"),
        ("tail", "Please answer just with the point’s letter"),
        ("tail", "Only answer using the letter"),
        ("tail", "Only supply its letter"),
        ("tail", "Just answer with the letter"),
        ("tail", "Answer with the point's letter only"),
        ("tail", "Only respond with the letter"),
        ("tail", "Only provide its letter"),
        ("tail", "Only give the letter as the answer"),
        ("tail", "Reply with only its letter"),
        ("tail", "Please answer with the letter only"), 
    ],
    "YorN": [
        ("tail", "Answer the question with Yes or No."),
        ("tail", "Answer yes or no."),
        ("tail", "Yes or No?"),
        ("tail", "Please respond with either Yes or No."),
        ("tail", "Answer using Yes or No."),
        ("tail", "Reply with Yes or No."),
        ("tail", "Choose either Yes or No as your answer."),
        ("tail", "Your answer should be either Yes or No."),
        ("tail", "Provide your answer as Yes or No."),
        ("tail", "Select Yes or No as your response."),
        ("tail", "Answer by selecting Yes or No."),
        ("tail", "Respond with a simple Yes or No."),
        ("tail", "Please choose Yes or No to answer."),
        ("tail", "Yes or No? Please select one."),
        ("head", "Answer the following question with Yes or No."),
        ("tail", "Please respond to the question with a Yes or No."),
        ("head", "Hint: Answer with Yes or No."),
        ("head", "For the following, respond with Yes or No."),
        ("tail", "Select either Yes or No as the correct answer."),
        ("tail", "Answer Yes or No only."),
        ("tail", "Reply only with Yes or No."),
        ("tail", "Just answer Yes or No."),
        ("tail", "Please respond with Yes or No."),
        ("tail", "Your reply should be Yes or No."),
        ("tail", "Respond Yes or No only."),
        ("tail", "Reply with Yes or No only."),
        ("tail", "Simply say Yes or No."),
        ("tail", "Only reply Yes or No."),
        ("tail", "Answer Yes or No."),
        ("tail", "Only answer Yes or No."),
        ("tail", "Respond with Yes or No."),
        ("tail", "Please answer Yes or No."),
        ("tail", "Reply Yes or No only."),
    ],
    "Phrase": [
        ("tail", "Answer the question with a single word (or phrase)."),
        ("tail", "Answer with a brief word or phrase."),
        ("tail", "Respond using just a short word or phrase."),
        ("tail", "Please provide a one-word or short phrase answer."),
        ("tail", "Answer concisely with a single word or phrase."),
        ("tail", "Give a brief response, either a word or a phrase."),
        ("tail", "Please reply with a short word or phrase."),
        ("tail", "Respond with a single word or a simple phrase."),
        ("tail", "Provide your answer in a brief word or phrase."),
        ("tail", "Answer briefly using a word or short phrase."),
        ("tail", "Please answer using only a word or a phrase."),
        ("tail", "Concise answer only."),
        ("tail", "Give a very brief answer."),
        ("tail", "Offer a very short reply."),
        ("tail", "Offer a terse response."),
        ("tail", "Your answer should be compact."),
        ("tail", "Make the answer very short."),
        ("tail", "Ensure brevity in your answer."),
        ("tail", "Provide a succinct answer."),
        ("tail", "Your answer should be very brief."),
        ("tail", "Your response must be concise."),
        ("tail", "Provide a short and direct response."),
        ("tail", "Be succinct."),
        ("tail", "Quick response, please."),
        ("tail", "Keep it brief."),
        ("tail", "Write a very short answer."),
        ("tail", "Answer briefly."),
        ("head", "Hint: Please answer the question and provide the final answer at the end."),
        ("head", "Answer the following question briefly with a word or phrase."),
        ("head", "Hint: Respond using a short word or phrase."),
        ("head", "Provide a brief answer, either a word or a phrase."),
        ("head", "Please respond with a concise word or phrase."),
        ("head", "Hint: Answer with a single word or a short phrase."),
        ("head", "Hint: Keep your answer brief, using a word or phrase."),
        ("head", "Please answer with a simple word or short phrase."),
        ("head", "Provide a one-word or brief phrase response."),
        ("head", "Please reply briefly, with just a word or phrase."),
        ("head", "Answer the question with a single word."),
    ],
    "Action_Phrase":[
        # in Penn_Action
        ("tail", "Reply with only the action."),
        ("tail", "Just give the action name."),
        ("tail", "Output the action name solely."),
        ("tail", "Output only the action name."),
        ("tail", "Respond with just the action."),
        ("tail", "Only state the action name."),
        ("tail", "Your answer should be the action name only."),
        ("tail", "Please provide only the action."),
        ("tail", "Respond with the action name only."),
        ("tail", "Output only the action."),
        ("tail", "Please limit your response to the action name."),
        ("tail", "Output the action name and nothing else."),
        ("tail", "Only the action name is required."),
        ("tail", "State only the action."),
        ("tail", "Output just the action name."),
        ("tail", "Provide the action name only."),
        ("tail", "Your output should be the action name."),
        ("tail", "Answer with just the action name."),
        ("tail", "Please respond with the action name only."),
    ],
    "Direction_Phrase":[
        # in 3D_Street_View
        ("tail", "Output only the direction name."),
        ("tail", "Please output only the direction."),
        ("tail", " and provide only the direction"),
        ("tail", "Just state the direction."),
        ("tail", "Output the selected direction solely."),
        ("tail", " and reply with just the direction"),
        ("tail", "Only the direction name is needed."),
        ("tail", "Write only the direction."),
        ("tail", "Your answer should be only the direction."),
        ("tail", "Provide only the direction as your answer."),
        ("tail", " and return only the direction term"),
        ("tail", "Output the direction label only."),
        ("tail", " Just give the direction."),
        ("tail", " and state only the direction"),
        ("tail", "Respond with only the direction."),
        ("tail", "Output the direction only."),
        ("tail", "Provide just the direction."),
        ("tail", "Output the direction solely."),
        ("tail", "Reply with only the direction."),
        ("tail", "Write down only the direction."),
        ("tail", "Only output the direction."),
        ("tail", " and provide only the direction name"),
        ("tail", "Output the direction only."),
        ("tail", "Just provide the direction."),
        ("tail", "Only state the direction."),
        ("tail", "Your answer should be the direction only."),
        ("tail", "Respond with just the direction."),
        ("tail", "Only the direction is required."),
        ("tail", "Output only the direction term."),
        ("tail", "Provide the direction name only."),
        ("tail", "Only state the direction."),
        ("tail", "Only state the direction."),
    ]
}

PROMPTS_ZH = {
    "multichoice": [
        ("tail", "直接用给定的字母回答。"),
        ("tail", "直接用选项中的字母回答。"),
        ("tail", "请仅回答正确答案对应的字母。"),
        ("head", "提示：请回答问题并在最后提供正确选项的字母，例如：A、B、C、D。"),
        ("tail", "只用选项的字母回答。"),
        ("tail", "选择正确的字母并作为你的答案。"),
        ("tail", "通过选择正确选项的字母来回答。"),
        ("tail", "提供你的答案，以相应选项的字母形式。"),
        ("tail", "请只回答对应正确答案的字母。"),
        ("tail", "选择正确的答案并用字母回答。"),
        ("tail", "只需用与正确选项对应的字母作答。"),
        ("tail", "使用与正确选项对应的字母回答。"),
        ("tail", "只给出正确选项的字母作为你的答案。"),
        ("tail", "用与最佳答案对应的字母回复。"),
        ("tail", "直接用给定选项的字母回答。"),
        ("head", "选择正确的选项字母（如 A、B、C、D...）来回答。"),
        ("head", "请使用给定的选项字母（如 A、B、C、D...）作答。"),
        ("head", "提示：选择正确答案对应的字母并将其作为答案。"),
        ("head", "请仅通过提供选项字母（如 A、B、C、D...）作答。"),
        ("head", "提示：正确的答案应为一个字母，例如 A、B、C、D。"),
        ("head", "通过选择正确答案对应的字母来回答问题。"),
        ("head", "只需回复正确答案的字母"),
        ("head", "仅需回复选项字母"),
        ("head", "通过选项字母回答相关选择题"),
        ("head", "请仅用正确的选项字母作答"),
        ("head", "用正确答案的字母回复问题"),
        ("head", "用正确选项对应的字母回答问题"),
        ("head", "用正确答案的字母回复问题"),
        ("head", "请仅回答对应的字母"),
        ("head", "通过选项字母回应相关问题"),
        ("head", "仅需回答选项字母"),
        ("tail", "请只用其对应字母作答"),
        ("tail", "只写字母即可"),
        ("tail", "请只写对应字母"),
        ("tail", "请仅回复字母"),
        ("tail", "只需要回答对应的字母"),
        ("tail", "只回答字母"),
        ("tail", "仅需填写相应字母"),
        ("tail", "请仅用字母作答"),
        ("tail", "只需填入其字母即可"),
        ("tail", "请直接回答其字母"),
    ],
    "YorN": [
        ("tail", "用是或否回答问题。"),
        ("tail", "回答是或否。"),
        ("tail", "是或否？"),
        ("tail", "请回答是或否。"),
        ("tail", "用是或否作答。"),
        ("tail", "用是或否回复。"),
        ("tail", "选择是或否作为你的答案。"),
        ("tail", "你的答案应该是是或否。"),
        ("tail", "用是或否作为答案。"),
        ("tail", "选择是或否作为你的回答。"),
        ("tail", "通过选择是或否来作答。"),
        ("tail", "请用简单的是或否回答。"),
        ("tail", "请选择是或否来作答。"),
        ("tail", "是或否？请选一个。"),
        ("head", "用是或否回答以下问题。"),
        ("tail", "请用是或否回答这个问题。"),
        ("head", "提示：用是或否作答。"),
        ("head", "对于以下内容，用是或否作答。"),
        ("tail", "选择是或否作为正确答案。"),
        ("tail", "通过选择是或否来回答问题。"),
        ("tail", "请只回答是或否。"),
        ("tail", "仅回答是或否。"),
        ("tail", "请回答‘是’或‘否’即可。"),
        ("tail", "请简要回复是或否。"),
        ("tail", "回答是或否。"),
        ("tail", "回答‘是’或‘否’即可。"),
        ("tail", "只需回复‘是’或‘否’。"),
        ("tail", "请仅用是或否作答。"),
        ("tail", "回复‘是’或‘否’即可。"),
        ("tail", "只需回答是或否。"),
        ("tail", "仅回答是或否。"),
        ("tail", "请只回复是或否。"),
        ("tail", "回复‘是’或‘否’即可。"),
        ("tail", "请回复是或否。"),
        ("tail", "简单回答是或否。"),
        ("tail", "仅用是或否回答。"),
        ("tail", "只回答是或否。"),
        ("tail", "，并回复是或否"),
        ("tail", "仅用是或否回答。"),
        ("tail", "仅用是或否回答。"),
    ],
    "Phrase": [
        ("tail", "用一个词或短语回答问题。"),
        ("tail", "用简短的一个词或短语回答。"),
        ("tail", "只需用一个简短的词或短语回答。"),
        ("tail", "请提供一个词或短语作为回答。"),
        ("tail", "用一个词或短语简洁作答。"),
        ("tail", "给出简短的答案，可以是一个词或短语。"),
        ("tail", "请用一个简短的词或短语回复。"),
        ("tail", "用一个词或简单短语作答。"),
        ("tail", "用简短的词或短语提供你的答案。"),
        ("tail", "简洁地用一个词或短语回答。"),
        ("tail", "请只用一个词或短语回答。"),
        ("tail", "只需简洁回答。"),
        ("tail", "给出非常简短的答案。"),
        ("head", "提示：请回答问题并在最后提供最终答案。"),
        ("head", "用一个词或短语简洁回答以下问题。"),
        ("head", "提示：用一个简短的词或短语作答。"),
        ("head", "提供一个简短的答案，可以是一个词或短语。"),
        ("head", "请用简洁的词或短语回答。"),
        ("head", "提示：用一个词或短语回答。"),
        ("head", "提示：保持简洁，用一个词或短语回答。"),
        ("head", "请用一个简单的词或短语回答。"),
        ("head", "提供一个词或简短的短语作为回答。"),
        ("head", "请简短作答，只需一个词或短语。"),
    ],
    "Action_Phrase":[
        # in Penn_action
        ("tail", "请仅提供动作名称。"), 
        ("tail", "请将您的回答限制在动作名称。"),
        ("tail", "输出动作名称，不要包含其他内容。"),
        ("tail", "你的答案应仅为动作名称。"),
        ("tail", "只陈述动作名称。"),
        ("tail", "仅需说明动作名称。"),
        ("tail", "仅回答动作即可。"),
        ("tail", "请只输出动作名称。"),
        ("tail", "请单独输出动作名称。"),
        ("tail", "只需给出动作名称。"),
        ("tail", "请仅输出动作名称。"),
        ("tail", "只需要动作名称。"),
        ("tail", "请仅用动作名称回答。"),
        ("tail", "请仅用动作回答。"),
        ("tail", "你的输出应该是动作名称。"),
        ("tail", "请仅用动作名称作答。"),
        ("tail", "请仅用动作名称来回应。"),
    ],
    "Direction_Phrase":[
        # in 3D_Street_View
        ("tail", "，仅需输出拍摄朝向即可"),
        ("tail", "请只输出方向名称。"),
        ("tail", "，并仅输出所选方向"),
        ("tail", "回答时仅需写出方向。"),
        ("tail", "只需给出方向。"),
        ("tail", "，并仅输出方向"),
        ("tail", "请直接输出方向。"),
        ("tail", "仅需回答方向名称。"),
        ("tail", "请只回答方向。"),
        ("tail", "输出结果仅包含方向。"),
        ("tail", "仅输出方向即可。"),
        ("tail", "，并只输出方向名称"),
        ("tail", "只写方向。"),
        ("tail", "仅需输出方向。"),
        ("tail", "请仅提供方向。"),
        ("tail", "只输出选择的方向。"),
        ("tail", "请仅输出方向。"),
        ("tail", "只需输出方向名称。"),
        ("tail", "仅输出方向。"),
        ("tail", "输出格式：仅方向。"),
        ("tail", "仅需回答方向。"),
        ("tail", "只输出方向名称。"),
        ("tail", "仅输出最终选择的方向。"),
        ("tail", "回答仅包含方向。"),
    ]
}

COT_PROMPTS = [
    "Let's think step by step:",
    "Generate a reason first and then output a short answer.",
    "Answer the question with detailed explanation.",
    "Answer the question with GPT-T-COCO format.",
    "Answer the question and provide an explanation.",
    "Please answer the question below, explaining your reasoning step by step before providing the final answer.",
    "Give reasoning steps and answers.",
    "First perform reasoning, then finally select the question from the choices in the following format: Answer: xxx.",
    "According to the question shown in the image, please first perform reasoning, then finally select the right answer from the choices, e.g., Answer: xxx.",
    "According to the question shown in the image, please first conduct reasoning, and then answer the question and provide the final value, e.g., The answer is xxx",
    "Let's describe the image first and think step by step."
]

OCR_PROMPTS = [
    "请直接使用图中出现的文字回答，不用考虑上下文，只回答图中出现的文字。",
    "Provide an answer to this question by directly using the text provided in the image.",
    "Directly use the text in the image to answer this question.",
    "Use the text from the image to respond to this question directly.",
    "Give your answer in one word or a phrase",
    "Provide the requested information directly",
    "Answer this question by directly utilizing the text within the image",
    "Use the format MM/DD/YYYY Use the text from the image to respond to this question directly",
    "请直接使用图中出现的文字回答，不用考虑上下文，只回答图中出现的文字",
    "(format: mm/dd/yy) Provide an answer to this question by directly using the text provided in the image",
    "Directly refer to the text in the image to answer this question",
    "Provide an answer to this question by directly using the text provided in the image",
    "Please use the format MM/DD/YY Use the text from the image to respond to this question directly",
    "Provide an answer to this question by directly using the text provided in the image",
    "Answer this question using the text in the image directly without any other context",
    "Answer this question using the text in the image directly",
    "Directly use the text in the image to answer this question",
    "Directly refer to the text in the image to answer this question",
    "Directly use the text in the image to answer this question",
    "Answer this question by directly utilizing the text within the image",
    "Provide the requested information directly",
    "Use the text from the image to respond to this question directly",
    "Please include \"-\" Use the text from the image to respond to this question directly"
]

ALL_SHORT_PROMPTS = [
    template[1]
        for prompt_lang in [PROMPTS, PROMPTS_ZH]
            for _, templates in prompt_lang.items() 
                for template in templates
]

ALL_PHRASE_PROMPTS = [
    template[1]
        for prompt_lang in [PROMPTS, PROMPTS_ZH]
            for name, templates in prompt_lang.items() 
                if name == "Phrase"
                    for template in templates
]

ALL_OPTION_PROMPTS = [
    template[1]
        for prompt_lang in [PROMPTS, PROMPTS_ZH]
            for name, templates in prompt_lang.items() 
                if name == "multichoice"
                    for template in templates
]

ALL_JUDGEMENT_PROMPTS = [
    template[1]
        for prompt_lang in [PROMPTS, PROMPTS_ZH]
            for name, templates in prompt_lang.items() 
                if name == "YorN"
                    for template in templates
]

SHORT_PROMPT_PATTERN = re.compile(
    '|'.join(re.escape(prompt) for prompt in ALL_SHORT_PROMPTS)
)

COT_PROMPT_PATTERN = re.compile(
        '|'.join(re.escape(prompt) + r'(\n)?' for prompt in COT_PROMPTS)
    )

PHRASE_PROMPT_PATTERN = re.compile(
    '|'.join(re.escape(prompt) for prompt in ALL_PHRASE_PROMPTS)
)

OPTION_PROMPT_PATTERN = re.compile(
    '|'.join(re.escape(prompt) for prompt in ALL_OPTION_PROMPTS)
)

JUDGEMENT_PROMPT_PATTERN = re.compile(
    '|'.join(re.escape(prompt) for prompt in ALL_JUDGEMENT_PROMPTS)
)

COT_PROMPT_PATTERN = re.compile(
    '|'.join(re.escape(prompt) for prompt in COT_PROMPTS)
)

if __name__ == "__main__":
    print(ALL_SHORT_PROMPTS)
    print(ALL_OPTION_PROMPTS)
    print(ALL_JUDGEMENT_PROMPTS)