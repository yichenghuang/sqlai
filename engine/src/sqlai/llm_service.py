import logging
import re
import anthropic
from openai import OpenAI
import google.generativeai as genai
from sqlai.core.config import ModelConfig
from sqlai.utils.str_utils import remove_code_block


logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


def fix_broken_llm_json(text: str) -> dict:
    r"""
    Fixes broken JSON from LLMs (unescaped quotes, \_id, etc.)
    while leaving correct JSON untouched.
    """

# 1. Remove ```json wrapper if present
    if "```" in text:
        match = re.search(r"```(?:json)?\s*([\s\S]*?)\s*```", text)
        if match:
            text = match.group(1)
        else:
            # Fallback: extract first { ... } or [ ... ]
            start = text.find("{")
            end = text.rfind("}") + 1
            if start != -1 and end > start:
                text = text[start:end]
            else:
                raise ValueError("No JSON found")

    # ------------------------------------------------------------------
    # 1. Remove Markdown escapes: \_ → _, \* → *, \< → <, \> → >
    # ------------------------------------------------------------------
    text = re.sub(r"\\(_|\*|<|>|`)", r"\1", text)

    ## 3. Fix ONLY strings that contain unescaped double quotes
    def fix_bad_quotes(match):
        key_part = match.group(1)      # e.g. "sql": "
        content  = match.group(2)      # the broken content
        closing  = match.group(3)      # final "

        # Only fix if there's a naked " inside
        if '"' in content and '\\"' not in content:
            content = content.replace("\\", "\\\\").replace('"', '\\"')
        return key_part + content + closing

    # Apply only to string values that likely have unescaped quotes
    text = re.sub(r'(:\s*")([^"\\]*(?:\\.[^"\\]*)*)(")', fix_bad_quotes, text)

    # 4. Remove trailing commas before } or ]
    text = re.sub(r",\s*([}\]])", r"\1", text)

    return text


### OpenAI
openai_def_sys_prompt="You are a data analyst. Only output valid JSON. Do not include any explanation or repeat the input."

def openai_chat(model, user_prompt, system_prompt=openai_def_sys_prompt):
    """
    Sends a message to the OpenAI model and returns the text response.

    Args:
        model: The OpenAI model instance (e.g., gpt-4.1).
        msg: The user's message as a string.

    Returns:
        The model's generated text as a string.
    """
    client = OpenAI()
    response = client.chat.completions.create(
        model = model,
        messages = [
            { "role": "system", "content": system_prompt },
            { "role": "user", "content": user_prompt }
        ],
    )
    return response.choices[0].message.content


### Gemini
genai_def_sys_prompt="You are a data analyst. You only output valid JSON objects and nothing else.",

def genai_chat(model, user_prompt, system_prompt=genai_def_sys_prompt):
    """
    Sends a message to the Gemini model and returns the text response.

    Args:
        model: The Gemini model instance (e.g., gemini-2.0-flash).
        msg: The user's message as a string.

    Returns:
        The model's generated text as a string.
    """
    try:
        llm_model = genai.GenerativeModel(
            model,
            system_instruction = system_prompt,
        )
        response = llm_model.generate_content(user_prompt)
        if response and response.candidates and response.candidates[0].content and response.candidates[0].content.parts:
            resp_text = fix_broken_llm_json(response.candidates[0].content.parts[0].text)
            # resp_text = remove_code_block(response.candidates[0].content.parts[0].text, 'json')

            if logger.isEnabledFor(logging.INFO):
                # hack the gemini usage_metadata since it is a protobuf message,
                extra = {"llm_prompt": user_prompt, 
                         "llm_response": resp_text,
                         "usage_metadata": {
                             "prompt_token_count": response.usage_metadata.prompt_token_count,
                             "candidates_token_count": response.usage_metadata.candidates_token_count,
                             "total_token_count": response.usage_metadata.total_token_count
                         },
                         "model_version": model}
                logger.info("call gemini", extra=extra)

            return resp_text
        else:
            return "No content generated."

    except Exception as e:
        logger.error("call gemini", f"An error occurred: {e}")
        return "Failed to get a response from the model."


### Anthrpic
anthropic_def_sys_prompt="You are a data analyst. You only output valid JSON objects and nothing else.",

def anthropic_chat(model, user_prompt, system_prompt=anthropic_def_sys_prompt):
    """
    Sends a message to the Anthropic model and returns the text response.

    Args:
        model: The Anthropic model instance (e.g., claude-sonnet-4-5).
        msg: The user's message as a string.

    Returns:
        The model's generated text as a string.
    """
    client = anthropic.Anthropic()
    response = client.messages.create(
        model = model,
        max_tokens=2048,
        system = system_prompt,
        messages=[
            {"role": "user", "content": user_prompt}
        ]
    )
    resp_text = remove_code_block(response.content[0].text, 'json')
    match_text = re.search(r'\{.*\}', resp_text, re.DOTALL)
    match_text = match_text.group()
    return match_text
    

def llm_chat(user_prompt, sys_prompt = None):
    model = ModelConfig.get_model()
    service_model = ModelConfig.get_service_model()
    if sys_prompt:
        match service_model:
            case 'gemini':
                 return genai_chat(model, user_prompt, sys_prompt)
            case 'gpt': 
                return openai_chat(model, user_prompt, sys_prompt)
            case 'claude':
                return anthropic_chat(model, user_prompt, sys_prompt)
            # case 'grok':
            #     print('Calling xAI Grok API...')
    else:       
        match service_model:
            case 'gemini':
                return genai_chat(model, user_prompt)

            case 'gpt':
                return openai_chat(model, user_prompt)
            case 'claude':
                return anthropic_chat(model, user_prompt, sys_prompt)
            # case 'grok':
            #     print('Calling xAI Grok API...')
