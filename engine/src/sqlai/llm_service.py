import logging
from openai import OpenAI
import google.generativeai as genai
from sqlai.core.config import ModelConfig
from sqlai.utils.str_utils import remove_code_block


logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


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
        ]
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
            # resp_text = remove_json_code_block(
            #     response.candidates[0].content.parts[0].text)
            resp_text = remove_code_block(response.candidates[0].content.parts[0].text, 'json')

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
    

def llm_chat(user_prompt, sys_prompt = None):
    model = ModelConfig.get_model()
    service_model = ModelConfig.get_service_model()
    if sys_prompt:
        match service_model:
            case 'gemini':
                 return genai_chat(model, user_prompt, sys_prompt)
            # case 'grok':
            #     print('Calling xAI Grok API...')
            case 'gpt': 
                return openai_chat(model, user_prompt, sys_prompt)
    else:       
        match service_model:
            case 'gemini':
                return genai_chat(model, user_prompt)
            # case 'grok':
            #     print('Calling xAI Grok API...')
            case 'gpt':
                return openai_chat(model, user_prompt)
