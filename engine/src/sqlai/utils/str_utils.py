import json


def parse_json(json_str): 
    if isinstance(json_str, str):
        return json.loads(json_str) 
    
    return json_str


def remove_code_block(text: str, marker: str) -> str:
    """
    Removes the ```marker and ``` in returned string
    """
    # Define the prefixes and suffixes to be removed
    prefix = '```' + marker
    suffix = '```'
    # Check and remove the prefix and suffix if they exist
    if text.startswith(prefix) and text.endswith(suffix):
        # Remove the markers and any leading/trailing whitespace
        return text.removeprefix(prefix).removesuffix(suffix).strip()

    # If the markers aren't present, return the original text
    return text