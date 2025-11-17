import json


def parse_json(json_str): 
    if isinstance(json_str, str):
        return json.loads(json_str) 
    
    return json_str


def ensure_json_string(data) -> str:
    if not isinstance(data, str):
        return json.dumps(data)
    return data


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


def extract_port(host_string, default_port=80):
    """
    Extract port number from a host string.
    
    Args:
        host_string (str): Host string, e.g., '127.0.0.1' or '127.0.0.1:3306'
        default_port (int): Default port to return if none is found (default: 80)
    
    Returns:
        int: Port number if found, otherwise default_port
    """
    try:
        # Split the string by ':' and take the last part as port
        parts = host_string.split(':')
        if len(parts) > 1:
            return int(parts[-1])
        return default_port
    except ValueError:
        # Return default if port is not a valid integer
        return default_port
    
def make_collectioname(s):
    return '_' + ''.join(c for c in s if c.isalnum() or c == '_')