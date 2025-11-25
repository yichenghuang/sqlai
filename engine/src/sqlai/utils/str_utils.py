import json
import re


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

def fix_and_parse_llm_json(malformed: str):
    fixed = malformed.replace(r'\"', '"')
    fixed = fixed.encode().decode('unicode-escape')
    return fixed

# def fix_and_parse_llm_json(malformed: str) -> dict:
#     """
#     Fixes common JSON issues produced by LLMs and parses it safely.
#     Handles:
#     - Raw \n, \t inside strings
#     - \\\" instead of \"
#     - Unescaped " inside strings
#     - Trailing commas
#     - Single quotes (')
#     - Extra backslashes
#     """
#     if not malformed.strip():
#         raise ValueError("Empty input")
    
#     s = malformed.strip()
#     # 1. Remove code block wrappers if present
#     # s = re.sub(r'^```json\s*', '', s)
#     # s = re.sub(r'^```\s*$', '', s)
#     # s = re.sub(r'^```', '', s)

#     # 2. Fix triple-escaped quotes: \\\" → \"
#     s = s.replace('\\\"', '"')

#     # 3. Fix double-escaped quotes where needed: \\" → \"
#     s = s.replace('\\"', '"')
    
#     # 4. Handle raw \n, \t, \r inside the string (common LLM mistake)
#     s = s.encode('utf-8').decode('unicode_escape')

#     # 5. Now properly escape internal double quotes that are NOT already escaped
#     # This is the tricky part: we want " inside values to become \"
#     def escape_quotes(match):
#         content = match.group(1)
#         # Escape unescaped " inside the value
#         content = content.replace('"', r'\"')
#         return f'"{content}"'
    
#     # Match "key": "value" patterns (even if value has newlines now normalized)
#     s = re.sub(r'("[\w\s-]+":)\s*"([^"]*?)"', escape_quotes, s, flags=re.DOTALL)

#     # 6. Replace single quotes with double quotes (for keys and string values)
#     s = re.sub(r"'([^']+)'", r'"\1"', s)

#     # 7. Remove trailing commas before ] or }
#     s = re.sub(r',\s*(\]|})', r'\1', s)
#     # 8. Remove any // or /* */ comments (rare but happens)
#     s = re.sub(r'//.*$', '', s, flags=re.MULTILINE)
#     s = re.sub(r'/\*.*?\*/', '', s, flags=re.DOTALL)

#     # 9. Final cleanup: normalize whitespace
#     s = re.sub(r'\s+', ' ', s)

#     # Try to parse
#     try:
#         return json.loads(s)
#     except json.JSONDecodeError as e:
#         print("Still failed to parse JSON. Remaining issues:")
#         print(f"Error: {e}")
#         print(f"Cleaned string:\n{s}\n")
#         # Optional: show line/col
#         raise


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


def serialize_value(value) -> str:
    """Recursively converts a value (string, list, or dict) into a flat string."""
    if isinstance(value, list):
        # Join list items with commas
        return ", ".join([serialize_value(item) for item in value])
    elif isinstance(value, dict):
        # Recursively process dict keys/values
        parts = []
        for k, v in value.items():
            # Format as "key: value"
            parts.append(f"{k}: {serialize_value(v)}")
        return "; ".join(parts)
    else:
        # Treat as a basic string
        return str(value)


