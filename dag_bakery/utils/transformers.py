import re


def lowercase(s) -> str:
    return str(s).lower()


def clean_key(string: str) -> str:
    string = lowercase(string)
    if not string:
        return string
    string = string.strip()
    return re.sub(r"[\W]+", "_", string)
