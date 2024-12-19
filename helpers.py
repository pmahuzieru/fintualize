from typing import Dict

def read_credentials() -> Dict[str, str]:
    file_path = ".credentials.txt"

    credentials = {}

    with open(file_path, "r") as f:
        for attr_line in f.readlines():
            key, value = attr_line.strip().split("=")
            credentials[key.strip()] = value.strip()
    
    return credentials
