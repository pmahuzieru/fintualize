from typing import Dict
import os

script_dir = os.path.dirname(os.path.abspath(__file__))

def read_credentials() -> Dict[str, str]:
    
    file_path = os.path.join(script_dir, ".credentials.txt")

    credentials = {}

    with open(file_path, "r") as f:
        for attr_line in f.readlines():
            key, value = attr_line.strip().split("=")
            credentials[key.strip()] = value.strip()
    
    return credentials
