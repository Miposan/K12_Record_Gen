import os
from pathlib import Path

env_path = Path(__file__).parent.parent / ".env"

if env_path.exists():
    from dotenv import dotenv_values
    values = dotenv_values(env_path)
    for k, v in values.items():
        if v is not None and len(v):
            os.environ[k] = v