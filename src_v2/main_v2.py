from __future__ import annotations
import asyncio
from .bot_v2 import run_bot_v2

def main() -> None:
    asyncio.run(run_bot_v2())

if __name__ == "__main__":
    main()
