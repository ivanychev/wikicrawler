import re

import aiohttp
import asyncpg

from typing import NamedTuple

class Task(NamedTuple):
    url: str
    conn: asyncpg.Connection
    mask: re.Pattern
    http_client: aiohttp.ClientSession
    visited: set

    def clone_with_url(self, url: str):
        return Task(url=url, conn=self.conn, mask=self.mask,
                    http_client=self.http_client, visited=self.visited)