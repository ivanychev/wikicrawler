import aiohttp
import asyncio
import asyncpg
import contextlib
import http
import logging
import json
import re

import furl
import sqlalchemy
import uvloop
from sqlalchemy_utils import database_exists, create_database
from bs4 import BeautifulSoup

from links_table import links, URL_MAX_LENGTH
from task import Task
from task_queue import TaskQueue

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

CONFIG_JSON_PATH = "config.json"
CRAWLER_DATABASE_NAME = "webcrawler"
LINKS_TABLE_NAME = "links"
URL_MASK_REGEX = re.compile(r".*wikipedia\.ru.*")
CONSUMER_COUNT = 100


def initialize_db(config):
    postgres_host = config["postgres_host"]
    postgres_port = config["postgres_port"]
    engine = sqlalchemy.create_engine(
        f"postgres://{postgres_host}:{postgres_port}/{CRAWLER_DATABASE_NAME}")
    if not database_exists(engine.url):
        logger.info("Database doesn't exist, creating....")
        create_database(engine.url)
    if not engine.dialect.has_table(engine, LINKS_TABLE_NAME):
        logger.info("Links table doesn't exist, creating....")
    links.create(engine, checkfirst=True)


def mask_match(url: str, mask: re.Pattern):
    match = False
    for _ in mask.finditer(url):
        match = True
        break
    return match

@contextlib.asynccontextmanager
async def get_task(task_queue: TaskQueue):
    task = await task_queue.get_task()
    try:
        yield task
    finally:
        task_queue.task_done()

async def task_consumer(consumer_index: int, task_queue: TaskQueue):
    logger.info("Consumer %s is in play!", consumer_index)
    while True:
        async with get_task(task_queue) as task:
            parsed_url = furl.furl(task.url)
            if task.url in task.visited:
                continue
            else:
                task.visited.add(task.url)
            # logger.info("%s got task with %s", consumer_index, task.url)
            async with task.http_client.get(task.url) as resp:
                if resp.status != http.HTTPStatus.OK:
                    continue
                soup = BeautifulSoup(await resp.text(), "lxml")
                urls = set(a['href'] for a in soup.find_all('a', href=True))
                # logger.info("Got %s urls", len(urls))
                for url in urls:
                    parsed = furl.furl(url).remove(
                        query_params=True, fragment_args=True, fragment_path=True)
                    cleaned_url = str(parsed)
                    is_absolute = bool(parsed.host)
                    new_url_to_crawl = None
                    # logger.info("%s: Url: %s", consumer_index, url)
                    if is_absolute and mask_match(cleaned_url, task.mask):
                        new_url_to_crawl = cleaned_url
                        # logger.info("%s: Absolute: %s", consumer_index, new_url_to_crawl)
                    elif not is_absolute:
                        new_url_to_crawl = str(parsed_url.copy().remove(path=True) / cleaned_url)
                        # logger.info("%s: Constructed new %s ...", consumer_index, new_url_to_crawl)
                    if (new_url_to_crawl and new_url_to_crawl not in task.visited and
                            len(new_url_to_crawl) <= URL_MAX_LENGTH):
                        # logger.info("%s: Pushing %s ...", consumer_index, new_url_to_crawl)
                        await task.conn.execute('''
                                INSERT INTO links(url_from, url_to, "count") VALUES($1, $2, 1)
                                ON CONFLICT DO NOTHING
                            ''', task.url, new_url_to_crawl)
                        await task_queue.add_task(task.clone_with_url(new_url_to_crawl))
        # await asyncio.sleep(1)


async def main():
    with open(CONFIG_JSON_PATH) as f:
        raw_config = f.read()
        config = json.loads(raw_config)
        logger.info("Read config: %s", config)
    initialize_db(config)
    conn = await asyncpg.connect(user="postgres", password="postgres",
                                 database=CRAWLER_DATABASE_NAME,
                                 host=config["postgres_host"],
                                 port=config["postgres_port"])
    url_mask = re.compile(config["url_mask_regex"])
    start_urls = config["start_urls"]
    visited = set()
    async with aiohttp.ClientSession() as client:
        tasks = [Task(url, conn, url_mask, client, visited) for url in start_urls]
        task_queue = TaskQueue.create(tasks)
        consumers = [asyncio.create_task(task_consumer(i, task_queue))
                     for i in range(CONSUMER_COUNT)]
        await task_queue.join()  # Implicitly awaits consumers, too
        await asyncio.gather(consumers)

def custom_exception_handler(loop, context):
    # first, handle with default handler
    print("!!!")
    loop.default_exception_handler(context)

    exception = context.get('exception')
    print(exception)

loop = uvloop.new_event_loop()
asyncio.set_event_loop(loop)
loop.set_exception_handler(custom_exception_handler)
asyncio.run(main())
