import asyncio
import logging
import random
import toml
import sys
import click
import pprint

from asyncio import CancelledError
from objprint import add_objprint

logging_format = "%(levelname)s %(asctime)s = %(message)s"
logging.basicConfig(
    level=logging.DEBUG,
    format="[%(asctime)s] %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

Config = None


def read_config(path):
    logger.info(f"Reading config: {path}")
    try:
        return Configuration.from_toml(toml.load(path))
    except:
        logger.error("error reading config file")
        raise


@add_objprint
class Configuration:
    def __init__(self, id, worker_pool_size, job_confs):
        self.id = id
        self.worker_pool_size = worker_pool_size
        self.job_confs = job_confs

    @classmethod
    def from_toml(cls, conf):
        return cls(conf["id"], conf["worker_pool_size"], conf["jobs"])


@add_objprint
class Job:
    """Job represent some work to be done by the job executor"""

    def __init__(self, path_to_cmd: str, *args):
        self.cmd = path_to_cmd
        self.args = args

    async def run(self):
        await asyncio.sleep(random.randrange(1, 10))
        self.proc = await asyncio.create_subprocess_exec(
            self.cmd,
            str(self.args),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await self.proc.communicate()
        return stdout, stderr


async def worker(jobs: asyncio.Queue, out: asyncio.Queue, id=0):
    """Worker read from jobs queue executes job and writes stdout to out queue"""
    logger.info(f"[worker {id}] Starting...")
    try:
        while True:
            job = await jobs.get()
            ## process job
            logger.info(f"[worker {id}] proccessing command: {job.cmd}")
            stdout, _ = await job.run()
            jobs.task_done()
            logger.info(
                f"[worker {id}] finished proccessing command: {job.cmd}: {stdout}"
            )
    except CancelledError:
        logger.info(f"[worker {id}] Cancelling...")
        raise
    finally:
        logger.info(f"[worker {id}] cleaning up")


async def handler(config: Configuration):
    jobs_list = []
    for job in config.job_confs:
        jobs_list.extend(
            [Job(job["path_to_cmd"], None) for _ in range(job["number_of_jobs"])]
        )
    random.shuffle(jobs_list)
    jobs_q = asyncio.Queue(maxsize=len(jobs_list))
    out = asyncio.Queue()
    # tasks is a task pool
    tasks = [
        asyncio.create_task(worker(jobs_q, out, id=i))
        for i in range(config.worker_pool_size)
    ]
    for job in jobs_list:
        jobs_q.put_nowait(job)
    await jobs_q.join()
    for task in tasks:
        if not task.done():
            task.cancel()
    while True:
        if all([task.done() for task in tasks]):
            break
        await asyncio.wait(tasks)


@click.command()
@click.option(
    "--config",
    "-c",
    "config_path",
    default="config.toml",
    help="path to config file",
    type=click.Path(),
)
def start(config_path):
    config = None
    try:
        config = read_config(config_path)
    except:
        raise
    asyncio.run(handler(config))


if __name__ == "__main__":
    start()
