import asyncio
import logging
import random
import toml
import click
import random
import datetime

from pathlib import Path
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
    def __init__(self, id, worker_pool_size, job_confs, out_dir):
        self.id = id
        self.worker_pool_size = worker_pool_size
        self.job_confs = job_confs
        self.out_dir = out_dir

    @classmethod
    def from_toml(cls, conf):
        return cls(
            id=conf["id"],
            worker_pool_size=conf["worker_pool_size"],
            job_confs=conf["jobs"],
            out_dir=conf["out_dir"],
        )


@add_objprint
class Job:
    """Job represent some work to be done by the job executor"""

    def __init__(self, path_to_cmd: str, *args):
        self.cmd = path_to_cmd
        self.args = args

    async def run(self, stdout_path):
        with open(stdout_path, mode="w") as f:
            self.proc = await asyncio.create_subprocess_exec(
                self.cmd,
                str(self.args),
                stdout=f,
                stderr=f,
            )
        stdout, stderr = await self.proc.communicate()
        return stdout, stderr


async def worker(jobs: asyncio.Queue, id, out_dir):
    """Worker read from jobs queue executes job and writes stdout to out queue"""
    if id == None:
        logger.warn(
            "worker id is not set, generating random id, this can lead to data loss"
        )
        id = random.randrange(0, 1024)
    logger.info(f"[worker {id}] Starting...")
    try:
        while True:
            ## Wait for next job
            job = await jobs.get()
            ## Create stdout path
            path = (
                Path(out_dir)
                / str(id)
                / job.cmd
                / str(round(datetime.datetime.now().timestamp()))
            )
            if path.is_dir():
                logger.warn(f"stdout path [{path}] already exists, overwriting")
            path.mkdir(parents=True, exist_ok=True)
            path = path / "stdout.txt"
            ## process job
            logger.info(
                f"[worker {id}] proccessing command: {job.cmd}, stdout: [{path}]"
            )
            stdout, _ = await job.run(str(path.absolute()))
            jobs.task_done()
            logger.info(
                f"[worker {id}] finished proccessing command: {job.cmd}: {stdout}"
            )
    except CancelledError:
        logger.info(f"[worker {id}] Cancelling...")
        raise
    except Exception as e:
        logger.error(f"There was an unexpected error {e}")
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
    # tasks is a task pool
    tasks = [
        asyncio.create_task(worker(jobs_q, id=i, out_dir=config.out_dir))
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
