# Dog Walker

Dog walker will run some command a number of time in a random order. 

- [config](./config.toml) is managed via a toml file

## Example run

```
$ python walker.py --config ./config.toml
[2021-11-18 20:51:18,105] INFO - Reading config: ./config.toml
[2021-11-18 20:51:18,105] DEBUG - Using selector: EpollSelector
[2021-11-18 20:51:18,105] INFO - [worker 0] Starting...
[2021-11-18 20:51:18,105] INFO - [worker 0] proccessing command: ./spark-batch.sh
[2021-11-18 20:51:18,105] INFO - [worker 1] Starting...
[2021-11-18 20:51:18,105] INFO - [worker 1] proccessing command: ./spark-stream.sh
[2021-11-18 20:51:18,105] INFO - [worker 2] Starting...
[2021-11-18 20:51:18,105] INFO - [worker 2] proccessing command: ./spark-ml.sh
[2021-11-18 20:51:25,112] INFO - [worker 1] finished proccessing command: ./spark-stream.sh: b'SPARK STREAM\n'
[2021-11-18 20:51:25,113] INFO - [worker 2] finished proccessing command: ./spark-ml.sh: b'SPARK ML\n'
[2021-11-18 20:51:26,111] INFO - [worker 0] finished proccessing command: ./spark-batch.sh: b'SPARK BATCH\n'
[2021-11-18 20:51:26,111] INFO - [worker 0] Cancelling...
[2021-11-18 20:51:26,111] INFO - [worker 0] cleaning up
[2021-11-18 20:51:26,111] INFO - [worker 1] Cancelling...
[2021-11-18 20:51:26,111] INFO - [worker 1] cleaning up
[2021-11-18 20:51:26,111] INFO - [worker 2] Cancelling...
[2021-11-18 20:51:26,111] INFO - [worker 2] cleaning up
```
