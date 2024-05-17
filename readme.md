# reliable, efficient and idempotent processing of laborious, runtime-error-ridden jobs

This project demonstrates idea of leveraging an RDBMS (PostgreSQL here) to achieve job distribution without the use of message queues.

## How to run

### Prerequisites

- Go (1.22)
- Docker
- GNU make utility (optional, it only makes the commands shorter)

To run the project, use the following command
```shell
make run
```
to use the default config. Check out the top part of [main.go](main.go), struct `config` for the list of environment variables.

To override some of them, run it like
```shell
SEED_SIZE=1000 make run
```
