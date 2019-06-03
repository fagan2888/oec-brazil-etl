## Software Requirements

* Git (https://git-scm.com/downloads)
* Visual Studio Code (https://code.visualstudio.com/download)
* Docker Desktop (https://www.docker.com/products/docker-desktop)
* Clickhouse (For server development) (https://clickhouse.yandex/#quick-start)

## Local Installation Instructions
1. Clone this repository with `git clone https://github.com/innerstage/oec-brazil-etl.git` and enter the folder with `cd oec-brazil-etl`
2. Create a Python 3 virtual environment in this folder: `virtualenv -p python3 py3` and activate it with `source ./py3/bin/activate` 
3. Install Python requirements: ```pip install -r requirements.txt```
4. Create Clickhouse Docker Container with `docker run -d --name clickhouse-local -p 8123:8123 -p 9000:9000 --ulimit nofile=262144:262144 yandex/clickhouse-server`
5. Create database "oec-brazil" by following these commands: 
```
sudo docker exec -it clickhouse-client bash
clickhouse-client
CREATE DATABASE "oec-brazil"
exit
```
6. Make Clickhouse connection in DBeaver to visualize the data.

## Running the scripts

