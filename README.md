## Software Requirements (Mac OS X)

* Git (https://git-scm.com/downloads)
* Visual Studio Code (https://code.visualstudio.com/download)
* Docker Desktop (https://www.docker.com/products/docker-desktop)
* Clickhouse (For server development) (https://clickhouse.yandex/#quick-start)

## Local installation instructions (Mac OSX)
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

## Linux Virtual Machine installation instructions

1. Install Clickhouse:

```
sudo apt-get install dirmngr
sudo apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv E0C56BD4

echo "deb http://repo.yandex.ru/clickhouse/deb/stable/ main/" | sudo tee /etc/apt/sources.list.d/clickhouse.list
sudo apt-get update

sudo apt-get install -y clickhouse-server clickhouse-client

sudo service clickhouse-server start
clickhouse-client
```

2. Create the database: `CREATE DATABASE oec_brazil` and `exit`

3. Clone the repository: `git clone https://github.com/innerstage/oec-brazil-etl.git`

4. Write connection to database in *conns.yaml* file if it's not there.

5. Create Python 3 virtual environment: `virtualenv -p python3 py3` and activate with `source ./py3/bin/activate`

6. Install Bamboo lib y Bamboo cli: `pip install dw-bamboo-cli`

7. Download one year of information:

```
cd oec-brazil-etl
source ./py3/bin/activate
python file_downloader.py "EXP" "2019"
```

## Running the scripts

