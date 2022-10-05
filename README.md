# Pyspark install and get data from mongodb 

$ sudo apt install python3-pip

1. sudo apt install default-jdk scala git -y
2. java -version; javac -version; scala -version; git --version
3. anaconda

$ cd /tmp
$ sudo apt install curl
$ curl
https://repo.anaconda.com/archive/Anaconda3-2020.02-Linux-x86_64.sh
--output anaconda.sh
$ sha256sum anaconda.sh
output: 2b9f088b2022edb474915d9f69a803d6449d5fdb4c303041f60ac4aefcc208bb
anaconda.sh

$ bash anaconda.sh
<yes>
<yes>

## opcional
## look for versions of python
(base) $ conda search "^python$"
## assing a virtual environment and install the last version of python
(base) $ conda create --name my_env python=3
## activate virtual environment
(base) $ conda activate my_env
(base) $ python --version
     output: 3.9.1 for this ... my_env
## start anaconda
$ source ~/.bashrc
## run anaconda
$ anaconda-navigator
## desactivate anaconda
(base) $ conda deactivate

4. download spark
https://spark.apache.org/downloads.html

tar xvf spark-3.2.2-bin-hadoop3.2.tgz
sudo mv spark-3.2.2-bin-hadoop3.2 /home/javier/spark

5. run spark
cd /spark/bin/
./pyspark

http://localhost:4040/jobs/

6. configure spark. version 3.2.2
nano ~/.bash_profile
-add these lines
     export SPARK_HOME=~/spark/
     export PATH="$SPARK_HOME/bin:$PATH"

source ~/.bash_profile

export SPARK_HOME=/home/javier/spark
export PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.9-src.zip:$PYTHONPATH
export PATH=$SPARK_HOME/bin:$SPARK_HOME/python:$PATH

$ pip install pyspark==3.2.2
$ pip install findspark

# run mongodb
$ sudo service mongod start
$ mongdb-compass
