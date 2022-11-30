#!/bin/bash
git clone https://github.com/rcamilo1526/fast-api-data.git 
cd fast-api-data
sudo docker build -t data_api:0.1 .
nohup sudo docker run -p 8000:8000 --name my-api data_api:0.1 > my.log 2>&1 &
