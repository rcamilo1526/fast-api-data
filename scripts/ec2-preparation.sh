sudo yum update -y
sudo amazon-linux-extras install docker
sudo service docker start
sudo yum install git -y
git clone https://github.com/rcamilo1526/globant_test.git
sudo docker build -t data_api:0.1 .
sudo docker run -d  -p 8000:8000 --name my-api data_api:0.1
docker run -p 8000:8000 --name my-api data_api:0.1