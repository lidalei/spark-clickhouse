gcloud compute --project "youtube8m-winner" ssh --zone "europe-west4-a" "instance-1"

### The following commands are run in VM
## install docker
# https://docs.docker.com/install/linux/docker-ce/ubuntu/#install-using-the-repository
sudo apt-get update
sudo apt-get install \
    apt-transport-https \
    ca-certificates \
    curl \
    gnupg-agent \
    software-properties-common

curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo apt-key add -

sudo apt-key fingerprint 0EBFCD88

sudo add-apt-repository "deb [arch=amd64] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable"

sudo apt-get update && sudo apt-get install docker-ce docker-ce-cli containerd.io

# verify docker is installed successfully
sudo docker run hello-world
## end docker install

## docker postinstall
# https://docs.docker.com/install/linux/linux-postinstall/
sudo groupadd docker
sudo usermod -aG docker $USER
exit
### The previous commands are run in VM

# restart VM
# reconnect to VM
gcloud compute --project "youtube8m-winner" ssh --zone "europe-west4-a" "instance-1"
### The following commands are run in VM
# activate the changes to groups
newgrp docker
# verify
docker run hello-world
## end docker postinstall

## install docker-compose 
# https://docs.docker.com/compose/install/
sudo curl -L "https://github.com/docker/compose/releases/download/1.25.0/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
sudo chmod +x /usr/local/bin/docker-compose
# sudo ln -s /usr/local/bin/docker-compose /usr/bin/docker-compose

## check if docker service starts on boot
# systemctl list-unit-files | grep enabled
# enable / disable this behavior
# sudo systemctl enable docker
# sudo systemctl disable docker
## end check if docker service starts on boot

# install python
sudo apt-get install python3.7 python3-pip
# install jdk 11
# sudo apt-get install default-jdk

docker run -d --name local-clickhouse-server --ulimit nofile=262144:262144 -p 9000:9000 yandex/clickhouse-server
# attach to the container
# docker ps -all
# assume clickhouse container id is ec9debd359e1
# docker exec -it ec9debd359e1 /bin/bash

# clone repository
git clone git@github.com:lidalei/spark-clickhouse.git
cd spark-clickhouse
python3.7 -m pip install -r requirements.txt --user

### The previous commands are run in VM


# after renaming / moving a git repository
# git remote set-url origin git@github.com:lidalei/spark-clickhouse.git