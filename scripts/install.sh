## install docker and docker compose in ubuntu
# https://docs.docker.com/install/linux/docker-ce/ubuntu/#install-using-the-repository
sudo apt-get update
sudo apt-get -y install \
    apt-transport-https \
    ca-certificates \
    curl \
    gnupg-agent \
    software-properties-common

curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo apt-key add -
sudo apt-key fingerprint 0EBFCD88
sudo add-apt-repository "deb [arch=amd64] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable"
sudo apt-get update && sudo apt-get -y install docker-ce docker-ce-cli containerd.io

# verify docker is installed successfully
sudo docker run hello-world

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

## docker postinstall
# https://docs.docker.com/install/linux/linux-postinstall/
# sudo groupadd docker
# sudo usermod -aG docker $USER
# exit

## reconnect o VM
# gcloud compute --project "youtube8m-winner" ssh --zone "europe-west4-a" "instance-1"
# activate the changes to groups
# newgrp docker
# verify
# docker run hello-world
## end docker postinstall