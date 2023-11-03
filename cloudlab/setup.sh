echo "Installing Docker:"
echo "" 

sudo apt-get update
sudo apt-get -y install \
    ca-certificates \
    curl \
    gnupg lsb-release

sudo install -m 0755 -d /etc/apt/keyrings
curl -fsSLk https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg  ## download without verification. Used to work without 'k'. Not sure why it doesnt. TODO: Fix
#curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
sudo chmod a+r /etc/apt/keyrings/docker.gpg

echo \
  "deb [arch="$(dpkg --print-architecture)" signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu \
  "$(. /etc/os-release && echo "$VERSION_CODENAME")" stable" | \
  sudo tee /etc/apt/sources.list.d/docker.list > /dev/null


sudo apt-get update #installation below failed without this update

sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin

dividerAction
echo -e "Checking Docker:\n"

sudo docker run hello-world

## For docker compose
sudo apt install docker-compose


sudo apt install python3-pip
pip install -r requirements.txt




