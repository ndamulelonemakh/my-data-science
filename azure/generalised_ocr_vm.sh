#cloud-config
package_update: true
packages:
  - python3.9
  - poppler-utils


runcmd:
  - sudo add-apt-repository "ppa:alex-p/tesseract-ocr-devel"
  - sudo apt-get update
  - sudo apt-get install -y tesseract-ocr

# SSH into the VM
# Run this inside the vm, then exit
sudo waagent -deprovision+user


# Exit the ssh session, and run the following
vm=tesseract-vm-04
rg=tesseract-vm-image-rg
imageName=reliableServImage

az vm deallocate -g $rg -n $vm
az vm generalize  -g $rg -n $vm
az image create -g $rg --name $imageName  --source $vm --hyper-v-generation V2

# Verify
# By default when you ssh, the packages installed before should be available
az vm create --resource-group $rg --name myocrvmfromtemplate --image $imageName --admin-username ndamu --ssh-key-value ~/.ssh/id_rsa.pub
