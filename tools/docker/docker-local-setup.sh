#!/bin/sh -ex

docker-machine create --driver virtualbox "docker-dart" || echo "docker-dart has alreay been created... skipping."

[ $(vboxmanage list runningvms | grep "docker-dart" | wc -l) -gt 0 ] && vboxmanage controlvm "docker-dart" acpipowerbutton

while [ $(vboxmanage list runningvms | grep "docker-dart" | wc -l) -gt 0 ]; do sleep 2; done


# remove any dart port forwarding rules if they exist
for RULE in $(vboxmanage showvminfo "docker-dart" | grep "dart-port-forward-" | perl -pi -e 's/^.*name = (.+?),.*$/\1/')
do
    vboxmanage modifyvm "docker-dart" --natpf1 delete "${RULE}"
done

# then set them up again
vboxmanage modifyvm "docker-dart" --natpf1 "dart-port-forward-2376,tcp,,2376,,2376" # docker
vboxmanage modifyvm "docker-dart" --natpf1 "dart-port-forward-5432,tcp,,5432,,5432" # postgres
vboxmanage modifyvm "docker-dart" --natpf1 "dart-port-forward-9324,tcp,,9324,,9324" # elasticmq
vboxmanage modifyvm "docker-dart" --natpf1 "dart-port-forward-8080,tcp,,8080,,8080" # nginx/flask
