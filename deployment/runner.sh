
container_name=lambda_layer
docker_image=lambda_layer:0.1

# Build Docker image (The Dockerfile is in the parent directory, that's why we have the "../" to access it)
docker build --platform=linux/arm64 -t $docker_image .

# Run Docker image and create a container from that image
docker run --platform linux/arm64 --name $container_name -i $docker_image


docker cp ./requirements.txt $container_name:/

docker exec -i $container_name /bin/bash < ./docker_install.sh

docker cp $container_name:/python.zip python.zip

#docker stop $container_name
#docker rm $container_name