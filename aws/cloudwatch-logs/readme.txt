steps to update cloudwatch logs:

1) clone this repo: https://github.com/awslabs/ecs-cloudwatch-logs
2) make any necessary updates to ../cloudformation/logs.json
3) update awslogs.conf in this folder (e.g. use YOUR log group name from step 2)
4) copy awslogs.conf from this folder to the ecs-cloudwatch-logs root
5) cd ecs-cloudwatch-logs
6) docker build -f Dockerfile -t ${YOUR_CLOUDWATCH_LOGS_DOCKER_IMAGE} .
7) docker push ${YOUR_CLOUDWATCH_LOGS_DOCKER_IMAGE}
8) update your dart configs to reference this docker image


Keep an eye out for ECS to support the awslogs logDriver: https://github.com/aws/amazon-ecs-agent/issues/9
