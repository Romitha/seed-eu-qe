if [ "$#" -ne 2 ]; then
    echo "Usage: etl.sh <environment> <file_names>"
    exit 1
fi
environment="$1"
file_names="$2"
echo "Starting container.."
containerId=$(podman run --network host --cgroup-manager=cgroupfs -d --user appuser test_auto_mvp_1:latest tail -f /dev/null)
if [[ -z "$containerId" ]]; then
    echo "Error: Failed to start the container."
    exit 1
fi
echo "Container started with ID: $containerId"
echo "Executing command.."
if podman exec $containerId bash -c "
aws configure set region eu-west-1 &&
python3 main.py --run_mode etl --test_env \"$environment\" --file_names \"$file_names\""
then
  echo "Command executed successfully"
  echo "Stopping container.."
  podman stop --time 5 $containerId
  while podman ps -a | grep -q $containerId; do
    echo 'Waiting for container ${containerId} to stop...'
    sleep 1
  done
  echo "Container stopped successfully."
  podman rm $containerId
  echo "Container removed successfully."
  exit 0
else
  echo "Command failed"
  echo "Stopping container.."
  podman stop --time 5 $containerId
  while podman ps -a | grep -q $containerId; do
    echo 'Waiting for container ${containerId} to stop...'
    sleep 1
  done
  echo "Container stopped successfully."
  podman rm $containerId
  echo "Container removed successfully."
  exit 1
fi