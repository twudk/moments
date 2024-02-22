pgrep -f capture_parameters.py
pkill -f capture_parameters.py
source ../myenv/bin/activate
nohup python ../src/capture_parameters.py &
