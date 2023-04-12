# mqtt4pihole

mqtt4pihole creates an interface between Pi-hole and MQTT, allowing certain elements to be exposed to Home Assistant as switches.

A switch is added to Home Assistant for any adlist, domain list, or group in Pi-hole having a comment/description that starts 'HASS'.

## Installation
This has been tested (a little bit) on a Raspberry Pi 4 running Raspbian Bullsye with Python3.11 in a virtual environment.
Mileage with other setups, will probably vary.
Python3.11 is needed as asyncio 'runner' context manager is not implemented in earlier versions.
The script must be run as the pihole user (i.e. pihole), or failing that, as root.

1. Copy the mqtt4pihole.py, mqtt_async.py, and config.yaml files into a local directory.
2. Edit the config.yaml file.
3. Create a virtual environment in the local directory using `venv -m .`.
4. Make sure all files and directories are owned by pihole.
5. Activate the virtual environment, e.g. `source .bin/activate`
6. Install dependant python packages, e.g. `pip install pyyaml paho-mqtt`
7. Run the script, ideally as a pihole daemon, but can be tested using root, e.g. `sudo python mqtt4pihole.py`