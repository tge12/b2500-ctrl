# b2500-ctrl
Control a Marstek B2500D storage manually (not in auto-mode) by continuously requesting total consumption/feed-in and adjusting the output power of the storage as needed.
This is based on https://github.com/tomquist/hame-relay (to talk to the B2500D) and (in my case, anything else is fine too) Volkszaehler (https://volkszaehler.org) to measure and calculate the total
consumption. A local MQTT server is needed as well.
The script b2500d_ctrl.pl must run continuously (control via service file), it will listen for current MQTT status responses from b2500d and poll the VZ server for current
consumption - from this calculate the actual energy demand and adjust the (time-based) storage output power.

The b2500d_mqtt.pl script can be started from a vzlogger (or whichever way else) and will request a status string from the storage via MQTT. The extracted metric values will be sent to the VZ server.
At the same time the b2500d_ctrl.pl script just subsribes to the same status responses and consumes them as described above.
