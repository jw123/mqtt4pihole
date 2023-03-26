"""A module that provides a means of creating a MQTT client and
connecting it to a broker on an external event loop using asyncio,
rather than using a different thread.

This is based on the loop_asyncio.py example provided as part of the
Eclipse Paho MQTT Python Client (:mod:`paho.mqtt.client`).

Copyright (c) 2007, Eclipse Foundation, Inc. and its licensors.

All rights reserved.

The Eclipse Paho MQTT Python Client is dual licensed under the
Eclipse Public License 2.0 and the Eclipse Distribution License 1.0.

Both the orignal source code and Licenses for the Eclipse Paho MQTT
Python Client can be found at https://github.com/eclipse/paho.mqtt.python
"""
import asyncio
import logging
import socket
import paho.mqtt.client as mqtt


class AsyncioHelper:
    """A helper class that sets the callback functions
    for an MQTT connection.
    """
    def __init__(self, loop, client, mqtt_sleep):
        """A helper class that sets the callback functions
        for an MQTT connection.

        Upon initiation, the callback functions are set to
        allow read and write to MQTT using asyncio.
        """
        self.loop = loop
        self.client = client
        self.client.on_socket_open = self.on_socket_open
        self.client.on_socket_close = self.on_socket_close
        self.client.on_socket_register_write = self.on_socket_register_write
        self.client.on_socket_unregister_write = (
            self.on_socket_unregister_write
            )
        self.mqtt_sleep = mqtt_sleep

    def on_socket_open(self, client, userdata, sock):
        """Callback function for when the socket has been
        opened.

        Adds a reader callback for the socket.

        Ensures that the MQTT client loop_misc() is called
        frequenty.
        """
        logging.info('Socket opened')

        def cb():
            logging.debug('Socket is readable, calling loop_read')
            client.loop_read()

        self.loop.add_reader(sock, cb)
        self.misc = self.loop.create_task(self.misc_loop())

    def on_socket_close(self, client, userdata, sock):
        """Callback function for when the socket has been
        closed.

        Removes the reader and stops calling loop_misc().
        """
        logging.info('Socket closed')
        if not self.misc.done():
            self.loop.remove_reader(sock)
            self.misc.cancel()

    def on_socket_register_write(self, client, userdata, sock):
        """Callback function for socket write register"""
        logging.debug('Watching socket for writability.')

        def cb():
            logging.debug('Socket is writable, calling loop_write')
            client.loop_write()

        self.loop.add_writer(sock, cb)

    def on_socket_unregister_write(self, client, userdata, sock):
        """Callback function for socket write unregister"""
        logging.debug('Stop watching socket for writability.')
        self.loop.remove_writer(sock)

    async def misc_loop(self):
        """Calls the MQTT client loop_misc() function at regular intervals"""
        logging.debug('misc_loop started')
        while self.client.loop_misc() == mqtt.MQTT_ERR_SUCCESS:
            try:
                await asyncio.sleep(self.mqtt_sleep)
            except asyncio.CancelledError:
                break
        logging.debug('misc_loop finished')


class mqtt_connection:
    """A class that presents an MQTT connection as an object."""
    def __init__(
            self,
            loop,
            user: str,
            pw: str,
            host: str,
            port: int,
            use_ssl: bool,
            keepalive: int = 60,
            sock_buff: int = 2048,
            sleep_time: float = 1,
            subscribe_topic: str = '#'):
        """A class that presents an MQTT connection as an object.

        When initialised, the object stores the intended event loop
        but does not connect.
        """
        self.loop = loop
        self.disconnect_intended = False
        self.subscribe_topic = subscribe_topic
        self.mqtt_user = user
        self.mqtt_pw = pw
        self.mqtt_host = host
        self.mqtt_port = port
        self.ssl = use_ssl
        self.mqtt_keepalive = keepalive
        self.mqtt_sock_buff = sock_buff
        self.mqtt_sleep = sleep_time

    def __enter__(self):
        """Open a connection"""
        logging.debug('Opening connection')
        self.connect()
        return self

    def __exit__(self, *args):
        """Close the connection"""
        logging.debug('MQTT Connection object disconnecting')
        self.disconnect_intended = True
        self.client.disconnect()
        if not self.aioh.misc.done():
            self.loop.remove_reader(self.client.socket())
            self.aioh.misc.cancel()

    def on_connect(self, client, userdata, flags, rc):
        """Callback function for when a MQTT connection is made.

        Subscribes to a MQTT topic.
        """
        logging.info('Connected with result code '+str(rc))
        logging.info('Subscribing...')
        try:
            client.subscribe(self.subscribe_topic)
        except Exception:
            logging.critical('MQTT client failed to subscribe')

    def on_message(self, client, userdata, msg):
        """Callback function for when a MQTT message is received."""
        if not self.got_message:
            logging.warning(f'Got unexpected message: {msg.payload.decode()}')
        else:
            self.got_message.set_result(msg)

    def on_disconnect(self, client, userdata, rc):
        """Callback function fow when the MQTT connection ends.

        Reconnect if this was not intended.
        """
        if self.disconnect_intended:
            self.disconnected.set_result(rc)
        else:
            logging.info(f'Unintentional MQTT disconnection (code {rc}).')
            logging.info('Reconnecting...')
            self.connect()

    def connect(self):
        """Create a new MQTT client object, attach it to the connection
        object, and connect to the broker.
        """
        self.disconnected = self.loop.create_future()
        self.got_message = None

        self.client = mqtt.Client()
        if self.ssl:
            self.client.tls_set()
        self.client.username_pw_set(self.mqtt_user, self.mqtt_pw)
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        self.client.on_disconnect = self.on_disconnect

        self.aioh = AsyncioHelper(
                self.loop,
                self.client,
                self.mqtt_sleep)

        self.__client_connect()

    def __client_connect(self):
        self.client.connect(
            self.mqtt_host,
            self.mqtt_port,
            self.mqtt_keepalive
            )
        self.client.socket().setsockopt(
            socket.SOL_SOCKET,
            socket.SO_SNDBUF,
            self.mqtt_sock_buff
            )

    def create_message_future(self):
        """Creates and returns a future that points to
        a message that is recieved.  This allows a coroutine
        to await a message and then respond accordingly.
        """
        self.got_message = self.loop.create_future()
        return self.got_message
