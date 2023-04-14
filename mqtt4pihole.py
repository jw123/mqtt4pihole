"""mqtt4pihole creates an interface between Pi-hole and MQTT,
allowing certain elements to be exposed to Home Assistant as switches
"""
__version__ = '0.2.1'

import json
import os
import signal
import sys
import sqlite3
import asyncio
import logging
import selectors
import yaml
import mqtt_async as mqtt

# Friendlier handle for version, as global
MQTT4PIHOLE_VERSION = __version__

logger = logging.getLogger(__name__)


def log_decorator(func, level=logging.DEBUG):
    """Decorator to log the output of a function"""
    def wrapper(*args, **kwargs):
        result = func(*args, **kwargs)
        logger.log(level, f'Function "{func.__name__}" returned "{result}"')
        return result
    return wrapper


class EL_policy(asyncio.DefaultEventLoopPolicy):
    """Child class to use selector event loop for new event loops"""
    def new_event_loop(self):
        """New event loop policy using selector"""
        selector = selectors.SelectSelector()
        return asyncio.SelectorEventLoop(selector)


class m4p_config(dict):
    """Dictionary child class that stores config for mqtt4pihole"""
    def load_config(self) -> bool:
        """Loads the configuration from config.yaml, setting
        defaults as needed.
        """
        # There are some default values that can be applied if
        # they are not in config.yaml
        with open('config.yaml', 'r') as config_file:
            self.update(yaml.safe_load(config_file))
        self.setdefault('mqtt_port', 8883)
        self.setdefault('mqtt_ssl', True)
        self.setdefault('mqtt_keepalive', 60)
        self.setdefault('mqtt_topic', 'mqtt4pihole')
        self.setdefault('mqtt_hass_topic', 'homeassistant')
        self.setdefault('mqtt_sock_buff', 2048)
        self.setdefault('db_file', '/etc/pihole/gravity.db')
        self.setdefault('pihole_update_frequency', 10)
        self.setdefault('pihole_check_frequency', 5)
        self.setdefault('mqtt_check_frequency', 1)
        self.setdefault('ftl_pid_file', '/run/pihole-FTL.pid')
        self.setdefault('pihole_url', 'http://pi.hole/admin')
        self.setdefault('hass_tag', 'HASS')
        self.setdefault('log_level', 'INFO')

        return all([x in self.keys() for x in (
            'mqtt_user',
            'mqtt_pw',
            'mqtt_host')])


class gravity_records(dict):
    """The collection of gravity.db records of a particular type
    (e.g. domain), along with associated methods that allow
    interaction between Pi-hole and Home Assistant.
    """
    class __gravity_record:
        """A gravity.db record represented as an object"""
        def __init__(self, record: list, publish_func) -> None:
            """A gravity.db record represented as an object.

            Creation of a record sets up its initial state
            """
            # Set initial state
            self.mqtt_state = None
            self.enabled = None
            self.publish = publish_func
            self.for_hass_deletion = self.for_hass_update = False
            self.record_update(record)
            if self.__class__.__name__ == '__gravity_record':
                self.switch_type = 'generic'
                self.st_topic = (f'{config["mqtt_topic"]}/'
                                 f'{self.switch_type}_{self.id}')
                self.hass_name = f'{self.switch_type} {self.id}'
            self.changes_in_gravity = False

            # Initialise some json templates for use in mqtt messages later on
            self.__json_announce_msg = json.loads(
                '{'
                f'"avty_t": "{config["mqtt_topic"]}/state", '
                '"cmd_t": "X", '
                '"dev": "X", '
                '"ic": "mdi:pi-hole", '
                '"name": "X", '
                '"pl_off": "OFF", '
                '"pl_on": "ON", '
                '"stat_t": "X", '
                '"val_tpl": "{{ value_json.state }}", '
                '"json_attr_t": "X", '
                '"uniq_id": "X", '
                '"platform": "mqtt"'
                '}'
            )
            self.__json_state_msg = json.loads(
                '{'
                '"state": "X"'
                '}'
            )

        def __repr__(self) -> str:
            """Return the class name and its id"""
            return f'{self.__class__.__name__}("{self.id}")'

        @log_decorator
        def st_update(self) -> dict:
            """Return a dictionary for the state of the record,
            for sending to MQTT
            """
            return {
                'state': 'ON' if self.enabled == 1 else 'OFF',
            }

        @log_decorator
        def for_hass_adding(self) -> bool:
            """Indicate whether the record has already been added to
            Home Assistant via MQTT yet
            """
            return self.mqtt_state is None

        @log_decorator
        def for_pihole_update(self) -> bool:
            """Test whether the last status recieved from MQTT
            differs from that held in the current record object,
            indicating that Pi-hole needs updating
            """
            rc = (
                self.mqtt_state not in [self.enabled, None]
                and not self.changes_in_gravity)
            return rc

        # Update the record based on a gravity.db query
        def record_update(self, record: list):
            """Update the gravity record object, based on a list contiaining:
            id, type, domain, enabled, date added, date modified, and comment
            """
            old_enabled = self.enabled
            (
                self.id,
                self.enabled,
            ) = record[1:3]
            self.for_hass_update = (
                self.for_hass_update or (old_enabled is not self.enabled)
                )
            logger.debug(f'Updated record {self.id}')

        async def hass_add(self):
            """Publish a message to MQTT that adds the record object as a
            switch entity to Home Assistant
            """
            ne_topic = (f'{config["mqtt_hass_topic"]}/switch/'
                        f'{self.switch_type}_{self.id}/config')
            ne_dev = {
                'cu': config['pihole_url'],
                'ids': [f'{config["mqtt_topic"]}_control_switches'],
                'mf': 'jw123',
                'mdl': 'mqtt4pihole',
                'sw': MQTT4PIHOLE_VERSION,
                'name': f'{config["mqtt_topic"]} control switches'
            }
            ne_update = {
                'cmd_t': f'{self.st_topic}/set',
                'dev': ne_dev,
                'name': self.hass_name,
                'stat_t': self.st_topic,
                'json_attr_t': self.st_topic,
                'uniq_id': (f'{config["mqtt_topic"]}_'
                            f'{self.switch_type}_{self.id}')
            }
            logger.info('Sending new entity message for '
                        f'{self.switch_type} id {self.id}')
            self.__json_announce_msg.update(ne_update)
            logger.debug(f'Payload: {self.__json_announce_msg}')
            self.publish(topic=ne_topic, payload=json.dumps(
                self.__json_announce_msg
            ))
            self.hass_upd()
            await asyncio.sleep(
                    float(config['mqtt_check_frequency']))
            self.hass_upd()

        def hass_del(self):
            """Publish a message to MQTT that removes the entity from
            Home Assistant, corresponding to the record object
            """
            ne_topic = (f'{config["mqtt_hass_topic"]}/switch/'
                        f'{self.switch_type}_{self.id}/config')
            ne_msg = ''
            logger.info(
                'Sending remove entity message for '
                f'{self.switch_type} id {self.id}')
            self.publish(topic=ne_topic, payload=ne_msg)

        def hass_upd(self):
            """Publish a message to MQTT reflecting the state of
            the record object
            """
            logger.debug('Sending update message for '
                         f'{self.switch_type} id {self.id}')
            self.__json_state_msg.update(self.st_update())
            self.publish(
                topic=self.st_topic,
                payload=json.dumps(self.__json_state_msg)
            )

    class gravity_adlist_record(__gravity_record):
        """A gravity.db adlist record represented as an object"""
        def __init__(self, record: list, publish_func) -> None:
            """A gravity.db adlist record represented as an object.

            Creation of a record sets up its initial state
            """
            self.switch_type = 'adlist'
            super().__init__(record, publish_func)
            self.st_topic = (f'{config["mqtt_topic"]}/'
                             f'{self.switch_type}_{self.id}')
            self.hass_name = f'{self.switch_type} {self.id}: {self.address}'
            self.__json_state_msg = json.loads(
                '{'
                '"address": "X", '
                '"state": "X", '
                '"description": "X", '
                '"date_updated": "X", '
                '"number": "X", '
                '"invalid_domains": "X", '
                '"status": "X"'
                '}'
            )

        @log_decorator
        def st_update(self) -> dict:
            """Return a dictionary for the state of the record,
            for sending to MQTT
            """
            status = {
                1: 'List download was successful',
                2: 'List unchanged upstream, Pi-hole used a local copy',
                3: 'List unavailable, Pi-hole used a local copy',
                4: 'List unavailable, there is no local copy of this '
                   'list available on your Pi-hole'
            }
            return {
                'address': self.address,
                'state': 'ON' if self.enabled == 1 else 'OFF',
                'description': self.comment,
                'date_updated': self.date_updated,
                'number': self.number,
                'invalid_domains': self.invalid_domains,
                'status': status[int(self.status)]
            }

        # Update the record based on a gravity.db query
        def record_update(self, record: list):
            """Update the gravity record object, based on a list contiaining:
            id, type, domain, enabled, date added, date modified, and comment
            """
            old_enabled = self.enabled
            (
                self.id,
                self.address,
                self.enabled,
                self.comment,
                self.date_updated,
                self.number,
                self.invalid_domains,
                self.status
            ) = record[1:9]
            self.for_hass_update = (
                self.for_hass_update or (old_enabled is not self.enabled)
                )
            logger.debug(f'Updated {self.switch_type} record {self.id}')

    class gravity_domain_record(__gravity_record):
        """A gravity.db domain record represented as an object"""
        def __init__(self, record: list, publish_func) -> None:
            """A gravity.db domain record represented as an object.

            Creation of a record sets up its initial state
            """
            self.switch_type = 'domainlist'
            super().__init__(record, publish_func)
            self.st_topic = (f'{config["mqtt_topic"]}/'
                             f'{self.switch_type}_{self.id}')
            self.hass_name = f'{self.switch_type} {self.id}: {self.domain}'
            self.__json_state_msg = json.loads(
                '{'
                '"type": "X", '
                '"domain": "X", '
                '"description": "X", '
                '"state": "X"'
                '}'
            )

        @log_decorator
        def st_update(self) -> dict:
            """Return a dictionary for the state of the record,
            for sending to MQTT
            """
            return {
                'state': 'ON' if self.enabled == 1 else 'OFF',
                'type': 'whitelist' if self.type in [0, 1] else 'blacklist',
                'domain': self.domain,
                'description': self.comment
            }

        # Update the record based on a gravity.db query
        def record_update(self, record: list):
            """Update the gravity record object, based on a list contiaining:
            id, type, domain, enabled, date added, date modified, and comment
            """
            old_enabled = self.enabled
            (
                self.id,
                self.type,
                self.domain,
                self.enabled,
                self.comment
            ) = record[1:6]
            self.for_hass_update = (
                self.for_hass_update or (old_enabled is not self.enabled)
                )
            logger.debug(f'Updated {self.switch_type} record {self.id}')

    class gravity_group_record(__gravity_record):
        """A gravity.db group record represented as an object"""
        def __init__(self, record: list, publish_func) -> None:
            """A gravity.db group record represented as an object.

            Creation of a record sets up its initial state
            """
            self.switch_type = 'group'
            super().__init__(record, publish_func)
            self.st_topic = (f'{config["mqtt_topic"]}/'
                             f'{self.switch_type}_{self.id}')
            self.hass_name = f'{self.switch_type} {self.id}: {self.name}'
            self.__json_state_msg = json.loads(
                '{'
                '"state": "X", '
                '"name": "X", '
                '"description": "X"'
                '}'
            )

        @log_decorator
        def st_update(self) -> dict:
            """Return a dictionary for the state of the record,
            for sending to MQTT
            """
            return {
                'state': 'ON' if self.enabled == 1 else 'OFF',
                'name': self.name,
                'description': self.description
            }

        # Update the record based on a gravity.db query
        def record_update(self, record: list):
            """Update the gravity record object, based on a list contiaining:
            id, type, domain, enabled, date added, date modified, and comment
            """
            old_enabled = self.enabled
            (
                self.id,
                self.enabled,
                self.name,
                self.description
            ) = record[1:5]
            self.for_hass_update = (
                self.for_hass_update or (old_enabled is not self.enabled)
                )
            logger.debug(f'Updated {self.switch_type} record {self.id}')

    def __init__(self, mqtt_connection, db_connection) -> None:
        """The collection of gravity.db records of a particular type
        (e.g. domain), along with associated methods that allow
        interaction between Pi-hole and Home Assistant.

        On initiation, the MQTT connection and event loop are
        stored for future retrieval.
        """
        super().__init__()
        self.connection = mqtt_connection
        self.loop = self.connection.loop
        self.db_con = db_connection
        self.db_cur = self.db_con.cursor()
        self.exiting = False
        signal.signal(signal.SIGINT, self.exit_intended)
        signal.signal(signal.SIGTERM, self.exit_intended)

    def pop(self, key):
        logger.debug(f'Removing record {key}')
        if key not in [None, 'None_None']:
            self[key].hass_del()
            return super().pop(key)

    async def main(self) -> int:
        """Checks for MQTT messages and updates Pi-hole at a set interval,
        checks for changes to gravity.db and updates Home Assistant.
        """
        async def check_mqtt() -> int:
            """The main MQTT connection loop for receiving messages"""
            logger.debug('Starting check_mqtt loop')
            while not self.exiting:
                logger.debug('Waiting for messages...')
                try:
                    msg = await self.connection.create_message_future()
                except asyncio.CancelledError:
                    logger.debug('Aborted waiting for MQTT message')
                else:
                    topic_list = msg.topic.split('/')
                    enabledVal = {'off': 0, 'on': 1}.get(
                        msg.payload.decode().lower(), -1
                        )
                    logger.debug(f'Message received for {topic_list[1]}. '
                                 f'Set to {enabledVal}')
                    # Set the value as required
                    if enabledVal >= 0:
                        try:
                            self[
                                str(topic_list[1])
                                ].mqtt_state = enabledVal
                        except KeyError as ke:
                            logger.error('Invalid message '
                                         f'recieved for "{ke}"')
            else:
                logger.debug('Finished check_mqtt loop')
                return 0
            logger.error('Error in check_mqtt loop')
            return 0b0001000

        async def check_gravity(
                existing_state={tuple(None for x in range(7))}) -> int:
            """Repeatedly check for changes in gravity.db (i.e. by Pi-hole).

            On the first check, all tagged gravity.db are added to
            Home Assitant as switches.

            Subsequent checks only happen if Pi-hole is detected to be
            running (otherwise there is no point in checking gravity.db),
            and will update and remove these entities as required.
            """
            record_types = {
                'adlist': self.gravity_adlist_record,
                'domainlist': self.gravity_domain_record,
                'group': self.gravity_group_record
            }
            logger.debug('Starting check_gravity loop')
            while not self.exiting:
                # get the current gravity db state for comparison
                # with the old state
                new_state = []
                new_a_state = self.db_cur.execute(
                    'SELECT id, address, enabled, comment, date_updated, '
                    'number, invalid_domains, status '
                    'FROM "adlist" WHERE comment '
                    f'LIKE "{config["hass_tag"]}%";'
                    ).fetchall()
                new_state.extend([('adlist', *x) for x in new_a_state])
                del new_a_state
                new_d_state = self.db_cur.execute(
                    'SELECT id, type, domain, enabled, comment '
                    'FROM "domainlist" WHERE comment '
                    f'LIKE "{config["hass_tag"]}%";'
                    ).fetchall()
                new_state.extend([('domainlist', *x) for x in new_d_state])
                del new_d_state
                new_g_state = self.db_cur.execute(
                    'SELECT id, enabled, name, description '
                    'FROM "group" WHERE description '
                    f'LIKE "{config["hass_tag"]}%";'
                    ).fetchall()
                new_state.extend([('group', *x) for x in new_g_state])
                del new_g_state
                logger.debug(new_state)
                logger.debug(f'Got {len(new_state)} records')
                # First check that the database is the same
                # as the stored list
                if existing_state != new_state:
                    self.changes_in_gravity = True
                    # Create sets of the new and old states and then
                    # compare them using boolean logic. This works out
                    # which entities are new, removed, or changed.
                    logger.debug(
                        'State of monitored domain records has changed'
                        )
                    existing_set = set(existing_state)
                    new_set = set(new_state)
                    h_ex_ids = {x[1] for x in existing_set}
                    h_new_ids = {x[1] for x in new_set}
                    changed_or_new_ids = {
                        x[1] for x in new_set.difference(existing_set)
                        }
                    new_ids = h_new_ids.difference(h_ex_ids)
                    removed_ids = h_ex_ids.difference(h_new_ids)
                    changed_ids = changed_or_new_ids.difference(new_ids)

                    # For each new entity, announce
                    for new_entity in [
                            x for x in new_state if x[1] in new_ids
                            ]:
                        key, val = (
                            f'{new_entity[0]}_{new_entity[1]}',
                            record_types[new_entity[0]](
                                new_entity,
                                self.connection.client.publish
                            )
                        )
                        self.update({key: val})
                        await self[key].hass_add()
                    logger.debug('Switches:')
                    logger.debug(self)

                    # for each removed entity, send a remove message
                    [self.pop(f'{r_ent[0]}_{r_ent[1]}') for
                     r_ent in existing_state if r_ent[1] in removed_ids]

                    # for each changed entity, publish the new state
                    for changed_entity in [
                            x for x in new_state if x[1] in changed_ids
                            ]:
                        key = f'{changed_entity[0]}_{changed_entity[1]}'
                        self[key].record_update(changed_entity)
                        self[key].hass_upd()

                    existing_state = new_state
                    self.changes_in_gravity = False

                # There is no point in checking gravity more than once
                # if pihole isn't running. Having this loop at the end
                # ensures entities are added even if pihole isn't running.
                while self.FTL_pid() == 0 and not self.exiting:
                    await asyncio.sleep(
                        float(config['pihole_check_frequency'])
                        )
                else:
                    await asyncio.sleep(
                        float(config['pihole_check_frequency'])
                    )
            else:
                logger.debug('Finished check_gravity loop')
                return 0
            logger.error('Error in check_gravity loop')
            return 0b0010000

        async def update_pihole() -> int:
            """Update gravity.db (in response to MQTT messages)
            and attempt to reload lists in Pi-hole
            """
            logger.debug('Starting update_pihole loop')
            while not self.exiting:
                await asyncio.sleep(
                    float(config['pihole_update_frequency']))
                updates = False
                for gr in [x for x in self.values() if x.for_pihole_update()]:
                    updates = True
                    logger.debug(
                        f'Setting state to {gr.mqtt_state} '
                        f'for {gr.switch_type} switch {gr.id}')
                    self.db_cur.execute(
                        f'UPDATE "{gr.switch_type}" '
                        f'SET enabled = {gr.mqtt_state} '
                        f'WHERE id = {gr.id};')
                    self.db_con.commit()
                    # Verify what's been sent to the db and publish
                    # this back to MQTT
                    sqlcheck = self.db_cur.execute(
                        'SELECT enabled '
                        f'FROM "{gr.switch_type}" '
                        f'WHERE id = {gr.id};'
                        ).fetchall()[0][0]
                    gr.enabled = sqlcheck
                    gr.hass_upd()
                    logger.debug('Check of db confirmed that '
                                 f'state was set to {str(sqlcheck)}')

                # If the gravity db is changed, we need to reload lists
                # in dnsmasq-FTL with SIGRTMIN
                if updates and self.FTL_pid() != 0:
                    try:
                        os.kill(self.FTL_pid(), signal.SIGRTMIN)
                    except Exception:
                        logger.error('Failed to reload Pi-hole lists',
                                     exc_info=1)
            else:
                logger.debug('Finished update_pihole loop')
                return 0
            logger.error('Error in update_pihole loop')
            return 0b0100000

        async def check_pihole() -> int:
            """Check that pihole is running and note the pid,
            otherwise log an error message
            """
            logger.debug('Starting check_pihole loop')
            while not self.exiting:
                pihole_on = bool(self.FTL_pid())
                if not pihole_on:
                    logger.error('Pi-hole is not running.  '
                                 'No action will be taken')
                self.availability(pihole_on)
                await asyncio.sleep(
                    float(config['pihole_check_frequency']))
            else:
                logger.debug('Finished check_pihole loop')
                return 0
            logger.error('Error in check_pihole loop')
            return 0b1000000

        # Run tasks in continuous loop
        return sum(await asyncio.gather(
            check_gravity(),
            update_pihole(),
            check_pihole(),
            check_mqtt()))

    def exit_intended(self, *args):
        """Callback function for trapping exit signals"""
        if int(args[0]) > 0:
            logger.info(f'Signal {signal.strsignal(args[0]).upper()} '
                        'received... exiting...')
        self.availability(False)
        self.exiting = True
        self.connection.persistent = False
        self.connection.cancel_message_future()

    @log_decorator
    def FTL_pid(self) -> int:
        """Get the pid of Pi-hole"""
        rc = 0
        with open(config['ftl_pid_file'], 'r') as file:
            rc = int(file.read().rstrip())
        return rc

    def availability(self, av_on: bool = True):
        logger.debug(f'Setting availability: {av_on}')
        self.connection.client.publish(
            topic=f'{config["mqtt_topic"]}/state',
            payload='online' if av_on else 'offline')


@log_decorator
def run() -> int:
    '''The run() function is the starter for running mqtt4pihole'''

    # Load config...
    if not config.load_config():
        logger.critical('One or more of mqtt_user, mqtt_pw, '
                        'or mqtt_host is missing - cannot continue')
        sys.exit('Error loading config')

    # Set up logging. Using a dictionary with get() handles invalid config.
    log_lookup = {
        'DEBUG': logging.DEBUG,
        'INFO': logging.INFO,
        'WARNING': logging.WARNING,
        'ERROR': logging.ERROR,
        'CRITICAL': logging.CRITICAL
    }

    logging.basicConfig(
        level=log_lookup.get(config['log_level'].upper(), logging.INFO),
        format=('[%(asctime)s] '
                '{%(filename)s:%(lineno)d} '
                '%(levelname)s - %(message)s')
    )

    logger.debug(f'Config: {config}')

    # Set rc for the event that connecting to the gravity.db file fails
    # this could be that the location is incorrectly specified or a
    # problem with permissions
    rc = 4

    # Create sqlite connection.
    # Connect to database - this connection is used for the regular
    # checking for changes (made in the Pi-hole webgui)
    with sqlite3.connect(config['db_file']) as db_con:
        # Set rc for the event that there is a problem setting up the
        # asyncio event loop.
        rc = 3

        # Set event loop policy
        asyncio.set_event_loop_policy(EL_policy())
        # Create and enter asyncio loop
        with asyncio.Runner() as runner:
            loop = runner.get_loop()

            # Set rc for the event that there is a problem setting up
            # the MQTT connection.
            rc = 2

            # Create and start mqtt connection
            with mqtt.mqtt_connection(
                    loop=loop,
                    user=config['mqtt_user'],
                    pw=config['mqtt_pw'],
                    host=config['mqtt_host'],
                    port=int(config['mqtt_port']),
                    use_ssl=bool(config['mqtt_ssl']),
                    keepalive=int(config['mqtt_keepalive']),
                    sock_buff=int(config['mqtt_sock_buff']),
                    sleep_time=float(config['mqtt_check_frequency']),
                    subscribe_topic=f'{config["mqtt_topic"]}/+/set'
                    ) as m_con:
                if m_con:
                    grav_recs = gravity_records(
                        mqtt_connection=m_con,
                        db_connection=db_con
                    )
                    rc = runner.run(grav_recs.main())
                logger.info('Exiting...')
    return rc


if __name__ == '__main__':
    # Inialise config dictionary, to store all config paramaters
    # these are loaded in the 'run' function.
    config = m4p_config()
    sys.exit(run())
