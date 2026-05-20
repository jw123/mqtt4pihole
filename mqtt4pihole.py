"""mqtt4pihole creates an interface between Pi-hole and MQTT,
allowing certain elements to be exposed to Home Assistant as switches
"""
__version__ = '0.4.0'

import json
import os
import signal
import socket
import sys
import sqlite3
import asyncio
import tomllib
import logging
import selectors
import yaml
import mqtt_async as mqtt
import time

logger = logging.getLogger(__name__)


def _startstate_off(value) -> bool:
    """Return True when a dnsrecords.yaml startstate value means 'off'.

    Handles PyYAML's YAML 1.1 booleans (`on`/`off` -> True/False) as
    well as string spellings.
    """
    if value is None:
        return False
    if isinstance(value, bool):
        return not value
    return str(value).strip().lower() in ('off', 'false', 'no', '0')


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
        self.setdefault('pihole_toml', '/etc/pihole/pihole.toml')
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
                'sw': __version__,
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
            self.publish(topic=ne_topic,
                         retain=True,
                         payload=json.dumps(self.__json_announce_msg)
                         )
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

    class dns_record(__gravity_record):
        """A local DNS record (an A record with optional associated
        CNAMEs, or a standalone CNAME) represented as a single switch.

        Local DNS records live in pihole.toml (dns.hosts and
        dns.cnameRecords), not in gravity.db, so updates happen via
        check_dns_records rather than update_pihole.
        """
        def __init__(self, record: list, publish_func) -> None:
            self.switch_type = 'dns'
            super().__init__(record, publish_func)
            self.st_topic = (f'{config["mqtt_topic"]}/'
                             f'{self.switch_type}_{self.id}')
            self.hass_name = (f'{self.switch_type} {self.id}: '
                              f'{self.domain_name}')

        @log_decorator
        def st_update(self) -> dict:
            """Return the state dict for sending to MQTT."""
            base = {
                'state': 'ON' if self.enabled == 1 else 'OFF',
                'type': self.dns_type,
                'domain_name': self.domain_name,
            }
            if self.dns_type == 'A':
                base['ip_address'] = self.ip_address
                base['c_names'] = self.c_names
            else:
                base['target'] = self.target
            return base

        def record_update(self, record: list):
            """Update from a list:
            ['dns', id, dns_type, enabled, domain_name,
             ip_address, c_names, target]
            """
            old_enabled = self.enabled
            (
                self.id,
                self.dns_type,
                self.enabled,
                self.domain_name,
                self.ip_address,
                self.c_names,
                self.target,
            ) = record[1:8]
            self.for_hass_update = (
                self.for_hass_update or (old_enabled is not self.enabled)
            )
            logger.debug(f'Updated {self.switch_type} record {self.id}')

        def toml_entries(self) -> tuple:
            """Return (hosts_lines, cname_lines) this record contributes
            to pihole.toml when enabled.
            """
            if self.dns_type == 'A':
                hosts = [f'{self.ip_address} {self.domain_name}']
                cnames = [
                    f'{c},{self.domain_name}'
                    for c in (self.c_names or [])
                ]
            else:
                hosts = []
                cnames = [f'{self.domain_name},{self.target}']
            return (hosts, cnames)

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
        self.pihole_on = False
        self.reinitialise = False
        self.dnsrecs = {}
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
                    if f'{config["mqtt_hass_topic"]}/status' in msg.topic:
                        self.reinitialise = (
                            msg.payload.decode().lower() == 'online'
                            )
                    elif config['mqtt_topic'] in msg.topic:
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
                                logger.warning('Invalid message '
                                               f'recieved for "{ke}"')
                # Check that mqtt is still connected, otherwise exit.
                if self.connection.disconnected.done():
                    self.exit_intended()
                    return 0b0000100
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
                # Clear the existing state if we need to reinitialise
                if self.reinitialise:
                    existing_state = {tuple(None for x in range(7))}
                    self.reinitialise = False
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
                while not self.pihole_on and not self.exiting:
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
            and attempt to reload lists in Pi-hole.

            DNS records live in pihole.toml, not gravity.db, and are
            handled by check_dns_records, so they are skipped here.
            """
            logger.debug('Starting update_pihole loop')
            while not self.exiting:
                await asyncio.sleep(
                    float(config['pihole_update_frequency']))
                updates = False
                for gr in [
                    x for x in self.values()
                    if x.for_pihole_update() and x.switch_type != 'dns'
                ]:
                    updates = True
                    logger.debug(
                        f'Setting state to {gr.mqtt_state} '
                        f'for {gr.switch_type} switch {gr.id}')
                    self.db_cur.execute(
                        f'UPDATE "{gr.switch_type}" '
                        f'SET enabled = {gr.mqtt_state} '
                        f'WHERE id = {gr.id};')
                    self.db_con.commit()
                    sqlcheck = self.db_cur.execute(
                        'SELECT enabled '
                        f'FROM "{gr.switch_type}" '
                        f'WHERE id = {gr.id};'
                    ).fetchall()[0][0]
                    gr.enabled = sqlcheck
                    logger.debug('Check of db confirmed that '
                                 f'state was set to {str(sqlcheck)}')
                ftl_pid = self.FTL_pid()
                if updates and ftl_pid != 0:
                    for gr in self.values():
                        gr.hass_upd()
                    try:
                        os.kill(ftl_pid, signal.SIGRTMIN)
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
            otherwise log a message
            """
            logger.debug('Starting check_pihole loop')
            # Require this many consecutive "not running" results before
            # declaring offline, to avoid brief FTL restarts (triggered by
            # SIGRTMIN during list reloads) causing spurious unavailability.
            offline_threshold = 2
            pihole_off_count = offline_threshold
            while not self.exiting:
                pihole_on_check = bool(self.FTL_pid())
                if not pihole_on_check:
                    logger.warning('Pi-hole is not running.  '
                                   'No action will be taken')
                    pihole_off_count = min(
                        pihole_off_count + 1, offline_threshold)
                else:
                    pihole_off_count = 0
                effective_on = (
                    pihole_on_check or pihole_off_count < offline_threshold)
                if effective_on != self.pihole_on:
                    self.availability(effective_on)
                    self.pihole_on = effective_on
                await asyncio.sleep(
                    float(config['pihole_check_frequency']))
            else:
                logger.debug('Finished check_pihole loop')
                return 0
            logger.error('Error in check_pihole loop')
            return 0b1000000

        async def check_dns_records() -> int:
            """Load dns records from dnsrecords.yaml, expose them as
            Home Assistant switches, and reconcile pihole.toml's
            dns.hosts and dns.cnameRecords arrays when switches toggle.

            Records are loaded once at startup (they are configured by
            the user via dnsrecords.yaml, not by Pi-hole itself).
            """

            async def get_ip(domain: str) -> str:
                try:
                    return await asyncio.to_thread(
                        socket.gethostbyname, domain
                    )
                except socket.gaierror:
                    logger.warning(f'Failed to lookup IP for {domain}, '
                                   'returning 0.0.0.0')
                    return '0.0.0.0'

            self.dnsrecs = {}
            try:
                with open('dnsrecords.yaml', 'r') as f:
                    self.dnsrecs = yaml.safe_load(f) or {}
                logger.debug('dnsrecords.yaml file loaded')
            except FileNotFoundError:
                logger.info('No dnsrecords.yaml file')
            except yaml.YAMLError:
                logger.warning('Error parsing dnsrecords.yaml, '
                               'skipping...', exc_info=1)
                self.dnsrecs = {}

            rec_id = 0
            a_section = self.dnsrecs.get('dnsrecords') or {}
            if isinstance(a_section, dict):
                for domain, spec in a_section.items():
                    if isinstance(spec, str):
                        spec = {'ip': spec}
                    elif not isinstance(spec, dict):
                        logger.warning(
                            f'Skipping invalid dnsrecord {domain}')
                        continue
                    ip = spec.get('ip', 'lookup')
                    if ip == 'lookup' or ip is None:
                        ip = await get_ip(domain)
                    cnames = spec.get('cnamerecords') or []
                    if isinstance(cnames, str):
                        cnames = [cnames]
                    enabled = 0 if _startstate_off(
                        spec.get('startstate')) else 1
                    key = f'dns_{rec_id}'
                    self[key] = self.dns_record(
                        ['dns', rec_id, 'A', enabled,
                         domain, ip, list(cnames), None],
                        self.connection.client.publish
                    )
                    await self[key].hass_add()
                    rec_id += 1

            c_section = self.dnsrecs.get('cnamerecords') or {}
            if isinstance(c_section, dict):
                for source, spec in c_section.items():
                    if isinstance(spec, str):
                        spec = {'target': spec}
                    elif not isinstance(spec, dict):
                        logger.warning(
                            f'Skipping invalid cnamerecord {source}')
                        continue
                    target = spec.get('target') or spec.get('cname')
                    if not target:
                        logger.warning(
                            f'cnamerecord {source} missing target, '
                            'skipping')
                        continue
                    enabled = 0 if _startstate_off(
                        spec.get('startstate')) else 1
                    key = f'dns_{rec_id}'
                    self[key] = self.dns_record(
                        ['dns', rec_id, 'CNAME', enabled,
                         source, None, [], target],
                        self.connection.client.publish
                    )
                    await self[key].hass_add()
                    rec_id += 1

            # Reconcile pihole.toml to current desired state once on
            # startup so startstate is honored, then on every change.
            first_pass = True
            while not self.exiting:
                dns_recs = [
                    gr for gr in self.values()
                    if gr.switch_type == 'dns'
                ]
                pending = [
                    gr for gr in dns_recs if gr.for_pihole_update()
                ]
                if dns_recs and (pending or first_pass) and self.pihole_on:
                    if await self._reconcile_pihole_dns(dns_recs, pending):
                        first_pass = False
                    for gr in pending:
                        gr.hass_upd()
                await asyncio.sleep(
                    float(config['pihole_update_frequency']))
            else:
                logger.debug('Finished check_dns_records loop')
                return 0
            logger.error('Error in check_dns_records loop')
            return 0b100000000

        # Run tasks in continuous loop
        return sum(await asyncio.gather(
            check_gravity(),
            update_pihole(),
            check_pihole(),
            check_mqtt(),
            check_dns_records()))

    def exit_intended(self, *args):
        """Callback function for trapping exit signals and/or
        exiting gravity_records."""
        try:
            if int(args[0]) > 0:
                logger.info('Signal '
                            f'{signal.strsignal(args[0]).upper()} '
                            'received... exiting...')
        except IndexError:
            logger.info('An issue has been detected.  Closing mqtt objects.')
        self.availability(False)
        self.exiting = True
        self.connection.cancel_message_future()

    @log_decorator
    def FTL_pid(self) -> int:
        """Get the pid of Pi-hole"""
        rc = 0
        try:
            with open(config['ftl_pid_file'], 'r') as file:
                rc = int(file.read().rstrip())
        except (
            FileNotFoundError,
            ValueError
        ):
            rc = 0
        return rc

    def availability(self, av_on: bool = True):
        logger.debug(f'Setting availability: {av_on}')
        self.connection.client.publish(
            topic=f'{config["mqtt_topic"]}/state',
            payload='online' if av_on else 'offline',
            retain=True)

    @staticmethod
    def _host_entry_name(entry: str) -> str:
        """Extract the hostname from a 'IP hostname' dns.hosts entry."""
        parts = str(entry).strip().split()
        return parts[1] if len(parts) >= 2 else ''

    @staticmethod
    def _cname_entry_source(entry: str) -> str:
        """Extract the source from a 'source,target[,ttl]' entry."""
        return str(entry).split(',', 1)[0].strip()

    @staticmethod
    def _toml_str_array(values: list) -> str:
        """Format a list of strings as a TOML inline array."""
        def esc(v):
            return str(v).replace('\\', '\\\\').replace('"', '\\"')
        return '[ ' + ', '.join(f'"{esc(v)}"' for v in values) + ' ]'

    def _read_pihole_dns_arrays(self) -> tuple:
        """Read the current dns.hosts and dns.cnameRecords arrays from
        pihole.toml. Returns ([], []) on failure.
        """
        try:
            with open(config['pihole_toml'], 'rb') as f:
                data = tomllib.load(f)
        except (FileNotFoundError, tomllib.TOMLDecodeError) as e:
            logger.error(
                f'Failed to read {config["pihole_toml"]}: {e}')
            return ([], [])
        dns_sec = data.get('dns', {}) or {}
        return (
            list(dns_sec.get('hosts', []) or []),
            list(dns_sec.get('cnameRecords', []) or []),
        )

    async def _pihole_set_config(self, key: str, values: list) -> bool:
        """Write a TOML array config value via `pihole-FTL --config`."""
        payload = self._toml_str_array(values)
        try:
            proc = await asyncio.create_subprocess_exec(
                'pihole-FTL', '--config', key, payload,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            _, stderr = await proc.communicate()
        except FileNotFoundError:
            logger.error('pihole-FTL not found; cannot apply DNS changes')
            return False
        if proc.returncode != 0:
            logger.error(
                f'pihole-FTL --config {key} failed '
                f'(rc={proc.returncode}): {stderr.decode().strip()}')
            return False
        logger.debug(f'pihole-FTL --config {key} set: {payload}')
        return True

    async def _pihole_reload_dns(self) -> bool:
        """Ask Pi-hole to reload its DNS configuration."""
        try:
            proc = await asyncio.create_subprocess_exec(
                'pihole', 'reloaddns',
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            await proc.communicate()
        except FileNotFoundError:
            logger.error('pihole command not found; DNS changes may '
                         'not take effect until next FTL restart')
            return False
        if proc.returncode != 0:
            logger.warning(
                f'pihole reloaddns exited with code {proc.returncode}')
            return False
        return True

    async def _reconcile_pihole_dns(
            self, dns_recs: list, pending: list) -> bool:
        """Rebuild pihole.toml's dns.hosts and dns.cnameRecords arrays
        so they reflect our records' current intended state, leaving
        any unmanaged entries (added by the user via the Pi-hole UI)
        untouched. Returns True if writes (when needed) succeeded.
        """
        cur_hosts, cur_cnames = self._read_pihole_dns_arrays()
        managed_a = {
            gr.domain_name for gr in dns_recs if gr.dns_type == 'A'
        }
        managed_c = (
            {gr.domain_name for gr in dns_recs if gr.dns_type == 'CNAME'}
            | {c for gr in dns_recs if gr.dns_type == 'A'
               for c in (gr.c_names or [])}
        )
        for gr in pending:
            logger.debug(
                f'Applying state {gr.mqtt_state} to dns record '
                f'{gr.id} ({gr.domain_name})')
            gr.enabled = gr.mqtt_state
        new_hosts = [
            h for h in cur_hosts
            if self._host_entry_name(h) not in managed_a
        ]
        new_cnames = [
            c for c in cur_cnames
            if self._cname_entry_source(c) not in managed_c
        ]
        for gr in dns_recs:
            if gr.enabled:
                h_lines, c_lines = gr.toml_entries()
                new_hosts.extend(h_lines)
                new_cnames.extend(c_lines)
        ok = True
        wrote = False
        if new_hosts != cur_hosts:
            if await self._pihole_set_config('dns.hosts', new_hosts):
                wrote = True
            else:
                ok = False
        if new_cnames != cur_cnames:
            if await self._pihole_set_config(
                    'dns.cnameRecords', new_cnames):
                wrote = True
            else:
                ok = False
        if wrote:
            await self._pihole_reload_dns()
        return ok


@log_decorator
def run() -> int:
    '''The run() function is the starter for running mqtt4pihole'''

    # Load config...
    if not config.load_config():
        logger.critical('One or more of mqtt_user, mqtt_pw, '
                        'or mqtt_host is missing - cannot continue')
        return 0b10000000

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
    rc = 0b0000011

    # We need to distinguish intended from unintended sqlite/MQTT disconnects.
    # If a disconnect is not intended a connection will be re-established.
    reconnect = True
    while reconnect:
        # Create sqlite connection.
        # Connect to database - this connection is used for the regular
        # checking for changes (made in the Pi-hole webgui)
        with sqlite3.connect(config['db_file']) as db_con:
            # Set rc for the event that there is a problem setting up the
            # asyncio event loop.
            rc = 0b0000010

            # Set event loop policy
            asyncio.set_event_loop_policy(EL_policy())

            # Create and enter asyncio loop
            with asyncio.Runner() as runner:
                loop = runner.get_loop()

                # Set rc for the event that there is a problem setting up
                # the MQTT connection.
                rc = 0b0000001

                # Create and start mqtt connection
                try:
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
                            subscribe_topics=[
                                f'{config["mqtt_topic"]}/+/set',
                                f'{config["mqtt_hass_topic"]}/status'
                            ]
                    ) as m_con:
                        if m_con:
                            grav_recs = gravity_records(
                                mqtt_connection=m_con,
                                db_connection=db_con
                            )
                            rc = runner.run(grav_recs.main())
                except mqtt.mqttConnectionError:
                    logger.error('Connection with mqtt broker failed.')
                    time.sleep(float(config['mqtt_check_frequency']))
                except sqlite3.OperationalError:
                    logger.error('Connection with pi-hole db failed.')
                    time.sleep(float(config['pihole_check_frequency']))

        if rc & 0b0110101:  # This covers rc values that warrant reconnecting.
            logger.info('Unintended disconnect, reconnecting...')
        else:
            logger.info('Exiting...')
            reconnect = False

    return rc


if __name__ == '__main__':
    # Inialise config dictionary, to store all config paramaters
    # these are loaded in the 'run' function.
    config = m4p_config()
    sys.exit(run())
