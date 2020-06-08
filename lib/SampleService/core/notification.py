"""
Contains code for Sample Service notifications to other services, primarily Kafka.
"""

import json as _json
import re as _re

from uuid import UUID
from typing import cast as _cast

from kafka import KafkaProducer as _KafkaProducer

from SampleService.core.arg_checkers import (
    not_falsy as _not_falsy,
    check_string as _check_string
)


class KafkaNotifier:
    """
    A notifier that sends JSON messages to Kafka.
    """
    # Unfortunately kafka-python does not yet support exactly once guarantees:
    # https://github.com/dpkp/kafka-python/issues/1063
    # TODO LATER KAFKA enable idempotence when kafka-python supports it.

    # For now, throw an exception when sending a message to Kafka fails. In the future, when
    # KBase error/log monitoring is more robust, could switch to merely logging. But for now,
    # throwing an exception is the way to ensure the issue gets attention.
    # Workspace and Groups take the same approach.

    # This implementation does things that slow down the send operation but improve
    # reliability and user messaging:
    # 1) Require full write to replicates before Kafka returns
    # 2) Wait for the return and check it worked. If not, throw an exception *in the calling
    # thread*. Thus the user is notified if something goes wrong.
    #
    # If this turns out to be a bad plan, we may need to relax those requirements.

    # may need to extract an interface if we want to make other notifiers. This seems unlikely
    # so YAGNI.

    # Currently messages can be lost if the service goes down between DB modification and
    # message send. To make this more robust, we could add a flag to DB records to note that
    # messages have / not been sent, and on startup check for unsent messages.

    # TODO KAFKA CLI to resend messages by sample/link ID and by created/expired stamps.

    # The confluent client is the other option here, but it is strictly asynchronous, and
    # so when throwing exceptions, there is no way to guarantee the exception is thrown in
    # the right thread / coroutine.

    # Since the Kafka producer tries to contact Kafka on startup, and we want to create our
    # own to make sure it's configured correctly, unit testing this seems like a pain (or
    # we do some really gross monkey patching). As such, it's only tested in the intergration
    # tests.

    _EVENT_TYPE = 'event_type'
    _SAMPLE_ID = 'sample_id'
    _SAMPLE_VERSION = 'sample_ver'
    _NEW_SAMPLE = 'NEW_SAMPLE'
    _ACL_CHANGE = 'ACL_CHANGE'
    _LINK_ID = 'link_id'
    _NEW_LINK = 'NEW_LINK'
    _EXPIRED_LINK = 'EXPIRED_LINK'

    _KAFKA_TOPIC_ILLEGAL_CHARS_RE = _re.compile('[^a-zA-Z0-9-]+')

    def __init__(self, bootstrap_servers: str, topic: str):
        """
        Create the notifier.

        :param bootstrap_servers: the Kafka bootstrap servers parameter.
        :param topic: the topic where messages will be sent. The notifier requires the topic
            name to consist of ASCII alphanumeric values and the hyphen to avoid Kafka issues
            around ambiguity between period and underscore values.
        """
        _check_string(bootstrap_servers, 'bootstrap_servers')
        self._topic = _check_string(topic, 'topic', max_len=249)
        match = self._KAFKA_TOPIC_ILLEGAL_CHARS_RE.search(_cast(str, self._topic))
        if match:
            raise ValueError(f'Illegal character in Kafka topic {self._topic}: {match.group()}')

        # TODO LATER KAFKA support delivery.timeout.ms when the client supports it
        # https://github.com/dpkp/kafka-python/issues/1723
        # since not supported, we half ass it with a retry count.
        # See https://kafka.apache.org/documentation/#producerconfigs
        # this will fail if it can't connect
        self._prod = _KafkaProducer(
            # can't test multiple servers without a massive PITA
            bootstrap_servers=bootstrap_servers.split(','),
            acks='all',
            # retries can occur from 100-30000 ms by default. If we allow 300 retries, that means
            # the send can take from 30s to 150m. Presumably another server timeout will kill
            # the request before then.
            retries=300,
            retry_backoff_ms=100,  # default is 100
            # low timeouts can cause message loss, apparently:
            # https://github.com/dpkp/kafka-python/issues/1723
            request_timeout_ms=30000,  # default is 30000
            # presumably this can be removed once idempotence is supported
            max_in_flight_requests_per_connection=1,
            )
        self._closed = False

    def notify_new_sample_version(self, sample_id: UUID, sample_ver: int):
        """
        Send a notification that a new sample version has been created.

        :param sample_id: the sample ID.
        :param sample_ver: the version of the sample.
        """
        if sample_ver < 1:
            raise ValueError('sample_ver must be > 0')
        self._send_message({
            self._EVENT_TYPE: self._NEW_SAMPLE,
            self._SAMPLE_ID: str(_not_falsy(sample_id, 'sample_id')),
            self._SAMPLE_VERSION: sample_ver
            })

    def notify_sample_acl_change(self, sample_id: UUID):
        """
        Send a notification for a sample ACL change.

        :param sample_id: the sample ID.
        """
        self._send_message({
            self._EVENT_TYPE: self._ACL_CHANGE,
            self._SAMPLE_ID: str(_not_falsy(sample_id, 'sample_id'))
            })

    def notify_new_link(self, link_id: UUID):
        """
        Send a notification that a link has been created.

        :param link_id: the link ID.
        """
        self._send_message({
            self._EVENT_TYPE: self._NEW_LINK,
            self._LINK_ID: str(_not_falsy(link_id, 'link_id'))
            })

    def notify_expired_link(self, link_id: UUID):
        """
        Send a notification that a link has been expired.

        :param link_id: the link ID.
        """
        self._send_message({
            self._EVENT_TYPE: self._EXPIRED_LINK,
            self._LINK_ID: str(_not_falsy(link_id, 'link_id'))
            })

    def _send_message(self, message):
        if self._closed:
            raise ValueError('client is closed')
        future = self._prod.send(self._topic, _json.dumps(message).encode('utf-8'))
        # ensure the message was send correctly, or if not throw an exeption in the correct thread
        future.get(timeout=35)  # this is very difficult to test

    def close(self):
        """
        Close the notifier.
        """
        # handle with context at some point
        self._prod.close()
        self._closed = True
