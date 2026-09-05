# -*- coding: utf-8 -*-
################################################################################
# Copyright (c) 2018 McAfee LLC - All Rights Reserved.
################################################################################

"""
Regression tests for bugs fixed in the client (msgpack >= 1.0 wire
compatibility, large payloads, request manager bookkeeping, file truncation,
configuration defaults, service re-registration retries and connection
failure handling).
"""

# Run with python -m unittest dxlclient.test.test_regressions

from __future__ import absolute_import
import os
import shutil
import tempfile
import threading
import unittest

from nose.plugins.attrib import attr
from mock import MagicMock

from dxlclient import Broker, DxlClient, DxlClientConfig, DxlException, \
    DxlUtils, ErrorResponse, Event, Message, Request, RequestManager, \
    Response, UuidGenerator, WaitTimeoutException
from dxlclient.callbacks import RequestCallback, ResponseCallback
from dxlclient.service import _ServiceRegistrationHandler, \
    ServiceRegistrationInfo
from dxlclient.test.base_test import BaseClientTest

# pylint: disable=missing-docstring, protected-access


class MessageWireFormatTest(unittest.TestCase):
    """
    The DXL wire format is the legacy msgpack format (no "bin" family, no
    "str"/"bin" distinction). msgpack >= 1.0 changed the packer and unpacker
    defaults; these tests fail if the legacy format is not pinned explicitly.
    """

    def test_event_round_trip_with_str_fields(self):
        event = Event(destination_topic="/test/topic")
        event.payload = b"hello-payload"
        event.broker_ids = ["{broker-1}", "{broker-2}"]
        event.other_fields = {"key": "value"}

        result = Message._from_bytes(event._to_bytes())

        self.assertIsInstance(result.message_id, str)
        self.assertEqual(event.message_id, result.message_id)
        self.assertEqual(b"hello-payload", result.payload)
        self.assertEqual(["{broker-1}", "{broker-2}"], result.broker_ids)
        self.assertTrue(all(isinstance(b, str) for b in result.broker_ids))
        self.assertEqual({"key": "value"}, result.other_fields)
        self.assertIsInstance(list(result.other_fields.keys())[0], str)

    def test_request_round_trip_preserves_service_id_as_str(self):
        request = Request(destination_topic="/test/service")
        request.service_id = "{service-id}"
        request.reply_to_topic = "/reply/topic"
        request.payload = b"ping"

        result = Message._from_bytes(request._to_bytes())

        self.assertIsInstance(result, Request)
        self.assertEqual("{service-id}", result.service_id)
        self.assertEqual("/reply/topic", result.reply_to_topic)
        self.assertEqual(b"ping", result.payload)

    def test_error_response_round_trip(self):
        request = Request(destination_topic="/test/service")
        request.reply_to_topic = "/reply/topic"
        error = ErrorResponse(request, error_code=42, error_message="boom")

        result = Message._from_bytes(error._to_bytes())

        self.assertIsInstance(result, ErrorResponse)
        self.assertEqual(42, result.error_code)
        self.assertEqual("boom", result.error_message)
        self.assertEqual(request.message_id, result.request_message_id)

    def test_payload_uses_legacy_raw_family_on_the_wire(self):
        # A 3 byte payload is encoded as "fixraw" (0xa3) in the legacy format.
        # With msgpack >= 1.0 defaults (use_bin_type=True) it would be encoded
        # as "bin 8" (0xc4 0x03), which brokers / other clients do not expect.
        event = Event(destination_topic="/test/topic")
        event.payload = b"abc"

        raw = event._to_bytes()

        self.assertIn(b"\xa3abc", raw)
        self.assertNotIn(b"\xc4\x03abc", raw)

    def test_payload_larger_than_one_mebibyte(self):
        # Older msgpack versions limit str/bin values to 1 MiB by default
        # (max_str_len / max_bin_len), which broke receiving responses
        # larger than 1 MiB from brokers with a raised messageSizeLimit.
        payload = b"x" * (2 * 1024 * 1024 + 1)
        event = Event(destination_topic="/test/topic")
        event.payload = payload

        result = Message._from_bytes(event._to_bytes())

        self.assertEqual(len(payload), len(result.payload))
        self.assertEqual(payload, result.payload)

    def test_unknown_message_type_raises_dxl_exception(self):
        import msgpack
        packer = msgpack.Packer(use_bin_type=False)
        raw = packer.pack(Message.MESSAGE_VERSION) + packer.pack(99)

        with self.assertRaises(DxlException) as context:
            Message._from_bytes(raw)
        self.assertIn("99", str(context.exception))


class MockDxlClient(object):
    def __init__(self, fail_send=False):
        self.unique_id = UuidGenerator.generate_id_as_string()
        self.fail_send = fail_send
        self.sent_requests = []

    def add_response_callback(self, channel, response_callback):
        pass

    def _send_request(self, request):
        if self.fail_send:
            raise DxlException("send failed")
        self.sent_requests.append(request)


class MockResponseCallback(ResponseCallback):
    def __init__(self):
        super(MockResponseCallback, self).__init__()
        self.responses = []

    def on_response(self, response):
        self.responses.append(response)


class MockRequestCallback(RequestCallback):
    def on_request(self, request):
        pass


class RequestManagerRegressionTest(unittest.TestCase):

    def test_async_callback_is_removed_when_send_fails(self):
        client = MockDxlClient(fail_send=True)
        request_manager = RequestManager(client)
        request = Request(destination_topic="/test/service")

        with self.assertRaises(DxlException):
            request_manager.async_request(request, MockResponseCallback())

        # The callback used to be looked up by the destination topic instead
        # of the message identifier and therefore leaked.
        self.assertEqual(0, request_manager._get_async_callback_count())
        self.assertEqual(0, request_manager.get_current_request_queue_size())

    def test_sync_request_timeout_is_not_extended_by_other_responses(self):
        client = MockDxlClient()
        request_manager = RequestManager(client)
        request = Request(destination_topic="/test/service")
        other_request = Request(destination_topic="/test/service")

        def deliver_unrelated_responses():
            # Wake up the waiting thread repeatedly with responses for a
            # different request. Previously each wake-up restarted the
            # full timeout.
            for _ in range(5):
                threading.Event().wait(0.05)
                other_request.reply_to_topic = "/reply"
                request_manager.on_response(Response(other_request))

        thread = threading.Thread(target=deliver_unrelated_responses)
        thread.daemon = True
        start = threading.Event()
        thread.start()
        start.set()

        import time
        started = time.time()
        with self.assertRaises(WaitTimeoutException):
            request_manager.sync_request(request, 0.5)
        elapsed = time.time() - started
        thread.join()

        self.assertLess(elapsed, 2.0)


class SaveToFileTest(unittest.TestCase):

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_existing_file_is_truncated(self):
        filename = os.path.join(self.temp_dir, "sub", "client.key")
        DxlUtils.save_to_file(filename, "A" * 100)
        DxlUtils.save_to_file(filename, "B" * 10)

        with open(filename, "r") as handle:
            content = handle.read()

        # Previously the file was opened without O_TRUNC, leaving 90 "A"
        # characters after the new content.
        self.assertEqual("B" * 10, content)

    def test_bytes_are_written(self):
        filename = os.path.join(self.temp_dir, "data.bin")
        DxlUtils.save_to_file(filename, b"\x00\x01\x02")
        DxlUtils.save_to_file(filename, b"\x03")

        with open(filename, "rb") as handle:
            self.assertEqual(b"\x03", handle.read())


class ClientConfigRegressionTest(unittest.TestCase):

    def test_websockets_default_when_only_websocket_brokers_given(self):
        config = DxlClientConfig(broker_ca_bundle="ca.crt",
                                 cert_file="client.crt",
                                 private_key="client.key",
                                 brokers=[],
                                 websocket_brokers=[Broker.parse("wss://b1")])

        self.assertTrue(config.use_websockets)
        self.assertEqual(1, len(config.brokers))

    def test_tcp_default_when_tcp_brokers_given(self):
        config = DxlClientConfig(broker_ca_bundle="ca.crt",
                                 cert_file="client.crt",
                                 private_key="client.key",
                                 brokers=[Broker.parse("ssl://b1")],
                                 websocket_brokers=[Broker.parse("wss://b1")])

        self.assertFalse(config.use_websockets)

    def test_tls_ciphers_default_and_override(self):
        config = DxlClientConfig(broker_ca_bundle="ca.crt",
                                 cert_file="client.crt",
                                 private_key="client.key",
                                 brokers=[Broker.parse("ssl://b1")])

        self.assertEqual(DxlClientConfig._DEFAULT_TLS_CIPHERS,
                         config.tls_ciphers)
        config.tls_ciphers = None
        self.assertIsNone(config.tls_ciphers)

    def test_default_cipher_list_contains_strong_and_legacy_suites(self):
        import ssl
        context = ssl.SSLContext(DxlClient._get_tls_protocol())
        context.set_ciphers(DxlClientConfig._DEFAULT_TLS_CIPHERS)
        names = [cipher["name"] for cipher in context.get_ciphers()]

        # Legacy suite required by Trellix DXL brokers < 6.1.1 and the open
        # source broker ...
        self.assertIn("AES128-SHA256", names)
        # ... but only as a fallback after forward-secrecy suites
        self.assertEqual("AES128-SHA256", names[-1])
        self.assertTrue(any(name.startswith("ECDHE-") for name in names))
        self.assertFalse(any("NULL" in name for name in names))

    def test_tls_ciphers_from_config_file(self):
        temp_dir = tempfile.mkdtemp()
        try:
            config_file = os.path.join(temp_dir, "dxlclient.config")
            for value, expected in (
                    (None, DxlClientConfig._DEFAULT_TLS_CIPHERS),
                    ("default", None),
                    ("", None),
                    ("ECDHE+AESGCM:!aNULL", "ECDHE+AESGCM:!aNULL")):
                general = "" if value is None else \
                    "TlsCiphers = {}\n".format(value)
                with open(config_file, "w") as handle:
                    handle.write(
                        "[General]\n" + general +
                        "[Certs]\nBrokerCertChain = ca.crt\n"
                        "CertFile = client.crt\nPrivateKey = client.key\n"
                        "[Brokers]\nb1 = b1;8883;broker1\n")
                config = DxlClientConfig.create_dxl_config_from_file(
                    config_file)
                self.assertEqual(expected, config.tls_ciphers,
                                 "TlsCiphers={!r}".format(value))

            # Round trip through write(): the value is persisted
            config.tls_ciphers = "AES128-SHA256"
            config.write(config_file)
            config = DxlClientConfig.create_dxl_config_from_file(config_file)
            self.assertEqual("AES128-SHA256", config.tls_ciphers)
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)

    def test_ipv6_broker_parsing(self):
        broker = Broker.parse("ssl://[::1]:8883")
        self.assertEqual("::1", broker.host_name)
        self.assertEqual(8883, broker.port)

        broker = Broker.parse("ssl://[fe80::1]")
        self.assertEqual("fe80::1", broker.host_name)
        self.assertEqual(8883, broker.port)

        # Config file broker line with an IPv6 address
        broker = Broker(host_name="none")
        broker._parse("{guid};8883;broker.example;2001:db8::10")
        self.assertEqual("{guid}", broker.unique_id)
        self.assertEqual("broker.example", broker.host_name)
        self.assertEqual("2001:db8::10", broker.ip_address)
        self.assertEqual("{guid};8883;broker.example;2001:db8::10",
                         broker._to_broker_string())


class ServiceRegistrationRetryTest(unittest.TestCase):
    """
    A failing (re-)registration request must not stop the TTL timer of a
    service, otherwise the service silently disappears from the fabric.
    """

    def _create_handler(self, sync_request_side_effect):
        client = MagicMock()
        client.connected = True
        client.sync_request = MagicMock(side_effect=sync_request_side_effect)
        info = ServiceRegistrationInfo(client, "/test/service")
        info.add_topic("/test/service/topic", MockRequestCallback())
        handler = _ServiceRegistrationHandler(client, info)
        return client, handler

    def test_timer_is_rescheduled_when_registration_raises(self):
        client, handler = self._create_handler(
            WaitTimeoutException("timeout"))
        try:
            handler._timer_callback()

            self.assertTrue(client.sync_request.called)
            self.assertIsNotNone(handler.ttl_timer)
            self.assertAlmostEqual(
                _ServiceRegistrationHandler._REGISTER_RETRY_DELAY,
                handler.ttl_timer.interval)
            self.assertEqual(0, handler.get_register_time())
        finally:
            handler.stop_timer()
            handler.destroy(unregister=False)

    def test_timer_is_rescheduled_with_ttl_after_success(self):
        request = Request("/mcafee/service/dxl/svcregistry/register")
        client, handler = self._create_handler(
            lambda req, timeout: Response(request))
        try:
            handler._timer_callback()

            self.assertIsNotNone(handler.ttl_timer)
            self.assertAlmostEqual(handler.ttl * 60, handler.ttl_timer.interval)
            self.assertGreater(handler.get_register_time(), 0)
        finally:
            handler.stop_timer()
            handler.destroy(unregister=False)


class ConnectFailureTest(BaseClientTest):
    """
    A failed connect() must not leave the MQTT network loop thread running
    in the background (it kept reconnecting forever, ignoring
    ``connect_retries``, and blocked the loop of a later successful connect).
    """

    @attr('system')
    def test_failed_connect_does_not_start_mqtt_loop(self):
        config = DxlClientConfig.create_dxl_config_from_file(
            os.path.dirname(os.path.abspath(__file__)) + "/client_config.cfg")
        # ``config.brokers`` returns the WebSocket broker list when the
        # WebSocket transport is configured, so replace both lists.
        good_brokers = config.brokers
        good_websocket_brokers = config.websocket_brokers
        bad_brokers = [Broker(host_name=good_brokers[0].host_name, port=1)]
        config.brokers = bad_brokers
        config.websocket_brokers = bad_brokers
        config.connect_retries = 0
        config.reconnect_delay = 0.1

        with self.create_client_from_config(config) as client:
            with self.assertRaises(DxlException):
                client.connect()

            self.assertFalse(client.connected)
            self.assertIsNone(client._client._thread)

            # A subsequent connect against a reachable broker must work
            config.brokers = good_brokers
            config.websocket_brokers = good_websocket_brokers
            client.connect()
            self.assertTrue(client.connected)
            client.disconnect()
            self.assertFalse(client.connected)

    def test_failed_connect_does_not_wait_for_connect_callback(self):
        """
        When the connect thread gives up without starting the MQTT loop
        there is no connect callback pending; ``connect()`` used to wait
        ``_DEFAULT_CONNECT_WAIT`` (10 s) for it anyway before raising.
        """
        config = DxlClientConfig.create_dxl_config_from_file(
            os.path.dirname(os.path.abspath(__file__)) + "/client_config.cfg")
        bad_brokers = [Broker(host_name="127.0.0.1", unique_id="bad",
                              ip_address="127.0.0.1", port=1)]
        config.brokers = bad_brokers
        config.websocket_brokers = bad_brokers
        config.connect_retries = 0
        config.reconnect_delay = 0.1

        with self.create_client_from_config(config) as client:
            waits = []
            original_wait = client._connected_wait_condition.wait

            def recording_wait(timeout=None):
                waits.append(timeout)
                return original_wait(timeout)

            client._connected_wait_condition.wait = recording_wait
            with self.assertRaises(DxlException):
                client.connect()

            self.assertFalse(client.connected)
            self.assertIsNone(client._client._thread)
            self.assertEqual([], waits)

    def create_client_from_config(self, config):
        from dxlclient.test.base_test import TestDxlClient
        return TestDxlClient(config)


if __name__ == '__main__':
    unittest.main()
