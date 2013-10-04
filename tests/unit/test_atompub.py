import functools
import unittest

import httplib2
import mox
import stubout

import yagi.config

from yagi.handler.atompub_handler import AtomPub


class MockResponse(object):
    def __init__(self, status_code=200):
        self.status = status_code


class MockMessage(object):
    def __init__(self, payload):
        self.payload = payload
        self.acknowledged = False

    def ack(self):
        self.acknowledged = True


class AtomPubTests(unittest.TestCase):
    """Tests to ensure the ATOM Pub code holds together as expected"""

    def setUp(self):
        self.mox = mox.Mox()
        self.stubs = stubout.StubOutForTesting()

        self.handler = AtomPub()
        self.mox.StubOutWithMock(self.handler, 'config_get')
        self.handler.config_get('retries').AndReturn(1)
        self.handler.config_get('interval').AndReturn(30)
        self.handler.config_get('max_wait').AndReturn(600)
        self.handler.config_get('generate_entity_links').AndReturn(False)
        self.handler.config_get('failures_before_reauth').AndReturn(5)
        self.handler.config_get('validate_ssl').AndReturn(False)
        self.handler.config_get('url').AndReturn(
            'http://127.0.0.1:9000/test/%(event_type)s')
        self.handler.config_get('stacktach_down').AndReturn(True)
        self.mox.ReplayAll()

    def tearDown(self):
        self.mox.UnsetStubs()
        self.stubs.UnsetAll()

    def test_notify(self):
        messages = [MockMessage({'event_type': 'instance_create',
                    'message_id': 1,
                    'content': dict(a=3)})]

        self.called = False

        def mock_request(*args, **kwargs):
            self.called = True
            return MockResponse(201), None

        self.stubs.Set(httplib2.Http, 'request', mock_request)
        self.handler.handle_messages(messages, dict())
        self.assertEqual(self.called, True)

    def test_notify_fails(self):
        messages = [MockMessage({'event_type': 'instance_create',
                    'message_id': 1,
                    'content': dict(a=3)})]
        self.called = False

        def mock_request(*args, **kwargs):
            self.called = True
            return MockResponse(404), None

        self.stubs.Set(httplib2.Http, 'request', mock_request)
        self.handler.handle_messages(messages, dict())
        self.assertEqual(self.called, True)

    def test_change_exists_event_to_verified_when_stacktach_down(self):
        payload = {'event_type': 'compute.instance.exists', 'message_id': 1,
                   'content': dict(a=3)}
        messages = [MockMessage(payload)]
        self.called = False

        def mock_request(*args, **kwargs):
            self.called = True
            return MockResponse(404), None

        self.mox.StubOutWithMock(self.handler, 'config_get')
        self.handler.config_get('stacktach_down').AndReturn(True)
        self.mox.StubOutWithMock(yagi.serializer.atom, 'dump_item')
        expected_entity = {
                    'event_type': 'compute.instance.exists.verified',
                    'message_id': 1,
                    'content': payload}

        yagi.serializer.atom.dump_item(expected_entity, entity_links=False)
        self.mox.ReplayAll()
        self.stubs.Set(httplib2.Http, 'request', mock_request)
        self.handler.handle_messages(messages, dict())
        self.mox.VerifyAll()
        self.assertEqual(self.called, True)
