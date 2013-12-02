import time

import yagi.auth
import yagi.config
import yagi.handler
from yagi.handler.http_connection import HttpConnection
from yagi.handler.http_connection import MessageDeliveryFailed
from yagi.handler.http_connection import UnauthorizedException
import yagi.log
import yagi.serializer.atom
from yagi import stats

with yagi.config.defaults_for("atompub") as default:
    default("validate_ssl", "False")
    default("generate_entity_links", "False")
    default("retries", "-1")
    default("url", "http://127.0.0.1/nova")
    default("max_wait", "600")
    default("failures_before_reauth", "5")
    default("interval", "30")

LOG = yagi.log.logger


class AtomPub(yagi.handler.BaseHandler):
    CONFIG_SECTION = "atompub"
    AUTO_ACK = True

    def note_result(self, env, payload, code=0, error=False, message=None):
        name = self.__class__.__name__.lower() + ".results"
        results = env.get(name) if name in env else dict()
        msgid = payload["message_id"]
        result = dict(error=error)
        if message is None:
            if error:
                message = "Error, unable to send notification"
            else:
                message = "Success"
        result['message'] = message
        result['code'] = code if not error else 0

        results[msgid] = result
        env[name] = results

    def _get_event_type(self, is_stacktach_down, payload):
        exclude_filter_list = []
        exclude_filters = yagi.config.get('exclude_filters', self.CONFIG_SECTION)
        if exclude_filters:
            exclude_filter_list = [a.strip() for a in exclude_filters.
                                   split(",")]
        event_type = payload['event_type']
        send_exists_copy = False
        if event_type == 'compute.instance.exists' and is_stacktach_down and ('compute.instance.exists.verified' not in exclude_filter_list):
                event_type = 'compute.instance.exists.verified'
                send_exists_copy = True
        return event_type, send_exists_copy

    def get_bool(self, bool_string):
        if bool_string[0] in ['t', 'T']:
            return True
        return False

    def handle_messages(self, messages, env):
        retries = int(self.config_get("retries"))
        interval = int(self.config_get("interval"))
        max_wait = int(self.config_get("max_wait"))
        entity_links = self.config_get("generate_entity_links") == "True"
        failures_before_reauth = int(self.config_get("failures_before_reauth"))
        is_stacktach_down = self.get_bool(self.config_get("stacktach_down"))
        connection = HttpConnection(self)

        for payload in self.iterate_payloads(messages, env):
            try:
                event_type, send_exists_copy = self._get_event_type(is_stacktach_down, payload)
                entity = dict(content=payload,
                              id=payload["message_id"],
                              event_type=event_type)
                payload_body = yagi.serializer.atom.dump_item(entity,
                    entity_links=entity_links)
                if send_exists_copy:
                    entity_copy = dict(content=payload,
                              id=payload["message_id"],
                              event_type="compute.instance.exists")
                    payload_body_copy = yagi.serializer.atom.dump_item(entity_copy,
                    entity_links=entity_links)
            except KeyError, e:
                error_msg = "Malformed Notification: %s" % payload
                LOG.error(error_msg)
                LOG.exception(e)
                self.note_result(env, payload, error=True, message=error_msg)
                continue

            endpoint = self.config_get("url")
            tries = 0
            failures = 0
            code = 0
            error_msg = ''

            while True:
                try:
                    code = connection.send_notification(endpoint, endpoint,
                                                        payload_body)
                    if send_exists_copy:
                        code = connection.send_notification(endpoint, endpoint,
                                                        payload_body_copy)
                    error = False
                    msg = ''
                    break
                except UnauthorizedException, e:
                    LOG.exception(e)
                    conn = None
                    code = 401
                    error_msg = "Unauthorized"
                except MessageDeliveryFailed, e:
                    LOG.exception(e)
                    code = e.code
                    error_msg = e.msg
                except Exception, e:
                    code = 0 #aka 'unknown failure'
                    error_msg = "AtomPub General Delivery Failure to %s with: %s" % (endpoint, e)
                    LOG.error(error_msg)
                    LOG.exception(e)

                #If we got here, something failed.
                stats.increment_stat(yagi.stats.failure_message())
                # Number of overall tries
                tries += 1
                # Number of tries between re-auth attempts
                failures += 1

                # Used primarily for testing, but it's possible we don't
                # care if we lose messages?
                if retries > 0:
                    if tries >= retries:
                        msg = "Exceeded retry limit. Error %s" % error_msg
                        self.note_result(env, payload, code=code, message=msg)
                        break
                wait = min(tries * interval, max_wait)
                LOG.error("Message delivery failed, going to sleep, will "
                         "try again in %s seconds" % str(wait))
                time.sleep(wait)

                if failures >= failures_before_reauth:
                    # Don't always try to reconnect, give it a few
                    # tries first
                    failures = 0
                    connection = None
                if connection is None:
                    connection = HttpConnection(self,force=True)

            self.note_result(env, payload, code=code)


