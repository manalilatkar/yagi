import datetime
import uuid
import yagi
import yagi.log

LOG = yagi.log.logger


def start_time(launched_at, audit_period_beginning):
        start_time = max(launched_at, audit_period_beginning)
        return format_time(start_time)


def end_time(deleted_at, audit_period_ending):
        if not deleted_at:
            return format_time(audit_period_ending)
        end_time = min(deleted_at, audit_period_ending)
        return format_time(end_time)


def format_time(when):
    if 'Z' in when:
        when = _try_parse(when, ["%Y-%m-%dT%H:%M:%SZ",
                                 "%Y-%m-%dT%H:%M:%S.%fZ"])
    elif 'T' in when:
        when = _try_parse(when, ["%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S"])
    else:
        when = _try_parse(when, ["%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S",
                                 "%d %m %Y %H:%M:%S"])

    return when


def _try_parse(when, formats):
    last_exception = None
    for date_format in formats:
        try:
            when = datetime.datetime.strptime(when, date_format)
            parsed = True
        except Exception, e:
            parsed = False
            last_exception = e
        if parsed:
            return when
    print "Bad DATE ", last_exception


class NotificationPayload(object):
    def __init__(self, payload_json):
        self.deleted_at = ''
        self.image_meta = payload_json.get('image_meta', {})
        self.options = self.image_meta.get('com.rackspace__1__options', '0')
        bandwidth = payload_json.get('bandwidth', {})
        public_bandwidth = bandwidth.get('public', {})
        self.bandwidth_in = public_bandwidth.get('bw_in', 0)
        self.bandwidth_out = public_bandwidth.get('bw_out', 0)

        self.launched_at = str(format_time(payload_json['launched_at']))

        self.audit_period_beginning = str(format_time(
            payload_json['audit_period_beginning']))

        self.audit_period_ending = str(format_time(
            payload_json['audit_period_ending']))

        if payload_json['deleted_at']:
            self.deleted_at = str(format_time(
                payload_json['deleted_at']))

        self.tenant_id = payload_json.get('tenant_id', "")
        self.instance_id = payload_json.get('instance_id', "")
        field_name = yagi.config.get('nova', 'nova_flavor_field_name')
        self.flavor_id = payload_json[field_name]
        self.flavor_name = payload_json['instance_type']
        task_state = payload_json.get('state_description', "")
        vm_state = payload_json.get('state', "")
        self.status = self._get_status(task_state, vm_state)
        self.start_time = start_time(self.launched_at,
                                     self.audit_period_beginning)
        self.end_time = end_time(self.deleted_at, self.audit_period_ending)

    def _get_status(self, task_state, vm_state):
        _STATE_MAP = {
            "active": {
                'default': 'ACTIVE',
                "rebooting": 'REBOOT',
                "rebooting_hard": 'HARD_REBOOT',
                "updating_password": 'PASSWORD',
                "rebuilding": 'REBUILD',
                "rebuild_block_device_mapping": 'REBUILD',
                "rebuild_spawning": 'REBUILD',
                "migrating": 'MIGRATING',
                "resize_prep": 'RESIZE',
                "resize_migrating": 'RESIZE',
                "resize_migrated": 'RESIZE',
                "resize_finish": 'RESIZE',
            },
            "building": {
                'default': 'BUILD',
            },
            "stopped": {
                'default': 'SHUTOFF',
            },
            "resized": {
                'default': 'VERIFY_RESIZE',
                "resize_reverting": 'REVERT_RESIZE',
            },
            "paused": {
                'default': 'PAUSED',
            },
            "suspended": {
                'default': 'SUSPENDED',
            },
            "rescued": {
                'default': 'RESCUE',
            },
            "error": {
                'default': 'ERROR',
            },
            "deleted": {
                'default': 'DELETED',
            },
            "soft-delete": {
                'default': 'SOFT_DELETED',
            },
            "shelved": {
                'default': 'SHELVED',
            },
            "shelved_offloaded": {
                'default': 'SHELVED_OFFLOADED',
            },
        }
        task_map = _STATE_MAP.get(vm_state, dict(default='UNKNOWN'))
        status = task_map.get(task_state, task_map['default'])
        if status == "UNKNOWN":
            LOG.error(("status is UNKNOWN from vm_state=%(vm_state)s "
                    "task_state=%(task_state)s. Bad upgrade or db "
                    "corrupted?"),
                  {'vm_state': vm_state, 'task_state': task_state})
        return status




class GlanceNotificationPayload(object):
    def __init__(self, payload_json):
        deleted_at = None
        self.images = []
        raw_images = payload_json.get('images', {})
        audit_period_beginning = payload_json.get('audit_period_beginning', "")
        audit_period_ending = payload_json.get('audit_period_ending', "")
        for raw_image in raw_images:
            image = {}
            image['id'] = uuid.uuid4()
            image['resource_id'] = raw_image.get('id', "")
            image['tenant_id'] = payload_json.get('owner', "")
            created_at = raw_image['created_at']
            if raw_image['deleted_at']:
                deleted_at = raw_image['deleted_at']
            image['start_time'] = start_time(created_at,
                                             audit_period_beginning)
            image['end_time'] = end_time(deleted_at,
                                         audit_period_ending)
            properties = raw_image.get('properties', {})
            image['resource_type'] = properties.get('image_type', "")
            image['server_id'] = properties.get('instance_uuid', "")
            image['server_name'] = properties.get('instance_name', "")
            image['storage'] = raw_image.get('size', "")
            self.images.append(image)
