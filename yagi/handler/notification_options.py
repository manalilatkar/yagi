class NotificationOptions(object):

    def __init__(self, options):
        self.options_bit_field = options['com.rackspace__1__options']

    def to_cuf_options(self):
        options_bit_to_dict_map = {'0': {},
                                   '1': {'system': ['isRedHat']},
                                   '2': {'system': ['isSELinux']},
                                   '4': {'system': ['isWindows']},
                                   '12': {'system': ['isWindows',
                                          'isMSSQL']},
                                   '36': {'system': ['isWindows',
                                          'isMSSQLWeb']},
                                   '64': {'appliance': ['VYATTA']}}
        options = options_bit_to_dict_map[self.options_bit_field]
        final_string = ""
        system_properties = options.get('system', [])
        if system_properties:
            for name in system_properties:
                final_string += (' %s="true"' % name)
            return final_string
        network_properties = options.get('appliance', [])
        if network_properties:
            for name in network_properties:
                final_string += (' appliance="%s"' % name)
        return final_string