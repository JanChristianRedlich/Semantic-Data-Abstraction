class SDATransformOperation:
    def __init__(self, action, field, connect=None, rename=None):
        self.action = action
        self.field = field
        self.connect = connect
        self.rename = rename

    def get_flatten_connect(self):
        fcon = []
        for connect in self.connect:
            fcon.append(connect.split(".")[-1])
        return fcon

    def __str__(self):
        return str(self.action) + " | " + str(self.field) + " | " + str(self.connect) + " | " + str(self.rename)