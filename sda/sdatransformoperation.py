class SDATransformOperation:
    """
    A class representing a transformation operation in the SDA (Structured Data Analysis) process.

    Attributes:
        action (str): The operation or action to be performed (e.g., 'flatten', 'rename').
        field (str): The field on which the action is applied.
        connect (list, optional): A list of connections or relationships associated with the field. Defaults to None.
        rename (str, optional): The new name for the field if the operation involves renaming. Defaults to None.
    """

    def __init__(self, action, field, connect=None, rename=None):
        """
        Initializes the SDATransformOperation instance.

        Args:
            action (str): The action to be performed.
            field (str): The field on which the action is applied.
            connect (list, optional): A list of connections for the field. Defaults to None.
            rename (str, optional): The new name for the field in case of renaming. Defaults to None.
        """
        self.action = action
        self.field = field
        self.connect = connect
        self.rename = rename

    def get_flatten_connect(self):
        """
        Extracts and returns the last segment from each connection in the `connect` list.

        Returns:
            list: A list of the last segments of the connections.
        """
        fcon = []
        for connect in self.connect:
            fcon.append(connect.split(".")[-1])
        return fcon

    def __str__(self):
        """
        Returns a string representation of the SDATransformOperation instance.

        Returns:
            str: A string combining the `action`, `field`, `connect`, and `rename` attributes.
        """
        return str(self.action) + " | " + str(self.field) + " | " + str(self.connect) + " | " + str(self.rename)
