class CliAction():
    def __init__(self, name, description, options=None):
        self.name = name
        self.description = description
        self.options = options

    def __str__(self):
        string = ' - {name}: {desc}'.format(name=self.name, desc=self.description)
        if self.options is not None:
            for option in self.options:
                string += '\n\t' + str(option)
        return string
