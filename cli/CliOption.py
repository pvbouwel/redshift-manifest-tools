class CliOption:
    def __init__(self, name, description, mandatory = False, default=None):
        self.name = name
        self.description = description
        self.mandatory = mandatory

    def __str__(self):
        mandatory_string = ''
        if self.mandatory:
            mandatory_string = ' MANDATORY'
        string = ' * {name}: {desc}{mand}'.format(name=self.name, desc=self.description, mand=mandatory_string)
        return string
