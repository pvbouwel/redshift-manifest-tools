import base64
import click


class SymmetricKey:
    def __init__(self, data, base_64_encoded=True):
        if base_64_encoded:
            self.key_data = base64.b64decode(data)
        else:
            self.key_data = data

    def get_key_data(self):
        return self.key_data


class SymmetricKeyParamType(click.ParamType):
    name = 'symmetric-key'

    def convert(self, value, param, ctx):
        try:
            return SymmetricKey(value)
        except Exception as e:
            self.fail('{val} is not a valid base 64 encoded symmetric key: {err}'.format(val=value, err=str(e)),
                      param,
                      ctx)
