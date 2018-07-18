import dill

handler_path = './handler.dill'

h = None
with open(handler_path, "rb") as f:
    h = dill.load(f)


def handler(event, context):
    h.handler(event, context)
