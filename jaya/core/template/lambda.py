import dill

handler_path = './handler.dill'
h = dill.load(open(handler_path, "rb"))


def handler(event, context):
    h(event, context)
