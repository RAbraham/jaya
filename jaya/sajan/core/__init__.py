from jaya.sajan.pipeline.pipe import Leaf


class Service(Leaf):
    def __init__(self, **kwargs):
        assert 'service_name' in kwargs, "name is a mandatory field for Service"
        self.service_name = kwargs['service_name']
        self.kwargs = kwargs
        super(Service, self).__init__(kwargs)

    def __hash__(self):
        return hash(frozenset(self.__dict__))

    def __eq__(self, other):
        return self.__dict__ == other.__dict__


class SajanContext(object):
    def __init__(self, children=None):
        self._children = children

    def children(self):
        return self._children
