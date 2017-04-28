from abc import ABCMeta
from jaya.lib import util


class Tree(object):
    pass


class Composite(Tree):
    def __init__(self, root, children):
        self._root = root
        self._children = children

    def __rshift__(self, one_or_list):

        '''
        A composite can add a node(or more) if it only has one leaf. If there are many leaves, which leaf node can one add to?
        :param other:
        :return:
        '''
        pass
        children = util.listify(one_or_list)
        return self.update_edge(children)

    def update_edge(self, new_leaves):
        if len(self._children) > 1:
            raise ValueError("Can't add leaves to a node with multiple children")
        else:
            return Composite(self, self._update_edge(self._children[0], new_leaves))

    def root(self):
        return self._root

    def children(self):
        # return [c.root() for c in self._children]
        return self._children

    @staticmethod
    def _update_edge(child, new_leaves):
        return [child.update_edge(new_leaves)]

    def __repr__(self):
        return 'Composite(' + str(self._root) + ',' + str(self._children) + ')'

    def __hash__(self):
        return hash(frozenset(self.__dict__))

    def __eq__(self, other):
        return hash(frozenset(self.__dict__))


class Leaf(Tree):
    def __init__(self, value):
        self.value = value

    def __repr__(self):
        return 'Node:' + str(self.value)

    def root(self):
        return self

    def __rshift__(self, one_or_list):
        children = util.listify(one_or_list)
        return Composite(self, children)

    def update_edge(self, new_leaves):
        return Composite(self, new_leaves)

    def children(self):
        return []

    pass

# class Leaf(Tree):
#     def __init__(self, value):
#         self.value = value
#
#     def __len__(self):
#         return 1
#
#     def __rshift__(self, other):
#
#     # def __pow__(self, *children):
#     #     self.children.extend(children)
#     #     return self
#
#     def __repr__(self):
#         return 'Node value:' + str(self.value)