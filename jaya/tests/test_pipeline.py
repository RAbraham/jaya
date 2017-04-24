import unittest
from jaya.pipeline.pipe import Leaf, Composite


class PipelineTestCase(unittest.TestCase):
    def test_single_node_tree(self):
        tree = Leaf(5)
        self.assertEqual(tree.children(), [])

    def test_two_node_tree(self):
        n1 = Leaf(5)
        n2 = Leaf(6)
        tree = n1 >> n2
        self.assertEqual(tree.children(), [n2])

    def test_single_node_two_children(self):
        n1 = Leaf(5)
        n2 = Leaf(6)
        n3 = Leaf(7)
        tree = n1 >> [n2, n3]
        self.assertEqual(tree.children(), [n2, n3])
        pass

    def test_three_node_trees(self):
        n1 = Leaf(5)
        n2 = Leaf(6)
        n3 = Leaf(7)
        tree = n1 >> n2 >> n3

        # self.assertEqual(tree.children(), [n2])
        self.assertEqual(tree.children(), [Composite(n2, [n3])])
        self.assertEqual(tree.children()[0].children(), [n3])



# TODO: Test empty tree

if __name__ == '__main__':
    unittest.main()
