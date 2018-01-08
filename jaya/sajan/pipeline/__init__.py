from typing import List
from .pipe import Tree


class Pipeline(object):
    def __init__(self, name: str, pipes: List[Tree]):
        self.name = name
        self.pipes = pipes
