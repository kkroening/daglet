from __future__ import unicode_literals

from ._utils import get_hash_int
from builtins import object
import copy


def _quote_arg(arg):
    return "'{}'".format(str(arg).replace("'", "\\'"))


def _arg_kwarg_repr(args=[], kwargs={}):
    items = ['{}'.format(arg) for arg in args]
    items += ['{}={}'.format(key, kwargs[key]) for key in sorted(kwargs)]
    return ', '.join(items)


class Edge(object):
    """Connection to one or more vertices in a directed-acyclic graph (DAG)."""
    def __init__(self, label=None, parent=None, extra_hash=None):
        if not isinstance(parent, Vertex):
            raise TypeError('Expected Vertex instance; got {}'.format(parent))
        self.__label = copy.copy(label)
        self.__parent = copy.copy(parent)
        self.__extra_hash = copy.copy(extra_hash)
        self.__hash = get_hash_int([label, parent, extra_hash])

    @property
    def label(self):
        return self.__label

    @property
    def parent(self):
        return self.__parent

    @property
    def extra_hash(self):
        return self.__extra_hash

    def __hash__(self):
        return self.__hash

    def __lt__(self, other):
        return hash(self) < hash(other)

    def __le__(self, other):
        return hash(self) <= hash(other)

    def __eq__(self, other):
        return hash(self) == hash(other)

    def __ne__(self, other):
        return hash(self) != hash(other)

    def __ge__(self, other):
        return hash(self) >= hash(other)

    def __gt__(self, other):
        return hash(self) > hash(other)

    @property
    def short_hash(self):
        return '{:x}'.format(abs(hash(self)))[:8]

    def get_repr(self, include_hash=True):
        args = []
        if self.__label is not None:
            args.append(repr(self.__label))
        if self.__parent or self.__extra_hash:
            args.append('...')
        ret = 'daglet.Edge({})'.format(_arg_kwarg_repr(args))
        if include_hash:
            ret = '{} <{}>'.format(ret, self.short_hash)
        return ret

    def __repr__(self):
        return self.get_repr()

    def clone(self, **kwargs):
        base_kwargs = {
            'label': self.__label,
            'parent': self.__parent,
            'extra_hash': self.__extra_hash,
        }
        base_kwargs.update(kwargs)
        return Edge(**base_kwargs)

    def vertex(self, label=None, extra_hash=None):
        return Vertex(label, [self], extra_hash)


class Vertex(object):
    """Vertex in a directed-acyclic graph (DAG).

    Hashing:
        Vertices must be hashable, and two vertices are considered to be equivalent if they have the same hash value.

        Vertices are immutable, and the hash should remain constant as a result.  If a vertex with new contents is
        required, create a new vertex and throw the old one away.
    """
    def __init__(self, label=None, parents=[], extra_hash=None):
        for parent in parents:
            if not isinstance(parent, Edge):
                raise TypeError('Expected Edge instance; got {}'.format(parent))
        parents = sorted(parents)
        self.__parents = parents
        self.__label = copy.copy(label)
        self.__extra_hash = copy.copy(extra_hash)
        self.__hash = get_hash_int([label, parents, extra_hash])

    @property
    def parents(self):
        return self.__parents

    @property
    def label(self):
        return self.__label

    @property
    def extra_hash(self):
        return self.__extra_hash

    def __hash__(self):
        return self.__hash

    def __lt__(self, other):
        return hash(self) < hash(other)

    def __le__(self, other):
        return hash(self) <= hash(other)

    def __eq__(self, other):
        return hash(self) == hash(other)

    def __ne__(self, other):
        return hash(self) != hash(other)

    def __ge__(self, other):
        return hash(self) >= hash(other)

    def __gt__(self, other):
        return hash(self) > hash(other)

    @property
    def short_hash(self):
        return '{:x}'.format(abs(hash(self)))[:8]

    def get_repr(self, include_hash=True):
        args = []
        if self.__label is not None:
            args.append(repr(self.__label))
        if self.__parents or self.__extra_hash:
            args.append('...')
        ret = 'daglet.Vertex({})'.format(_arg_kwarg_repr(args))
        if include_hash:
            ret = '{} <{}>'.format(ret, self.short_hash)
        return ret

    def __repr__(self):
        return self.get_repr()

    def clone(self, **kwargs):
        base_kwargs = {
            'label': self.__label,
            'parents': self.__parents,
            'extra_hash': self.__extra_hash,
        }
        base_kwargs.update(kwargs)
        return Vertex(**base_kwargs)

    def transplant(self, new_parents):
        """Create a copy of this Vertex with new parent edges."""
        return Vertex(self.__label, new_parents, self.__extra_hash)

    def edge(self, label=None, extra_hash=None):
        """Create edge with specified label.

        Example:
            The following example creates a DAG with three vertices connected with two edges (``n1 -> n2 -> n3``):
            ```
            n3 = (daglet
                .Vertex('n1').edge('e1')
                .vertex('n2').edge('e2')
                .vertex('n3')
            )
            ```
        """
        return Edge(label, self, extra_hash)


def analyze(vertices):
    marked_vertices = []
    sorted_vertices = []
    vertex_child_map = {}
    edge_child_map = {}

    def visit(vertex, child_edge):
        if vertex in marked_vertices:
            raise RuntimeError('Graph is not a DAG')

        vertex_children = vertex_child_map.get(vertex, set())
        if child_edge is not None:
            vertex_children.add(child_edge)
        vertex_child_map[vertex] = vertex_children

        if vertex not in sorted_vertices:
            marked_vertices.append(vertex)
            for edge in vertex.parents:
                edge_children = edge_child_map.get(edge, set())
                edge_children.add(vertex)
                edge_child_map[edge] = edge_children
                visit(edge.parent, edge)
            marked_vertices.remove(vertex)
            sorted_vertices.append(vertex)

    unvisited_vertices = copy.copy(vertices)
    while unvisited_vertices:
        vertex = unvisited_vertices.pop()
        visit(vertex, child_edge=None)
    return sorted_vertices, vertex_child_map, edge_child_map
