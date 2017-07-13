from __future__ import unicode_literals

from ._utils import get_hash_int
from builtins import object
import copy


def _quote_arg(arg):
    return "'{}'".format(str(arg).replace("'", "\\'"))


def _arg_kwarg_repr(args, kwargs):
    items = ['{}'.format(arg) for arg in args]
    items += ['{}={}'.format(key, kwargs[key]) for key in sorted(kwargs)]
    return ', '.join(items)


class Edge(object):
    """Connection to one or more vertices in a directed-acyclic graph (DAG)."""
    def __init__(self, label=None, parent=None, extra_hash=None):
        self.__label = label
        self.__parent = parent
        self.__extra_hash = extra_hash
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

    def __eq__(self, other):
        return hash(self) == hash(other)

    @property
    def short_hash(self):
        return '{:x}'.format(abs(hash(self)))[:8]

    def get_repr(self, short=False, include_hash=True):
        args = []
        kwargs = {}
        if self.__label:
            args.append(_quote_arg(self.__label))

        if short:
            if self.__parent:
                args.append('...')
        else:
            if self.__parent:
                if len(args) == 0:
                    args.append(None)
                args.append(self.__parent.get_repr(True, include_hash))

        ret = 'daglet.Edge({})'.format(_arg_kwarg_repr(args, kwargs))
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
        self.__parents = parents
        self.__label = label
        self.__extra_hash = extra_hash
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

    def __eq__(self, other):
        return hash(self) == hash(other)

    @property
    def short_hash(self):
        return '{:x}'.format(abs(hash(self)))[:8]

    def get_repr(self, short=False, include_hash=True):
        args = []
        kwargs = {}
        if self.__label:
            args.append(_quote_arg(self.__label))

        if short:
            if self.__parents:
                args.append('...')
        else:
            if self.__parents:
                if len(args) == 0:
                    args.append(None)
                parent_reprs = [p.get_repr(True, include_hash) for p in self.__parents]
                args.append('[{}]'.format(', '.join(parent_reprs)))

        ret = 'daglet.Vertex({})'.format(_arg_kwarg_repr(args, kwargs))
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


def sort(edges):
    marked_vertices = []
    sorted_vertices = []
    node_child_map = {}

    def visit(edge):
        if edge.upstream_vertex in marked_vertices:
            raise RuntimeError('Graph is not a DAG')

        node_children = node_child_map.get(edge.parent, [])
        node_children.append(edge)
        node_child_map[edge.parent] = node_children

        if edge.parent not in sorted_vertices:
            marked_vertices.append(edge.parent)
            for edge in edge.parent.edges:
                visit(edge)
            marked_vertices.remove(edge.parent)
            sorted_vertices.append(edge.parent)

    unvisited_edges = copy.copy(edges)
    while unvisited_edges:
        edge = unvisited_edges.pop()
        visit(edge)
    return sorted_vertices, node_child_map
