from __future__ import unicode_literals

from ._utils import get_hash_int
from builtins import object
import copy


def _arg_kwarg_repr(args, kwargs):
    items = ['{!r}'.format(arg) for arg in args]
    items += ['{}={!r}'.format(key, kwargs[key]) for key in sorted(kwargs)]
    return ', '.join(items)


class Edge(object):
    """Connection to one or more vertices in a directed-acyclic graph (DAG)."""
    def __init__(self, label, upstream_vertex, downstream_vertex=None, extra_hash=None):
        self.__label = label
        self.__upstream_vertex = upstream_vertex
        self.__downstream_vertex = downstream_vertex
        self.__extra_hash = extra_hash
        self.__hash = get_hash_int([label, upstream_vertex, downstream_vertex, extra_hash])

    @property
    def label(self):
        return self.__label

    def clone(self, **kwargs):
        base_kwargs = {
            'label': self.__label,
            'upstream_vertex': self.__upstream_vertex,
            'downstream_vertex': self._downstream_vertex,
            'extra_hash': self.__extra_hash,
        }
        base_kwargs.update(kwargs)
        return Edge(**base_kwargs)

    @property
    def upstream_vertex(self):
        return self.__upstream_vertex

    @property
    def downstream_vertex(self):
        return self.__downstream_vertex

    def __hash__(self):
        return self.__hash

    def __eq__(self, other):
        return hash(self) == hash(other)

    @property
    def short_repr(self):
        upstream = repr(self.__upstream_vertex) if self.__upstream_vertex is not None else None
        downstream = repr(self.__downstream_vertex) if self.__downstream_vertex is not None else None
        return 'daglet.Edge({!r}, upstream_vertex={}, downstream_vertex={})'.format(self.__label, upstream, downstream)

    @property
    def short_hash(self):
        return '{:x}'.format(abs(hash(self)))[:8]

    def __repr__(self):
        return '{} <{}>'.format(self.short_repr, self.short_hash)

    @property
    def upstream_index(self):
        if self.__upstream_vertex is not None:
            index = self.__upstream_vertex.get_incoming_edge_index(self)
        else:
            index = None
        return index

    @property
    def downstream_index(self):
        if self.__downstream_vertex is not None:
            index = self.__downstream_vertex.get_incoming_edge_index(self)
        else:
            index = None
        return index

    def vertex(self, label, extra_hash=None):
        return Vertex(label, extra_hash, [self])

    def reverse(self):
        return Edge(self.__label, self.__downstream_vertex, self.__upstream_vertex, self.__extra_hash)


class Vertex(object):
    """Vertex in a directed-acyclic graph (DAG).

    Hashing:
        Vertices must be hashable, and two vertices are considered to be equivalent if they have the same hash value.

        Vertices are immutable, and the hash should remain constant as a result.  If a vertex with new contents is
        required, create a new vertex and throw the old one away.
    """
    def __init__(self, label, edges=[], extra_hash=None):
        self.__edges = edges
        self.__label = label
        self.__extra_hash = extra_hash
        self.__hash = get_hash_int([label, edges, extra_hash])

    @property
    def edges(self):
        return self.__edges

    @property
    def incoming_edges(self):
        return [x for x in self.__edges if x.upstream_vertex not in [None, self]]

    @property
    def outgoing_edges(self):
        return [x for x in self.__edges if x.downstream_vertex not in [None, self]]

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

    def clone(self, **kwargs):
        base_kwargs = {
            'label': self.__label,
            'edges': self.__edges,
            'extra_hash': self.__extra_hash,
        }
        base_kwargs.update(kwargs)
        return Vertex(**base_kwargs)

    def transplant(self, new_edges):
        """Create a copy of this Vertex with new edges."""
        return Vertex(self.__label, new_edges, self.__extra_hash)

    def edge(self, label=None):
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
        return Edge(label, self)

    @property
    def short_repr(self):
        args = [self.__label]
        kwargs = {}
        if self.__extra_hash is not None:
            kwargs['extra_hash'] = self.__extra_hash
        return 'daglet.Vertex({})'.format(_arg_kwarg_repr(args, kwargs))

    @property
    def short_hash(self):
        return '{:x}'.format(abs(hash(self)))[:8]

    def __repr__(self):
        return '{} <{}>'.format(self.short_repr, self.short_hash)


def sort(edges):
    marked_vertices = []
    sorted_vertices = []
    node_edges_map = {}

    def visit(edge):
        if edge.upstream_vertex in marked_vertices:
            raise RuntimeError('Graph is not a DAG')

        node_edges = node_edges_map.get(edge.upstream_vertex, [])
        node_edges.append(edge)
        node_edges_map[edge.upstream_vertex] = node_edges

        if edge.upstream_vertex not in sorted_vertices:
            marked_vertices.append(edge.upstream_vertex)
            for edge in edge.upstream_vertex.edges:
                visit(edge)
            marked_vertices.remove(edge.upstream_vertex)
            sorted_vertices.append(edge.upstream_vertex)

    unvisited_edges = copy.copy(edges)
    while unvisited_edges:
        edge = unvisited_edges.pop()
        visit(edge)
    return sorted_vertices, node_edges_map
