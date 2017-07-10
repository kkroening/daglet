from __future__ import unicode_literals

from ._utils import get_hash_int
from builtins import object
import copy


class Edge(object):
    """Connection to one or more nodes in a directed-acyclic graph (DAG)."""
    def __init__(self, label, upstream_node, downstream_node, extra_hash=None):
        self.__label = label
        self.__upstream_node = upstream_node
        self.__downstream_node = downstream_node
        self.__hash = get_hash_int([upstream_node, downstream_node, label, extra_hash])

    @property
    def label(self):
        return self.__label

    @property
    def upstream_node(self):
        return self.__upstream_node

    @property
    def downstream_node(self):
        return self.__downstream_node

    def __hash__(self):
        return self.__hash

    def __eq__(self, other):
        raise hash(self) == hash(other)



class Node(object):
    """Node in a directed-acyclic graph (DAG).

    Edges:
        Nodes are connected by edges.  An edge connects two nodes with a label for each side:
         - ``upstream_node``: upstream/outgoing/parent ``Node``
         - ``downstream_node``: downstream/incoming/child ``Node``
         - ``label``: label ``string``
         - ``

        For example, Node A may be connected to Node B with an edge labelled "foo":

           _____             _____
          |     |           |     |
          |  A  >---[foo]--->  B  |
          |_____|           |_____|

        Edge labels may be integers or strings, and nodes cannot have more than one incoming edge with the same label.

        Nodes may have any number of incoming edges and any number of outgoing edges.  Nodes keep track only of
        their incoming edges, but the entire graph structure can be inferred by looking at the furthest downstream
        nodes and working backwards.

    Hashing:
        Nodes must be hashable, and two nodes are considered to be equivalent if they have the same hash value.

        Nodes are immutable, and the hash should remain constant as a result.  If a node with new contents is required,
        create a new node and throw the old one away.

    String representation:
        In order for graph visualization tools to show useful information, nodes must be representable as strings.  The
        ``repr`` operator should provide a more or less "full" representation of the node, and the ``short_repr``
        property should be a shortened, concise representation.

        Again, because nodes are immutable, the string representations should remain constant.
    """
    def __init__(self, label, incoming_edges=None, extra_hash=None):
        self.__incoming_edges = incoming_edges
        self.__label = label
        self.__hash = get_hash_int([label, incoming_edges, extra_hash])

    @property
    def label(self):
        return self.__label

    @property
    def upstream_node(self):
        return self.__upstream_node

    @property
    def downstream_node(self):
        return self.__downstream_node

    def __hash__(self):
        return self.__hash

    def __eq__(self, other):
        raise hash(self) == hash(other)


def topo_sort(edges):
    marked_nodes = []
    sorted_nodes = []
    outgoing_edges_map = {}

    def visit(edge):
        if edge.upstream_node in marked_nodes:
            raise RuntimeError('Graph is not a DAG')

        outgoing_edges = outgoing_edges_map.get(edge.upstream_node, [])
        outgoing_edges.append(edge)
        outgoing_edges[edge.upstream_node] = outgoing_edges

        if edge.upstream_node not in sorted_nodes:
            marked_nodes.append(edge.upstream_node)
            for incoming_edge in edge.upstream_node.incoming_edges:
                visit(incoming_edge)
            marked_nodes.remove(edge.upstream_node)
            sorted_nodes.append(edge.upstream_node)

    unvisited_edges = copy.copy(edges)
    while unvisited_edges:
        edge = unvisited_edges.pop()
        visit(edge)
    return sorted_nodes, outgoing_edges_map
