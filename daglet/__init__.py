from __future__ import unicode_literals

from .view import view
from ._utils import get_hash_int
from builtins import object
import copy
import operator


(view)  # silence linter


def _quote_arg(arg):
    return "'{}'".format(str(arg).replace("'", "\\'"))


def _arg_kwarg_repr(args=[], kwargs={}):
    items = ['{}'.format(arg) for arg in args]
    items += ['{}={}'.format(key, kwargs[key]) for key in sorted(kwargs)]
    return ', '.join(items)


class Vertex(object):
    """Vertex in a directed-acyclic graph (DAG).

    Hashing:
        Vertices must be hashable, and two vertices are considered to be equivalent if they have the same hash value.

        Vertices are immutable, and the hash should remain constant as a result.  If a vertex with new contents is
        required, create a new vertex and throw the old one away.
    """
    def __init__(self, label=None, parents=[], extra_hash=None):
        for parent in parents:
            if not isinstance(parent, Vertex):
                raise TypeError('Expected Vertex instance; got {}'.format(parent))
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

    def vertex(self, label=None, extra_hash=None):
        """Create downstream vertex with specified label.

        Example:
            The following example creates a DAG with three vertices connected with two edges (``n1 -> n2 -> n3``):
            ```
            n3 = daglet.Vertex('n1').vertex('n2').vertex('n3')
            ```
        """
        return Vertex(label, [self], extra_hash)


def map_object_graph(objs, start_func, finish_func=None):
    marked_objs = set()
    sorted_objs = []
    child_map = {}
    value_map = {}

    def visit(obj, child_obj):
        if obj in marked_objs:
            # TODO: optionally break cycles.
            raise RuntimeError('Graph is not a DAG')

        children = child_map.get(obj, set())
        if child_obj is not None:
            children.add(child_obj)
        child_map[obj] = children

        if obj not in sorted_objs:
            parent_objs = start_func(obj)

            marked_objs.add(obj)
            for parent_obj in parent_objs:
                visit(parent_obj, obj)
            marked_objs.remove(obj)

            sorted_objs.append(obj)
            parent_values = [value_map[parent_obj] for parent_obj in parent_objs]
            if finish_func is not None:
                value = finish_func(obj, parent_values)
            else:
                value = None
            value_map[obj] = value

    unvisited_objs = copy.copy(objs)
    while unvisited_objs:
        obj = unvisited_objs.pop()
        visit(obj, None)
    return sorted_objs, child_map, value_map


def analyze(vertices):
    sorted_vertices, child_map, _ = map_object_graph(vertices, lambda x: x.parents)
    return sorted_vertices, child_map


def get_edges(objs, child_map=None):
    if child_map is None:
        for obj in objs:
            if not isinstance(obj, Vertex):
                raise TypeError('Expected Vertex instance; got {}'.format(obj))
        objs, child_map, _ = map_object_graph(objs, lambda x: x.parents)
    edges = []
    for parent_obj in objs:
        edges += [(parent_obj, child_obj) for child_obj in child_map[parent_obj]]
    return edges


def squish_vertices(vertices):
    sorted_vertices, child_map = analyze(vertices)
    vertex_map = {}
    for vertex in sorted_vertices:
        is_edge = any(x in vertex_map for x in vertex.parents)
        if not is_edge:
            parents = reduce(operator.add, [x.parents for x in vertex.parents], [])
            new_parents = [vertex_map[x] for x in parents]
            vertex_map[vertex] = vertex.clone(parents=new_parents)
    return vertex_map


def double_squish_vertices(vertices):
    sorted_vertices, child_map = analyze(vertices)
    vertex_map = {}
    for vertex in sorted_vertices:
        is_output = any(x in vertex_map for x in vertex.parents)
        if not is_output:
            parents = reduce(operator.add, [x.parents for x in vertex.parents], [])
            is_input = any(x in vertex_map for x in parents)
            if not is_input:
                parents = reduce(operator.add, [x.parents for x in parents], [])
                new_parents = [vertex_map[x] for x in parents]
                vertex_map[vertex] = vertex.clone(parents=new_parents)
    return vertex_map


def convert_obj_graph_to_dag(objs, get_parents_func, repr_func=repr):
    def start(obj):
        parents = get_parents_func(obj)
        return parents

    def finish(obj, parent_vertices):
        return Vertex(repr_func(obj), parent_vertices)

    _, _, value_map = map_object_graph(objs, start, finish)
    return value_map


class Dag(object):
    def __init__(self, vertices):
        vertices, child_map = analyze(vertices)
        self.__vertices = vertices
        self.__child_map = child_map

    @property
    def vertices(self):
        return self.__vertices

    @property
    def child_map(self):
        return self.__child_map

    def map(self, func):
        out = {}
        for vertex in self.__vertices:
            out[vertex] = func(vertex)
        return out


def convert_dag_to_obj_graph(vertices, visit_vertex_func):
    return map_object_graph(vertices, lambda x: x.parents, visit_vertex_func)


def invert(d):
    return {v: k for k, v in d.items()}


def commute(xform1, xform2):
    return {k: xform2[v] for k, v in xform1.items() if k in xform2}


def transform_list(xform, l):
    return [xform[x] for x in l if x in xform]


def transform_keys(key_xform, d):
    # TODO: figure out how to deal with non-injective mappings
    return {key_xform[k]: v for k, v in d.items() if k in key_xform}


#def map_keys(func, d):
#    out = {}
#    for k, v in d.items():
#        k = func(k)
#        if k is not None:
#            out[k] = v
#    return out


# TODO: possibly move to transformations.py.
#def rebase(downstream_vertices, new_base,
#    ...
