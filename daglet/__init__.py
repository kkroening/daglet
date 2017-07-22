from __future__ import unicode_literals

from ._utils import get_hash_int
from builtins import object
from collections import defaultdict
import copy
import operator


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


def default_get_parents(x):
    return x.parents


def toposort(objs, parent_func=default_get_parents):
    marked_objs = set()
    sorted_objs = []

    def visit(obj, child_obj):
        if obj in marked_objs:
            # TODO: optionally break cycles.
            raise RuntimeError('Graph is not a DAG')

        if obj not in sorted_objs:
            parent_objs = parent_func(obj)

            marked_objs.add(obj)
            for parent_obj in parent_objs:
                visit(parent_obj, obj)
            marked_objs.remove(obj)

            sorted_objs.append(obj)

    unvisited_objs = copy.copy(objs)
    while unvisited_objs:
        obj = unvisited_objs.pop()
        visit(obj, None)
    return sorted_objs


def transform(objs, vertex_func=None, edge_func=None, parent_func=default_get_parents):
    if edge_func is None:
        edge_func = lambda parent_obj, obj, parent_value: parent_value
    if vertex_func is None:
        vertex_func = lambda obj, parent_values: None

    sorted_objs = toposort(objs, parent_func)

    vertex_map = {}
    edge_map = {}
    for obj in sorted_objs:
        parent_objs = parent_func(obj)
        if edge_func is not None:
            parent_values = []
            for parent_obj in parent_objs:
                value = edge_func(parent_obj, obj, vertex_map[parent_obj])
                edge_map[parent_obj, obj] = value
                parent_values.append(value)
        else:
            parent_values = [vertex_map[parent_obj] for parent_obj in parent_objs]
        value = vertex_func(obj, parent_values)
        vertex_map[obj] = value

    return vertex_map, edge_map


def transform_vertices(objs, vertex_func, parent_func=default_get_parents):
    vertex_map, _ = transform(objs, vertex_func, None, parent_func)
    return vertex_map


def transform_edges(objs, edge_func, parent_func=default_get_parents):
    _, edge_map = transform(objs, None, edge_func, parent_func)
    return edge_map


def get_parent_map(objs, parent_func=default_get_parents):
    sorted_objs = toposort(objs, parent_func)
    parent_map = defaultdict(set)
    for obj in sorted_objs:
        for parent in parent_func(obj):
            parent_map[obj].append(parent)
    return parent_map


def get_child_map(objs, parent_func=default_get_parents):
    sorted_objs = toposort(objs, parent_func)
    child_map = defaultdict(set)
    for obj in sorted_objs:
        for parent in parent_func(obj):
            child_map[parent].append(obj)
    return child_map


def invert_multimap(multimap):
    inverted_multimap = defaultdict(set)
    for key, values in multimap.items():
        for value in values:
            inverted_multimap[value].append(key)
    return inverted_multimap


def multimap(mapper, items):
    """Apply a multimap to a set of items in an iterable.

    Params:
        mapper(key), mapper[key]: a function or dictionary that maps a key to a list/set of new keys.  If the function
            returns None or the key is not found in the dictionary, it's assumed to map to the empty set.
    """
    if isinstance(mapper, dict):
        mapper = mapper.get
    mapped = reduce(operator.add, [mapper(x) for x in items])
    return [x for x in mapped if x is not None]


def multimap_keys(mapper, d, clone_func=None, merge_func=None):
    """Apply a multimap to a set of keys in a dictionary.

    Params:
        mapper(key), mapper[key]: a function or dictionary that maps a key to a list/set of new keys.  If the function
            returns None or the key is not found in the dictionary, it's assumed to map to the empty set.
        d: a dictionary with keys to be mapped.
        clone_func(old_key, new_key, value): function to copy values for new keys.
        merge_func(new_key, old_value_map): if two or more keys map to the same value, resolve conflict and return a
            single value.  If no ``merge_func`` and a conflict is encountered, an exception is raised.
    """
    if isinstance(mapper, dict):
        key_map = mapper
    else:
        key_map = {key: mapper(key) for key in d.keys()}

    if clone_func is None:
        clone_func = lambda old_key, new_key, value: value

    inverted_key_map = invert_multimap(key_map)
    new_dict = {}
    for new_key, old_keys in inverted_key_map.items():
        if len(old_keys) > 1:
            if merge_func is None:
                raise KeyError('Multiple keys map to the same value; must specify `merge_func` to resolve conflict')
            old_value_map = {k: d[k] for k in old_keys}
            value = merge_func(new_key, old_value_map)
        else:
            old_key = old_keys[0]
            value = clone_func(old_key, new_key, d[old_key])
        new_dict[new_key] = value
    return value


'''
def squish_vertices(vertices):
    sorted_vertices = toposort(vertices)
    vertex_map = {}
    for vertex in sorted_vertices:
        is_edge = any(x in vertex_map for x in vertex.parents)
        if not is_edge:
            parents = reduce(operator.add, [x.parents for x in vertex.parents], [])
            new_parents = [vertex_map[x] for x in parents]
            vertex_map[vertex] = vertex.clone(parents=new_parents)
    return vertex_map


def double_squish_vertices(vertices):
    sorted_vertices = toposort(vertices)
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


# TODO: possibly move to transformations.py.
#def rebase(downstream_vertices, new_base,
#    ...
'''

from .view import view
(view)  # silence linter
