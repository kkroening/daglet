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


def toposort(objs, parent_func, tree=False):
    marked_objs = set()
    sorted_objs = []

    def visit(obj, child_obj):
        if not tree and obj in marked_objs:
            # TODO: optionally break cycles.
            raise RuntimeError('Graph is not a DAG; recursively encountered {}'.format(obj))

        if tree or obj not in sorted_objs:
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


def transform(objs, parent_func, vertex_func=None, edge_func=None, vertex_map={}):
    if vertex_func is None:
        vertex_func = lambda obj, parent_values: None
    if vertex_map is not None:
        old_parent_func = parent_func
        parent_func = lambda x: old_parent_func(x) if x not in vertex_map else []
    if edge_func is None:
        edge_func = lambda parent_obj, obj, parent_value: parent_value

    sorted_objs = toposort(objs, parent_func)

    new_vertex_map = {}
    new_edge_map = {}
    for obj in sorted_objs:
        if obj in vertex_map:
            value = vertex_map[obj]
        else:
            parent_objs = parent_func(obj)
            parent_values = []
            for parent_obj in parent_objs:
                value = edge_func(parent_obj, obj, new_vertex_map[parent_obj])
                new_edge_map[parent_obj, obj] = value
                parent_values.append(value)
            value = vertex_func(obj, parent_values)
        new_vertex_map[obj] = value

    return new_vertex_map, new_edge_map


def transform_vertices(objs, parent_func, vertex_func, vertex_map={}):
    vertex_map, _ = transform(objs, parent_func, vertex_func, None, vertex_map)
    return vertex_map


def transform_edges(objs, parent_func, edge_func):
    _, edge_map = transform(objs, parent_func, None, edge_func)
    return edge_map


def get_parent_map(objs, parent_func):
    sorted_objs = toposort(objs, parent_func)
    parent_map = defaultdict(set)
    for obj in sorted_objs:
        for parent in parent_func(obj):
            parent_map[obj].append(parent)
    return parent_map


def get_child_map(objs, parent_func):
    sorted_objs = toposort(objs, parent_func)
    child_map = defaultdict(set)
    for obj in sorted_objs:
        for parent in parent_func(obj):
            child_map[parent].add(obj)
    return child_map


def invert_map(map_):
    """Inverts a mapping and produces a multimap to gracefully deal with non-injective mappings."""
    inverted_multimap = defaultdict(set)
    for key, value in map_.items():
        inverted_multimap[value].add(key)
    return inverted_multimap


def invert_multimap(multimap):
    inverted_multimap = defaultdict(set)
    for key, values in multimap.items():
        for value in values:
            inverted_multimap[value].add(key)
    return inverted_multimap


def apply_multi_mapping(multimap, d, clone_func=None, merge_func=None):
    """Apply a multimap to a set of keys in a dictionary.

    TODO: add delete_func.
    TODO: add create_func and call it on any None->[a,b,c...] mapping.

    Params:
        multimap(key), multimap[key]: a function or dictionary that maps a key to a new key or list/set of new keys.
            If the function returns None or the key is not found in the dictionary, it's assumed to map to the empty
            set.
        d: a dictionary with keys to be mapped.
        clone_func(old_key, new_key, value): function to copy values for new keys.
        merge_func(new_key, old_value_map): if two or more keys map to the same value, resolve conflict and return a
            single value.  If no ``merge_func`` and a conflict is encountered, an exception is raised.
    """
    if isinstance(multimap, dict):
        key_map = multimap
    else:
        key_map = {key: multimap(key) for key in d.keys()}

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
            old_key = list(old_keys)[0]
            value = clone_func(old_key, new_key, d[old_key])
        new_dict[new_key] = value
    return new_dict


def apply_mapping(map_, d, clone_func=None, merge_func=None):
    if isinstance(map_, dict):
        multimap = {k: [v] for k, v in map_.items()}
    else:
        multimap = {k: map_(k) for k in d.keys()}
    return apply_multi_mapping(multimap, d, clone_func, merge_func)


def get_edge_map(vertex_map, source_parent_func, dest_parent_func):
    #raise NotImplementedError()
    return {}


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
