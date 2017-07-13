from __future__ import unicode_literals

import daglet


def test_edge_eq_no_parents_match():
    assert daglet.Edge('e1') == daglet.Edge('e1')


def test_edge_eq_no_parents_label_mismatch():
    assert daglet.Edge('e1') != daglet.Edge('e2')


def test_edge_eq_no_parents_extra_hash_match():
    assert daglet.Edge('e1', extra_hash=1) == daglet.Edge('e1', extra_hash=1)


def test_edge_eq_no_parents_extra_hash_mismatch():
    assert daglet.Edge('e1', extra_hash=1) != daglet.Edge('e1', extra_hash=2)


def test_edge_eq_match():
    assert daglet.Vertex('v1').edge('e1') == daglet.Vertex('v1').edge('e1')
    assert daglet.Vertex('v1').edge('e1') == daglet.Edge('e1', daglet.Vertex('v1'))


def test_edge_eq_label_mismatch():
    assert daglet.Vertex('v1').edge('e1') != daglet.Vertex('v1').edge('e2')


def test_edge_eq_parent_mismatch():
    assert daglet.Vertex('v1').edge('e1') != daglet.Vertex('v2').edge('e1')


def test_edge_eq_extra_hash_match():
    assert daglet.Vertex('v1').edge('e1', extra_hash=1) == daglet.Vertex('v1').edge('e1', extra_hash=1)


def test_edge_eq_extra_hash_mismatch():
    assert daglet.Vertex('v1').edge('e1', extra_hash=1) != daglet.Vertex('v1').edge('e1', extra_hash=2)


def test_vertex_eq_no_parents_match():
    assert daglet.Vertex('v1') == daglet.Vertex('v1')


def test_vertex_eq_no_parents_label_mismatch():
    assert daglet.Vertex('v1') != daglet.Vertex('v2')


def test_vertex_eq_no_parents_extra_hash_match():
    assert daglet.Vertex('v1', extra_hash=1) == daglet.Vertex('v1', extra_hash=1)


def test_vertex_eq_no_parents_extra_hash_mismatch():
    assert daglet.Vertex('v1', extra_hash=1) != daglet.Vertex('v1', extra_hash=2)


def test_vertex_eq_match():
    assert daglet.Vertex('v1').edge('e1').vertex('v2') == daglet.Vertex('v1').edge('e1').vertex('v2')


def test_vertex_eq_multiple_parents_match():
    v3a = (daglet.Vertex('v3', [
        daglet.Vertex('v1').edge('e1'),
        daglet.Vertex('v2').edge('e2')
    ]))
    v3b = (daglet.Vertex('v3', [
        daglet.Vertex('v1').edge('e1'),
        daglet.Vertex('v2').edge('e2')
    ]))
    assert v3a == v3b


def test_vertex_eq_multiple_parents_wrong_order():
    v3a = (daglet.Vertex('v3', [
        daglet.Vertex('v1').edge('e1'),
        daglet.Vertex('v2').edge('e2')
    ]))
    v3b = (daglet.Vertex('v3', [
        daglet.Vertex('v2').edge('e2'),
        daglet.Vertex('v1').edge('e1')
    ]))
    assert v3a != v3b


def test_vertex_label_mismatch():
    assert daglet.Vertex('v1').edge('e1').vertex('v2') != daglet.Vertex('v1').edge('e1').vertex('v3')


def test_vertex_edge_mismatch():
    assert daglet.Vertex('v1').edge('e1').vertex('v2') != daglet.Vertex('v1').edge('e2').vertex('v2')


def test_edge_clone():
    # FIXME
    pass


def test_vertex_clone():
    assert daglet.Vertex('v1').clone(label='v2').label == 'v2'


def test_vertex_transplant():
    v2 = daglet.Vertex('v2')
    assert daglet.Vertex('v1').transplant([v2]).parents == [v2]


def test_vertex_short_repr():
    assert daglet.Vertex('v1').short_repr == "daglet.Vertex({!r})".format('v1')


def test_vertex_short_hash():
    short_hash = daglet.Vertex('v1').short_hash
    assert int(short_hash, base=16)


def test_vertex_repr():
    v1 = daglet.Vertex('v1')
    assert repr(v1) == '{} <{}>'.format(v1.short_repr, v1.short_hash)
