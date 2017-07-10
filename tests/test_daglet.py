from __future__ import unicode_literals

import daglet


def test_edge_eq_no_vertices_label_mismatch():
    assert daglet.Edge('e1', None) != daglet.Edge('e2', None)


def test_edge_eq_no_vertices_extra_hash_match():
    assert daglet.Edge('e1', None, extra_hash=1) == daglet.Edge('e1', None, extra_hash=1)


def test_edge_eq_no_vertices_extra_hash_mismatch():
    assert daglet.Edge('e1', None, extra_hash=1) != daglet.Edge('e1', None, extra_hash=2)


def test_edge_eq_fully_connected_same():
    assert daglet.Edge('e1', daglet.Vertex('v1'), daglet.Vertex('v2')) == \
        daglet.Edge('e1', daglet.Vertex('v1'), daglet.Vertex('v2'))


def test_edge_eq_fully_connected_label_mismatch():
    assert daglet.Edge('e1', daglet.Vertex('v1'), daglet.Vertex('v2')) != \
        daglet.Edge('e2', daglet.Vertex('v1'), daglet.Vertex('v2'))


def test_edge_eq_fully_upstream_vertex_mismatch():
    assert daglet.Edge('e1', daglet.Vertex('v1'), daglet.Vertex('v2')) != \
        daglet.Edge('e1', daglet.Vertex('v0'), daglet.Vertex('v2'))


def test_edge_eq_fully_downstream_vertex_mismatch():
    assert daglet.Edge('e1', daglet.Vertex('v1'), daglet.Vertex('v2')) != \
        daglet.Edge('e1', daglet.Vertex('v1'), daglet.Vertex('v3'))


def test_edge_eq_same():
    assert daglet.Vertex('v1').edge('e1') == daglet.Vertex('v1').edge('e1')


def test_edge_eq_label_mismatch():
    assert daglet.Vertex('v1').edge('e1') != daglet.Vertex('v1').edge('e2')


def test_edge_eq_upstream_mismatch():
    assert daglet.Vertex('v1').edge('e1') != daglet.Vertex('v1').edge('e2')


def test_edge_eq_reverse():
    assert daglet.Vertex('v1').edge('e1').reverse() == daglet.Vertex('v1').edge('e1').reverse()


def test_edge_eq_reverse_reverse():
    assert daglet.Vertex('v1').edge('e1').reverse().reverse() == daglet.Vertex('v1').edge('e1')


def test_vertex_eq_no_edges_same():
    assert daglet.Vertex('v1') == daglet.Vertex('v1')


def test_vertex_eq_no_edges_label_mismatch():
    assert daglet.Vertex('v1') != daglet.Vertex('v2')


def test_vertex_eq_no_edges_extra_hash_match():
    assert daglet.Vertex('v1', extra_hash=1) == daglet.Vertex('v1', extra_hash=1)


def test_vertex_eq_no_edges_extra_hash_mismatch():
    assert daglet.Vertex('v1', extra_hash=1) != daglet.Vertex('v1', extra_hash=2)


def test_edge_eq_no_vertices_same():
    assert daglet.Edge('e1', None) == daglet.Edge('e1', None)


def test_vertex_eq_same_one_incoming_edge():
    assert daglet.Vertex('v1').edge('e1').vertex('v2') == daglet.Vertex('v1').edge('e1').vertex('v2')


def test_vertex_eq_multi_incoming_same():
    v3a = (daglet.Vertex('v3', [
        daglet.Vertex('v1').edge('e1'),
        daglet.Vertex('v2').edge('e2')
    ]))
    v3b = (daglet.Vertex('v3', [
        daglet.Vertex('v1').edge('e1'),
        daglet.Vertex('v2').edge('e2')
    ]))
    assert v3a == v3b


def test_vertex_eq_multi_incoming_wrong_order():
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


def test_vertex_reversed_edge_match():
    assert daglet.Vertex('v1').edge('e1').reverse().vertex('v2') != \
        daglet.Vertex('v1').edge('e1').reverse().vertex('v2')


def test_vertex_reversed_edge_mismatch():
    assert daglet.Vertex('v1').edge('e1').reverse().vertex('v2') != \
        daglet.Vertex('v1').edge('e2').reverse().vertex('v2')


def test_vertex_upstream_edges():
    daglet.Edge('e1', daglet.Vertex('v1'), daglet.Vertex('v2'))
    e1 = daglet.Vertex('v1').edge('e1')
    e2 = daglet.Vertex('v3').edge('e2').reverse()
    v2 = daglet.Vertex('v2', [e1, e2])
    assert v2.upstream_edges == [e1]
    assert v2.downstream_edges == [e2]


def test_vertex_downstream_edges():
    pass  # FIXME


def test_vertex_clone():
    assert daglet.Vertex('v1').clone(label='v2').label == 'v2'


def test_vertex_transplant():
    v2 = daglet.Vertex('v2')
    assert daglet.Vertex('v1').transplant([v2]).edges == [v2]


def test_vertex_short_repr():
    assert daglet.Vertex('v1').short_repr == "daglet.Vertex({!r})".format('v1')


def test_vertex_short_hash():
    short_hash = daglet.Vertex('v1').short_hash
    assert int(short_hash, base=16)


def test_vertex_repr():
    v1 = daglet.Vertex('v1')
    assert repr(v1) == '{} <{}>'.format(v1.short_repr, v1.short_hash)
