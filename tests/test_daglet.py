from __future__ import unicode_literals

import daglet


def test_edge_label():
    assert daglet.Edge('e1').label == 'e1'


def test_edge_parent():
    assert daglet.Edge('e1').parent == None
    assert daglet.Vertex('v1').edge('e1').parent == daglet.Vertex('v1')


def test_edge_extra_hash():
    assert daglet.Edge(extra_hash=5).extra_hash == 5


def test_edge_hash():
    assert isinstance(hash(daglet.Edge('e1')), int)
    assert hash(daglet.Edge('e1')) == hash(daglet.Edge('e1'))
    assert hash(daglet.Edge('e1')) != hash(daglet.Edge('e2'))


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


def test_edge_short_hash():
    h1 = daglet.Edge('e1').short_hash
    h2 = daglet.Edge('e1').short_hash
    h3 = daglet.Edge('e2').short_hash
    assert isinstance(h1, basestring)
    assert int(h1, base=16)
    assert len(h1) == 8
    assert h1 == h2
    assert h1 != h3


def test_edge_get_repr():
    e1 = daglet.Edge('e1')
    v1 = daglet.Vertex('v1')
    e2 = v1.edge('e2')
    v2 = e1.vertex('v2')
    e3 = v2.edge('e3')
    assert e1.get_repr() == "daglet.Edge('e1') <{}>".format(e1.short_hash)
    assert e2.get_repr() == "daglet.Edge('e2', daglet.Vertex('v1') <{}>) <{}>".format(v1.short_hash, e2.short_hash)
    assert e3.get_repr() == "daglet.Edge('e3', daglet.Vertex('v2', ...) <{}>) <{}>".format(v2.short_hash,
        e3.short_hash)
    assert e3.get_repr(include_hash=False) == "daglet.Edge('e3', daglet.Vertex('v2', ...))"
    assert e3.get_repr(short=True) == "daglet.Edge('e3', ...) <{}>".format(e3.short_hash)
    assert daglet.Edge().get_repr(short=True, include_hash=False) == 'daglet.Edge()'
    assert daglet.Vertex().edge().get_repr(short=True, include_hash=False) == 'daglet.Edge(...)'


def test_edge_clone():
    assert daglet.Edge('e1').clone(label='e2').label == 'e2'


def test_vertex_parents():
    assert daglet.Vertex('v1').parents == []
    assert daglet.Edge('e1').vertex('e1').parents == [daglet.Edge('e1')]


def test_vertex_label():
    assert daglet.Vertex('v1').label == 'v1'


def test_vertex_hash():
    assert isinstance(hash(daglet.Vertex('v1')), int)
    assert hash(daglet.Vertex('v1')) == hash(daglet.Vertex('v1'))
    assert hash(daglet.Vertex('v1')) != hash(daglet.Vertex('v2'))


def test_vertex_extra_hash():
    assert daglet.Vertex(extra_hash=5).extra_hash == 5


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


def test_vertex_eq_label_mismatch():
    assert daglet.Vertex('v1').edge('e1').vertex('v2') != daglet.Vertex('v1').edge('e1').vertex('v3')


def test_vertex_eq_parent_mismatch():
    assert daglet.Vertex('v1').edge('e1').vertex('v2') != daglet.Vertex('v1').edge('e2').vertex('v2')


def test_vertex_short_hash():
    h1 = daglet.Vertex('v1').short_hash
    h2 = daglet.Vertex('v1').short_hash
    h3 = daglet.Vertex('v2').short_hash
    assert isinstance(h1, basestring)
    assert int(h1, base=16)
    assert len(h1) == 8
    assert h1 == h2
    assert h1 != h3


def test_vertex_get_repr():
    v1 = daglet.Vertex('v1')
    e1 = daglet.Edge('e1')
    v2 = e1.vertex('v2')
    e2 = v1.edge('e2')
    v3 = e2.vertex('v3')
    v4 = daglet.Vertex('v4', [e1, e2])
    assert v1.get_repr() == "daglet.Vertex('v1') <{}>".format(v1.short_hash)
    assert v2.get_repr() == "daglet.Vertex('v2', [daglet.Edge('e1') <{}>]) <{}>".format(e1.short_hash, v2.short_hash)
    assert v3.get_repr() == "daglet.Vertex('v3', [daglet.Edge('e2', ...) <{}>]) <{}>".format(e2.short_hash,
        v3.short_hash)
    assert v3.get_repr(include_hash=False) == "daglet.Vertex('v3', [daglet.Edge('e2', ...)])"
    assert v3.get_repr(short=True) == "daglet.Vertex('v3', ...) <{}>".format(v3.short_hash)
    assert daglet.Vertex().get_repr(short=True, include_hash=False) == 'daglet.Vertex()'
    assert daglet.Edge().vertex().get_repr(short=True, include_hash=False) == 'daglet.Vertex(...)'
    assert v4.get_repr(include_hash=False) == "daglet.Vertex('v4', [daglet.Edge('e1'), daglet.Edge('e2', ...)])"


def test_vertex_clone():
    assert daglet.Vertex('v1').clone(label='v2').label == 'v2'


def test_vertex_transplant():
    v2 = daglet.Vertex('v2')
    assert daglet.Vertex('v1').transplant([v2]).parents == [v2]
