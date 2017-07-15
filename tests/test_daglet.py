from __future__ import unicode_literals

import daglet
import itertools
import json


def test_edge_label():
    assert daglet.Vertex().edge().label == None
    assert daglet.Vertex().edge('e1').label == 'e1'


def test_edge_parent():
    assert daglet.Vertex().edge().parent == daglet.Vertex()
    assert daglet.Vertex().edge().parent == daglet.Vertex()


def test_edge_extra_hash():
    assert daglet.Vertex().edge().extra_hash == None
    assert daglet.Vertex().edge(extra_hash=5).extra_hash == 5


def test_edge_hash():
    v1 = daglet.Vertex('v1')
    assert isinstance(hash(v1.edge()), int)
    assert hash(v1.edge()) == hash(v1.edge())
    assert hash(v1.edge('e1')) == hash(v1.edge('e1'))
    assert hash(v1.edge('e1')) != hash(v1.edge('e2'))


def test_edge_eq():
    assert daglet.Vertex().edge() == daglet.Vertex().edge()
    assert daglet.Vertex().edge() == daglet.Edge(parent=daglet.Vertex())
    assert daglet.Vertex().edge('e1') == daglet.Vertex().edge('e1')
    assert daglet.Vertex().edge('e1') != daglet.Vertex().edge('e2')
    assert daglet.Vertex('v1').edge('e1') == daglet.Vertex('v1').edge('e1')
    assert daglet.Vertex('v1').edge('e1') != daglet.Vertex('v2').edge('e1')
    assert daglet.Vertex().edge(extra_hash=1) == daglet.Vertex().edge(extra_hash=1)
    assert daglet.Vertex().edge(extra_hash=1) != daglet.Vertex().edge(extra_hash=2)


def test_edge_cmp():
    e1 = daglet.Vertex().edge('e1')
    e2 = daglet.Vertex().edge('e2')
    es = sorted([e1, e2])
    assert es == sorted([e2, e1])
    ea = es[0]
    eb = es[1]
    assert hash(ea) < hash(eb)
    assert ea < eb
    assert not (eb < ea)
    assert ea <= eb
    assert not (eb <= ea)
    assert ea <= ea
    assert ea == ea
    assert not (ea == eb)
    assert ea != eb
    assert not (ea != ea)
    assert eb >= ea
    assert not (ea >= eb)
    assert eb >= eb
    assert eb > ea
    assert not (ea > eb)


def test_edge_short_hash():
    h1 = daglet.Vertex().edge('e1').short_hash
    h2 = daglet.Vertex().edge('e1').short_hash
    h3 = daglet.Vertex().edge('e2').short_hash
    assert isinstance(h1, basestring)
    assert int(h1, base=16)
    assert len(h1) == 8
    assert h1 == h2
    assert h1 != h3


def test_edge_clone():
    assert daglet.Vertex().edge('e1').clone(label='e2').label == 'e2'


def test_vertex_parents():
    v1 = daglet.Vertex()
    v2 = daglet.Vertex()
    e1 = v1.edge('e1')
    e2 = v1.edge('e2')
    e3 = v2.edge('e3')
    assert v1.parents == []
    assert e1.vertex().parents == [e1]
    assert e2.vertex().parents == [e2]
    assert daglet.Vertex(parents=[e1, e3]).parents == sorted([e1, e3])
    assert daglet.Vertex(parents=[e3, e1]).parents == sorted([e1, e3])


def test_vertex_label():
    assert daglet.Vertex().label == None
    assert daglet.Vertex('v1').label == 'v1'


def test_vertex_hash():
    assert isinstance(hash(daglet.Vertex('v1')), int)
    assert hash(daglet.Vertex('v1')) == hash(daglet.Vertex('v1'))
    assert hash(daglet.Vertex('v1')) != hash(daglet.Vertex('v2'))


def test_vertex_extra_hash():
    assert daglet.Vertex().extra_hash == None
    assert daglet.Vertex(extra_hash=5).extra_hash == 5


def test_vertex_eq():
    assert daglet.Vertex() == daglet.Vertex()
    assert daglet.Vertex('v1') == daglet.Vertex('v1')
    assert daglet.Vertex('v1') != daglet.Vertex('v2')
    assert not (daglet.Vertex('v1') != daglet.Vertex('v1'))
    assert daglet.Vertex(extra_hash=1) == daglet.Vertex(extra_hash=1)
    assert daglet.Vertex(extra_hash=1) != daglet.Vertex(extra_hash=2)
    assert daglet.Vertex().edge().vertex() == daglet.Vertex().edge().vertex()
    assert daglet.Vertex().edge().vertex() != daglet.Vertex()
    assert daglet.Vertex().edge().vertex('v1') == daglet.Vertex().edge().vertex('v1')
    assert daglet.Vertex().edge().vertex('v1') != daglet.Vertex().edge().vertex('v2')
    assert daglet.Vertex().edge('e1').vertex() == daglet.Vertex().edge('e1').vertex()
    assert daglet.Vertex().edge('e1').vertex() != daglet.Vertex().edge('e2').vertex()

    v1 = daglet.Vertex('v1')
    v2 = daglet.Vertex('v2')
    assert daglet.Vertex(parents=[v1.edge(), v2.edge()]) == daglet.Vertex(parents=[v1.edge(), v2.edge()])
    assert daglet.Vertex(parents=[v1.edge(), v2.edge()]) == daglet.Vertex(parents=[v2.edge(), v1.edge()])


def test_vertex_cmp():
    v1 = daglet.Vertex('v1')
    v2 = daglet.Vertex('v2')
    vs = sorted([v1, v2])
    assert vs == sorted([v2, v1])
    va = vs[0]
    vb = vs[1]
    assert hash(va) < hash(vb)
    assert va < vb
    assert not (vb < va)
    assert va <= vb
    assert not (vb <= va)
    assert va <= va
    assert va == va
    assert not (va == vb)
    assert va != vb
    assert not (va != va)
    assert vb >= va
    assert not (va >= vb)
    assert vb >= vb
    assert vb > va
    assert not (va > vb)


def test_vertex_short_hash():
    h1 = daglet.Vertex('v1').short_hash
    h2 = daglet.Vertex('v1').short_hash
    h3 = daglet.Vertex('v2').short_hash
    assert isinstance(h1, basestring)
    assert int(h1, base=16)
    assert len(h1) == 8
    assert h1 == h2
    assert h1 != h3


def test_edge_get_repr():
    v1 = daglet.Vertex('v1')
    e1 = v1.edge('e1')
    v2 = e1.vertex('v2')
    e2 = v2.edge('e2')
    assert e1.get_repr() == repr(e1)
    assert repr(e1) == "daglet.Edge({!r}, ...) <{}>".format('e1', e1.short_hash)
    assert repr(e2) == "daglet.Edge({!r}, ...) <{}>".format('e2', e2.short_hash)
    assert e2.get_repr(include_hash=False) == "daglet.Edge({!r}, ...)".format('e2')
    assert v1.edge().get_repr(include_hash=False) == 'daglet.Edge(...)'
    assert v1.edge(0).get_repr(include_hash=False) == "daglet.Edge(0, ...)"


def test_vertex_get_repr():
    v1 = daglet.Vertex('v1')
    e1 = daglet.Edge('e1', v1)
    v2 = e1.vertex('v2')
    assert v1.get_repr() == repr(v1)
    assert repr(v1) == "daglet.Vertex({!r}) <{}>".format('v1', v1.short_hash)
    assert repr(v2) == "daglet.Vertex({!r}, ...) <{}>".format('v2', v2.short_hash)
    assert v2.get_repr(include_hash=False) == "daglet.Vertex({!r}, ...)".format('v2')
    assert daglet.Vertex().get_repr(include_hash=False) == 'daglet.Vertex()'
    assert daglet.Vertex(extra_hash=5).get_repr(include_hash=False) == 'daglet.Vertex(...)'
    assert e1.vertex().get_repr(include_hash=False) == 'daglet.Vertex(...)'
    assert daglet.Vertex(0).get_repr(include_hash=False) == 'daglet.Vertex(0)'


def test_vertex_clone():
    assert daglet.Vertex().clone(label='v2').label == 'v2'


def test_vertex_transplant():
    v2 = daglet.Vertex('v2')
    assert daglet.Vertex().transplant([v2.edge()]).parents == [v2.edge()]


def test_analyze():
    v = daglet.Vertex()
    e1 = v.edge()
    v2 = e1.vertex()
    assert daglet.analyze([]) == ([], {}, {})
    assert daglet.analyze([v]) == ([v], {v: set()}, {})
    assert daglet.analyze([v2]) == ([v, v2], {v: {e1}, v2: set()}, {e1: {v2}})

    v3 = daglet.Vertex('v3')
    v4 = v3.edge(0).vertex('v4')
    v5 = v3.edge(1).vertex('v5')
    v6 = v5.edge().vertex('v6')
    v7 = v5.edge().vertex('v7')
    v8 = daglet.Vertex('v8')
    v9 = daglet.Vertex('v9', [v4.edge(), v6.edge(), v7.edge()])
    v10 = daglet.Vertex('v10', [v3.edge(2), v8.edge()])
    v11 = daglet.Vertex('v11')
    vertices, vertex_child_map, edge_child_map = daglet.analyze([v4, v9, v10, v11])
    assert vertices == [v11, v8, v3, v10, v4, v5, v7, v6, v9]
    assert vertex_child_map == {
        v3: {v3.edge(0), v3.edge(1), v3.edge(2)},
        v4: {v4.edge()},
        v5: {v5.edge()},
        v6: {v6.edge()},
        v7: {v7.edge()},
        v8: {v8.edge()},
        v9: set(),
        v10: set(),
        v11: set(),
    }
    assert edge_child_map == {
        v3.edge(0): {v4},
        v3.edge(1): {v5},
        v3.edge(2): {v10},
        v4.edge(): {v9},
        v5.edge(): {v6, v7},
        v6.edge(): {v9},
        v7.edge(): {v9},
        v8.edge(): {v10},
    }


def test_composition():
    class Stream(object):
        def __init__(self, parent_node, upstream_id):
            self.parent_node = parent_node
            self.upstream_id = upstream_id

        def __filter(self, name, args=[]):
            """Create single-output filter."""
            return Filter([self], name, args).stream(0)

        def hflip(self):
            return self.__filter('hflip')

        def vflip(self):
            return self.__filter('vflip')

        def overlay(self, overlay_stream):
            return Filter([self, overlay_stream], 'overlay').stream(0)

        def split(self):
            return Filter([self], 'split')

        def output(self, filename):
            return Output(self, filename)

    class Node(object):
        def __init__(self, streams, name, args=[]):
            self.streams = streams
            self.name = name
            self.args = args

        def get_parent_nodes(self):
            return [x.parent_node for x in self.streams]

        def get_dag_vertex(self, parent_vertices):
            edges = []
            for downstream_id, stream, vertex in zip(itertools.count(), self.streams, parent_vertices):
                label = '{} -> {}'.format(stream.upstream_id, downstream_id)
                edges.append(vertex.edge(label))
            label = '{}({})'.format(self.name, ', '.join([json.dumps(x) for x in self.args]))
            return daglet.Vertex(label, edges)

    class Input(Node):
        def __init__(self, filename):
            super(Input, self).__init__([], 'input', [filename])

        def stream(self, upstream_id):
            return Stream(self, upstream_id)

    class Filter(Node):
        def stream(self, upstream_id):
            return Stream(self, upstream_id)

    class Output(Node):
        def __init__(self, stream, filename):
            super(Output, self).__init__([stream], 'output', [filename])

    def input(filename):
        return Input(filename).stream(0)

    def concat(*streams):
        return Filter(streams, 'concat').stream(0)

    def compile(outputs):
        for output in outputs:
            assert isinstance(Output, output)
        vertices = [x.dag_vertex for x in outputs]
        # TODO: get mapping
        vertices, vertex_child_map, edge_child_map = daglet.analyze(vertices)

    def get_dag(nodes):
        return daglet.map_object_graph(nodes, Node.get_parent_nodes, Node.get_dag_vertex)

    def get_node_color(node):
        if isinstance(node, Input):
            color = '#99cc00'
        elif isinstance(node, Output):
            color = '#99ccff'
        elif isinstance(node, Filter):
            color = '#ffcc00'
        else:
            color = None
        return color

    identity = input('in.mp4').output('out.mp4')

    flipped = input('in.mp4').hflip().output('out.mp4')
    overlay = input('overlay.png').hflip()
    overlayed = input('in.mp4').overlay(overlay)
    out = (concat(
            overlayed.split().stream(0).vflip(),
            overlayed.split().stream(1),
        )
        .output('out.mp4')
    )
    daglet.view(daglet.convert_obj_graph_to_dag([out], Node.get_parent_nodes))
    nodes, _, node_vertex_map = get_dag([out])

    vertex_node_map = {v: k for k, v in node_vertex_map.items()}
    color_map = {node_vertex_map[n]: get_node_color(n) for n in nodes}
    def get_vertex_color(vertex):
        return get_node_color(vertex_node_map[vertex])
    #daglet.view(node_vertex_map.values(), color_func=color_map.get)
    daglet.view(node_vertex_map.values(), color_func=get_vertex_color)

    #daglet.view(daglet.convert_obj_graph_to_dag(vertex_node_map.values(), daglet._get_vertex_or_edge_parents))
