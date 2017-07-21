from __future__ import unicode_literals

import daglet
import itertools
import json
import subprocess


def test_vertex_parents():
    v1 = daglet.Vertex()
    v2 = daglet.Vertex()
    v3 = v1.vertex('v3')
    v4 = v1.vertex('v4')
    v5 = v2.vertex('v5')
    assert v1.parents == []
    assert v3.vertex().parents == [v3]
    assert v4.vertex().parents == [v4]
    assert daglet.Vertex(parents=[v3, v5]).parents == sorted([v3, v5])
    assert daglet.Vertex(parents=[v5, v3]).parents == sorted([v3, v5])


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
    assert daglet.Vertex().vertex() == daglet.Vertex().vertex()
    assert daglet.Vertex().vertex() != daglet.Vertex()
    assert daglet.Vertex().vertex('v1') == daglet.Vertex().vertex('v1')
    assert daglet.Vertex().vertex('v1') != daglet.Vertex().vertex('v2')
    assert daglet.Vertex('v1').vertex() == daglet.Vertex('v1').vertex()
    assert daglet.Vertex('v1').vertex() != daglet.Vertex('v2').vertex()

    v1 = daglet.Vertex('v1')
    v2 = daglet.Vertex('v2')
    assert daglet.Vertex(parents=[v1, v2]) == daglet.Vertex(parents=[v1, v2])
    assert daglet.Vertex(parents=[v1, v2]) == daglet.Vertex(parents=[v2, v1])


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


def test_vertex_get_repr():
    v1 = daglet.Vertex('v1')
    v2 = daglet.Vertex('v2', parents=[v1])
    v3 = v2.vertex('v3')
    assert v1.get_repr() == repr(v1)
    assert repr(v1) == "daglet.Vertex({!r}) <{}>".format('v1', v1.short_hash)
    assert repr(v3) == "daglet.Vertex({!r}, ...) <{}>".format('v3', v3.short_hash)
    assert v3.get_repr(include_hash=False) == "daglet.Vertex({!r}, ...)".format('v3')
    assert daglet.Vertex().get_repr(include_hash=False) == 'daglet.Vertex()'
    assert daglet.Vertex(extra_hash=5).get_repr(include_hash=False) == 'daglet.Vertex(...)'
    assert v2.vertex().get_repr(include_hash=False) == 'daglet.Vertex(...)'
    assert daglet.Vertex(0).get_repr(include_hash=False) == 'daglet.Vertex(0)'


def test_vertex_clone():
    assert daglet.Vertex().clone(label='v2').label == 'v2'


def test_vertex_transplant():
    v2 = daglet.Vertex('v2')
    assert daglet.Vertex().transplant([v2]).parents == [v2]


def test_analyze():
    v1 = daglet.Vertex()
    v2 = v1.vertex()
    assert daglet.analyze([]) == ([], {})
    assert daglet.analyze([v1]) == ([v1], {v1: set()})
    assert daglet.analyze([v2]) == ([v1, v2], {v1: {v2}, v2: set()})

    v3 = daglet.Vertex('v3')
    v4 = v3.vertex('v4')
    v5 = v3.vertex('v5')
    v6 = v5.vertex('v6')
    v7 = v5.vertex('v7')
    v8 = daglet.Vertex('v8')
    v9 = daglet.Vertex('v9', [v4, v6, v7])
    v10 = daglet.Vertex('v10', [v3, v8])
    v11 = daglet.Vertex('v11')
    vertices, child_map = daglet.analyze([v4, v9, v10, v11])
    assert vertices == [v11, v3, v8, v10, v5, v7, v6, v4, v9]
    assert child_map == {
        v3: {v4, v5, v10},
        v4: {v9},
        v5: {v6, v7},
        v6: {v9},
        v7: {v9},
        v8: {v10},
        v9: set(),
        v10: set(),
        v11: set(),
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
                # TODO: make a vertex for each output + each input rather than combining into one vertex.
                edges.append(vertex.vertex(str(stream.upstream_id)).vertex(str(downstream_id)))
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
        vertices, child_map = daglet.analyze(vertices)

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

    input('in.mp4').output('out.mp4')
    input('in.mp4').hflip().output('out.mp4')

    # Layer 0: AST
    # Layer 1: dag with extra vertices
    # Layer 2: dag
    overlay = input('overlay.png').hflip()
    overlayed = input('in.mp4').overlay(overlay)
    out = (concat(
            overlayed.split().stream(0).vflip(),
            overlayed.split().stream(1),
        )
        .output('out.mp4')
    )
    #daglet.view(daglet.convert_obj_graph_to_dag([out], Node.get_parent_nodes).values())

    nodes, child_map, xform1 = get_dag([out])

    color_map = {n: get_node_color(n) for n in nodes}

    edges0 = daglet.get_edges(nodes, child_map)
    edge_label_map = {}
    for edge in edges0:
        parent_node, child_node = edge
        upstream_id, downstream_id = next((s.upstream_id, i) for s, i in zip(child_node.streams, itertools.count()) if
            s.parent_node == parent_node)  # jeezus this is ugly
        edge_label_map[edge] = '{} -> {}'.format(upstream_id, downstream_id)

    for parent_vertex, child_vertex in edges1:
        parent_node, child_node = yform0

    for obj in objs:
        if isinstance(obj, Stream):
            upstream_id = obj.upstream_id
            downstream_id = child_map[obj]

    #daglet.view(xform1.values(), color_func=color_map.get, edge_label_func=get_edge_label)

    xform2 = daglet.double_squish_vertices(xform1.values())
    #yform1 = daglet.invert(xform1)
    #yform2 = daglet.invert(xform2)

    edge_label_map = {(xform1[n1], xform1[n2]): v for (n1, n2), v in edge_label_map.items()}
    edge_label_map = {(xform2[n1], xform2[n2]): v for (n1, n2), v in edge_label_map.items()}

    color_map = daglet.transform_keys(xform1, color_map)
    color_map = daglet.transform_keys(xform2, color_map)
    daglet.view(xform2.values(), color_func=color_map.get, edge_label_func=edge_label_map.get)


def test_git():
    repo_dir = '.'
    def get_parent_hashes(commit_hash):
        return (subprocess
            .check_output(['git', 'rev-list', '--parents', '-n1', commit_hash], cwd=repo_dir)
            .strip()
            .split(' ')[1:]
        )

    def get_commit_repr(commit_hash):
        return subprocess.check_output(['git', 'log', '-n1', '--pretty=short', commit_hash], cwd=repo_dir)

    dag = daglet.convert_obj_graph_to_dag(['HEAD'], get_parent_hashes, get_commit_repr).values()
    #daglet.view(dag, rankdir=None)
