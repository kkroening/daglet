# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from daglet._utils import get_hash_int
from textwrap import dedent
import copy
import daglet
import operator
import os
import pytest
import subprocess
import yaml


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


def test_toposort():
    get_parents = lambda x: x.parents
    v1 = daglet.Vertex()
    v2 = v1.vertex()
    assert daglet.toposort([], get_parents) == []
    assert daglet.toposort([v1], get_parents) == [v1]
    assert daglet.toposort([v2], get_parents) == [v1, v2]

    v3 = daglet.Vertex('v3')
    v4 = v3.vertex('v4')
    v5 = v3.vertex('v5')
    v6 = v5.vertex('v6')
    v7 = v5.vertex('v7')
    v8 = daglet.Vertex('v8')
    v9 = daglet.Vertex('v9', [v4, v6, v7])
    v10 = daglet.Vertex('v10', [v3, v8])
    v11 = daglet.Vertex('v11')
    sorted_vertices = daglet.toposort([v4, v9, v10, v11], get_parents)
    assert sorted_vertices == [v11, v3, v8, v10, v5, v7, v6, v4, v9]


def test_transform():
    get_parents = lambda x: x.parents
    v1 = daglet.Vertex()
    v2 = v1.vertex()
    assert daglet.transform([], get_parents) == ({}, {})
    assert daglet.transform([v1], get_parents) == ({v1: None}, {})
    assert daglet.transform([v2], get_parents) == ({v1: None, v2: None}, {(v1, v2): None})
    vertex_dummy_func = lambda obj, parent_values: (obj, parent_values)
    edge_dummy_func = lambda parent, child, parent_value: 'test'
    assert daglet.transform([v2], get_parents, vertex_dummy_func, edge_dummy_func) == (
        {
            v1: (v1, []),
            v2: (v2, ['test'])
        },
        {
            (v1, v2): 'test'
        }
    )

    v3 = daglet.Vertex('v3')
    v4 = v3.vertex('v4')
    v5 = v3.vertex('v5')
    v6 = v5.vertex('v6')
    v7 = v5.vertex('v7')
    v8 = daglet.Vertex('v8')
    v9 = daglet.Vertex('v9', [v4, v6, v7])
    v10 = daglet.Vertex('v10', [v3, v8])
    v11 = daglet.Vertex('v11')
    vertex_rank_func = lambda obj, parent_ranks: max(parent_ranks) + 1 if len(parent_ranks) else 0
    vertex_map, edge_map = daglet.transform([v4, v9, v10, v11], get_parents, vertex_rank_func)
    assert vertex_map == {
        v3: 0,
        v4: 1,
        v5: 1,
        v6: 2,
        v7: 2,
        v8: 0,
        v9: 3,
        v10: 1,
        v11: 0,
    }
    assert edge_map == {
        (v3, v4): 0,
        (v3, v5): 0,
        (v3, v10): 0,
        (v4, v9): 1,
        (v5, v6): 1,
        (v5, v7): 1,
        (v6, v9): 2,
        (v7, v9): 2,
        (v8, v10): 0,
    }

    vertex_labels = {
        v3: 'v3',
        v4: 'v4',
        v5: 'v5',
        v6: 'v6',
        v7: 'v7',
        v8: 'v8',
        v9: 'v9',
        v10: 'v10',
        v11: 'v11',
    }
    vertex_colors = {
        v3: 'red',
        v4: 'yellow',
        v5: 'purple',
        v6: 'purple',
        v7: 'lightblue',
        v8: 'green',
        v9: 'white',
        v11: 'orange',
    }

    #daglet.view([v4, v9, v10, v11], get_parents, vertex_label_func=vertex_labels.get,
    #    vertex_color_func=vertex_colors.get)


def test_git():
    repo_dir = '.'
    def get_parent_hashes(commit_hash):
        return (subprocess
            .check_output(['git', 'rev-list', '--parents', '-n1', commit_hash], cwd=repo_dir)
            .strip()
            .split(' ')[1:]
        )

    class Commit(object):
        def __init__(self, commit_hash, parents):
            self.commit_hash = commit_hash
            self.parents = parents
            self.log = subprocess.check_output(['git', 'log', '-n1', '--pretty=short', commit_hash], cwd=repo_dir)

        def get_parents(self):
            return self.parents

        def get_log(self):
            return self.log

    vertex_map = daglet.transform_vertices(['HEAD'], get_parent_hashes, Commit)
    #daglet.view(vertex_map.values(), rankdir=None, parent_func=Commit.get_parents, vertex_label_func=Commit.get_log,
    #    vertex_color_func=lambda x: 'lightblue')



def load_formulas(template_dir):
    filenames = [x for x in os.listdir(template_dir) if x.endswith('.reppltmpl')]
    return {f: yaml.load(file(os.path.join(template_dir, f))) for f in filenames}

def get_input_tags(formula):
    return [x['tag'] for x in formula['inputs'].values()]

def get_output_tags(formula):
    return [x['tag'] for x in formula['outputs'].values()]

def get_tag_map(formula_map):
    """Generate mapping from tag name to source formula id."""
    tag_map = {}
    for formula_id, formula in formula_map.items():
        tag_map.update({tag: formula_id for tag in get_output_tags(formula)})
    return tag_map


def get_formula_parents(formula_map, tag_map, formula_id):
    parents = []
    if formula_id in formula_map:
        input_tags = get_input_tags(formula_map[formula_id])
        for tag in input_tags:
            if tag in tag_map:
                formula_id = tag_map[tag]
            else:
                formula_id = tag
            parents.append(formula_id)
    return parents


class Formula(object):
    def __init__(self, formula_id, parents):
        self.formula_id = formula_id
        self.parents = parents

    def get_parents(self):
        return self.parents

    def get_label(self):
        suffix = '.reppltmpl'
        if self.formula_id.endswith(suffix):
            return self.formula_id[:-len(suffix)]
        else:
            return self.formula_id


def get_downstream_formula_ids(vertex_map, child_map, formula_id):
    formula = vertex_map[formula_id]
    return [x.formula_id for x in child_map[formula]]

def find_input_tag(formula, tag_name):
    return tag_name in get_input_tags(formula)

def get_unconnected_outputs(vertex_map, child_map, formula_map, formula_id):
    downstream_formula_ids = get_downstream_formula_ids(vertex_map, child_map, formula_id)
    output_tags = get_output_tags(formula_map[formula_id])
    unconnected_tags = []
    for tag in output_tags:
        if not any(find_input_tag(formula_map[f], tag) for f in downstream_formula_ids):
            unconnected_tags.append(tag)
    return unconnected_tags

def add_output_vertices(vertex_map, formula_map):
    child_map = daglet.get_child_map(vertex_map.values(), Formula.get_parents)
    output_vertices = []
    for formula_id in formula_map.keys():
        tags = get_unconnected_outputs(vertex_map, child_map, formula_map, formula_id)
        parent_formula = vertex_map[formula_id]
        for tag in tags:
            output_vertex = Formula(tag, [parent_formula])
            output_vertices.append(output_vertex)
    return output_vertices


class RunRecord(object):
    def __init__(self, formula, parents):
        self.formula = formula
        self.parents = parents

    def get_parents(self):
        return self.parents

    def get_label(self):
        return self.formula.get_label()


def run_formula(formula, parent_run_records):
    print 'Running formula {}'.format(formula.formula_id)
    return RunRecord(formula, parent_run_records)

def get_edge_label(formula_map, parent_formula, child_formula, parent_value):
    if parent_formula.formula_id in formula_map and child_formula.formula_id in formula_map:
        parent_formula_dict = formula_map[parent_formula.formula_id]
        child_formula_dict = formula_map[child_formula.formula_id]
        output_tags = set(get_output_tags(parent_formula_dict))
        input_tags = set(get_input_tags(child_formula_dict))
        tags = output_tags.intersection(input_tags)
        label = ', '.join(tags)
    else:
        label = None
    return label


@pytest.mark.skip('')
def test_omnibus():
    # Layer 1: yaml as dictionaries.
    formula_map = load_formulas('/Users/karlk/src/notmine/timeless/omnibus')
    tag_map = get_tag_map(formula_map)
    get_parents = lambda formula_id: get_formula_parents(formula_map, tag_map, formula_id)
    label_func = lambda formula_id: formula_id
    daglet.view(formula_map.keys(), parent_func=get_parents, vertex_label_func=label_func)

    # Layer 2a: Formula objects.
    make_formula = lambda formula_id, parents: Formula(formula_id, parents)
    vertex_map = daglet.transform_vertices(formula_map.keys(), get_parents, make_formula)
    vertex_color_map = {v: '#ffcc00' if f in formula_map else '#99cc00' for f, v in vertex_map.items()}
    get_edge_label_func = lambda parent, child, value: get_edge_label(formula_map, parent, child, value)
    edge_label_map = daglet.transform_edges(vertex_map.values(), Formula.get_parents, get_edge_label_func)
    daglet.view(vertex_map.values(), parent_func=Formula.get_parents, vertex_color_func=vertex_color_map.get,
        vertex_label_func=Formula.get_label, edge_label_func=edge_label_map.get)

    # Layer 2b: Formula objects with outputs.
    output_vertices = add_output_vertices(vertex_map, formula_map)
    vertex_color_map.update({x: '#99ccff' for x in output_vertices})
    daglet.view(output_vertices, parent_func=Formula.get_parents, vertex_color_func=vertex_color_map.get,
        vertex_label_func=Formula.get_label, edge_label_func=edge_label_map.get)

    # Layer 3: RunRecord objects.
    vertex_map2 = daglet.transform_vertices(output_vertices, Formula.get_parents, run_formula)
    edge_map2 = daglet.get_edge_map(vertex_map2, Formula.get_parents, RunRecord.get_parents)
    vertex_color_map2 = daglet.apply_mapping(vertex_map2, vertex_color_map)
    edge_label_map2 = daglet.apply_mapping(edge_map2, edge_label_map)
    daglet.view(vertex_map2.values(), parent_func=RunRecord.get_parents, vertex_label_func=RunRecord.get_label,
        vertex_color_func=vertex_color_map2.get, edge_label_func=edge_label_map2.get)


@pytest.mark.skip('')
def test_omnibus_simple():
    # Layer 1: yaml as dictionaries.
    formula_map = load_formulas('/Users/karlk/src/notmine/timeless/omnibus')
    tag_map = get_tag_map(formula_map)
    get_parents = lambda formula_id: get_formula_parents(formula_map, tag_map, formula_id)

    # Layer 2a: Formula objects.
    make_formula = lambda formula_id, parents: Formula(formula_id, parents)
    vertex_map = daglet.transform_vertices(formula_map.keys(), get_parents, make_formula)

    # Layer 2b: Formula objects with outputs.
    output_vertices = add_output_vertices(vertex_map, formula_map)

    # Layer 3: RunRecord objects.
    run_records = daglet.transform_vertices(output_vertices, Formula.get_parents, run_formula)


def test_dom():
    class TextBuffer(object):
        def __init__(self, row_count, col_count):
            self.rows = [' ' * col_count] * row_count

        @property
        def row_count(self):
            return len(self.rows)

        @property
        def col_count(self):
            return len(self.rows[0])

        @property
        def text(self):
            return '\n'.join(self.rows)

        def draw_text(self, row, col, text):
            assert len(text.split('\n')) == 1  # FIXME
            if 0 <= row < len(self.rows):
                start = self.rows[row][:col]
                end = self.rows[row][col+len(text):]
                self.rows[row] = '{}{}{}'.format(start, text, end)[:self.col_count]

        def draw_border(self, row, col, row_count, col_count):
            V_CHAR = u'\u2502'
            TL_CHAR = u'\u250c'
            TR_CHAR = u'\u2510'
            H_CHAR = u'\u2500'
            BL_CHAR = u'\u2514'
            BR_CHAR = u'\u2518'
            self.draw_text(row, col, u'{}{}{}'.format(TL_CHAR, H_CHAR * (col_count - 2), TR_CHAR))
            for row2 in range(row + 1, row + row_count - 1):
                self.draw_text(row2, col, V_CHAR)
                self.draw_text(row2, col + col_count - 1, V_CHAR)
            self.draw_text(row + row_count - 1, col, u'{}{}{}'.format(BL_CHAR, H_CHAR * (col_count - 2), BR_CHAR))

        def draw_buf(self, row, col, buf):
            for row2 in range(buf.row_count):
                self.draw_text(row + row2, col, buf.rows[row2])

    class Component(object):
        def __init__(self, props={}, children=[]):
            self.props = props
            self.children = children

        def __hash__(self):
            child_hashes = [hash(x) for x in self.children]
            return get_hash_int([self.props, self.__class__.__name__, child_hashes])

        def __eq__(self, other):
            return hash(self) == hash(other)

        def expand(self):
            return self._expand()

        def collapse(self, children):
            return self._collapse(children)

        def textify(self, child_bufs):
            return self._textify(child_bufs)

        def _expand(self):
            return self.children

        def _collapse(self, children):
            collapsed = copy.copy(self)
            collapsed.children = children
            return collapsed

        def _textify(self, child_bufs):
            raise NotImplementedError()

    class Div(Component):
        def __init__(self, props, children):
            super(Div, self).__init__(props, children)

        def _textify(self, child_bufs):
            child_bufs = child_bufs or []
            row_count = reduce(operator.add, [x.row_count for x in child_bufs]) + 2
            col_count = max([x.col_count for x in child_bufs]) + 4
            buf = TextBuffer(row_count, col_count)
            row = 1
            for child_buf in child_bufs:
                buf.draw_buf(row, 2, child_buf)
                row += child_buf.row_count
            buf.draw_border(0, 0, row_count, col_count)
            return buf

    class Text(Component):
        def __init__(self, text):
            super(Text, self).__init__({'text': text})

        def _textify(self, child_bufs):
            buf = TextBuffer(1, len(self.props['text']))
            buf.draw_text(0, 0, self.props['text'])
            return buf

    class CompositeComponent(Component):
        def render(self):
            raise NotImplementedError()

        def _expand(self):
            return [self.render()]

        def _collapse(self, children):
            assert len(children) == 1
            return children[0]

    subpage_render_count = [0]

    class SubPage(CompositeComponent):
        def render(self):
            subpage_render_count[0] += 1
            out = Div({}, [
                Text('# sub page #'),
            ])
            return out

    class MainPage(CompositeComponent):
        def __init__(self, text):
            super(MainPage, self).__init__({'text': text})

        def render(self):
            return Div({}, [
                Div({}, [
                    Text('# main page #'),
                    Text('sub item'),
                    Text(self.props['text']),
                ]),
                SubPage(),
                Text('sub sub item'),
            ])

    assert subpage_render_count[0] == 0

    # Create initial vdom.
    root = MainPage('some text')
    vdom = daglet.transform_vertices([root], Component.expand, Component.collapse)
    # FIXME: combine `toposort` and `transform` so that the parent_func only gets hit once; expect only one render.
    assert subpage_render_count[0] == 2

    # Turn vdom into text.
    rendered_root = vdom[root]
    buf_map = daglet.transform_vertices([rendered_root], lambda x: x.children, Component.textify)
    buf = buf_map[rendered_root]
    assert buf.text == dedent("""\
        ┌───────────────────┐
        │ ┌───────────────┐ │
        │ │ # main page # │ │
        │ │ sub item      │ │
        │ │ some text     │ │
        │ └───────────────┘ │
        │ ┌──────────────┐  │
        │ │ # sub page # │  │
        │ └──────────────┘  │
        │ sub sub item      │
        └───────────────────┘"""
    )

    # Create new vdom incrementally.
    root2 = MainPage('some other text')
    vdom2 = daglet.transform_vertices([root2], Component.expand, Component.collapse, vertex_map=vdom)
    assert subpage_render_count[0] == 2

    # Turn vdom into text again, incrementally.  Only redraw changed portions.
    rendered_root2 = vdom2[root2]
    buf_map2 = daglet.transform_vertices([rendered_root2], lambda x: x.children, Component.textify, vertex_map=buf_map)
    buf2 = buf_map2[rendered_root2]
    assert buf2.text == dedent("""\
        ┌─────────────────────┐
        │ ┌─────────────────┐ │
        │ │ # main page #   │ │
        │ │ sub item        │ │
        │ │ some other text │ │
        │ └─────────────────┘ │
        │ ┌──────────────┐    │
        │ │ # sub page # │    │
        │ └──────────────┘    │
        │ sub sub item        │
        └─────────────────────┘"""
    )


''''
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
'''
