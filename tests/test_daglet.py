# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from builtins import object
from builtins import range
from daglet._utils import get_hash_int, get_hash
from functools import reduce
from past.builtins import basestring
from textwrap import dedent
import copy
import daglet
import operator
import subprocess


def test__get_hash():
    assert get_hash(None) == '6adf97f83acf6453d4a6a4b1070f3754'
    assert get_hash(5) == 'e4da3b7fbbce2345d7772b0674a318d5'
    assert get_hash({'a': 'b'}) == '31ee3af152948dc06066ec1a7a4c5f31'


def test__vertex_parents():
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


def test__vertex_label():
    assert daglet.Vertex().label == None
    assert daglet.Vertex('v1').label == 'v1'


def test__vertex_hash():
    v1 = daglet.Vertex('v1')
    v2 = daglet.Vertex('v2', [v1])
    assert isinstance(hash(v1), int)
    assert hash(v1) == 352423289548818779
    assert hash(v2) == 5230371954595182985
    assert hash(v1) == hash(daglet.Vertex('v1'))
    assert hash(v2) != hash(v1)
    assert hash(v1) != hash(daglet.Vertex('v3'))


def test__vertex_extra_hash():
    assert daglet.Vertex().extra_hash == None
    assert daglet.Vertex(extra_hash=5).extra_hash == 5


def test__vertex_eq():
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


def test__vertex_cmp():
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


def test__vertex_short_hash():
    h1 = daglet.Vertex('v1').short_hash
    h2 = daglet.Vertex('v1').short_hash
    h3 = daglet.Vertex('v2').short_hash
    assert isinstance(h1, basestring)
    assert int(h1, base=16)
    assert len(h1) == 8
    assert h1 == h2
    assert h1 != h3


def test__vertex_get_repr():
    v1 = daglet.Vertex('v1')
    v2 = daglet.Vertex('v2', parents=[v1])
    v3 = v2.vertex('v3')
    assert v1.get_repr() == repr(v1)
    assert repr(v1) == "daglet.Vertex('{}') <{}>".format('v1', v1.short_hash)
    assert repr(v3) == "daglet.Vertex('{}', ...) <{}>".format('v3', v3.short_hash)
    assert v3.get_repr(include_hash=False) == "daglet.Vertex('{}', ...)".format('v3')
    assert daglet.Vertex().get_repr(include_hash=False) == 'daglet.Vertex()'
    assert daglet.Vertex(extra_hash=5).get_repr(include_hash=False) == 'daglet.Vertex(...)'
    assert v2.vertex().get_repr(include_hash=False) == 'daglet.Vertex(...)'
    assert daglet.Vertex(0).get_repr(include_hash=False) == 'daglet.Vertex(0)'


def test__vertex_clone():
    assert daglet.Vertex().clone(label='v2').label == 'v2'


def test__vertex_transplant():
    v2 = daglet.Vertex('v2')
    assert daglet.Vertex().transplant([v2]).parents == [v2]


def test__toposort():
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
    assert sorted_vertices == [v11, v8, v3, v10, v5, v7, v4, v6, v9]


def test__transform():
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

    debug = False
    if debug:
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
        daglet.view([v4, v9, v10, v11], get_parents, vertex_label_func=vertex_labels.get,
            vertex_color_func=vertex_colors.get)


def test__example__git():
    REPO_DIR = '.'

    def get_parent_hashes(commit_hash):
        return (subprocess
            .check_output(['git', 'rev-list', '--parents', '-n1', commit_hash], cwd=REPO_DIR)
            .decode()
            .strip()
            .split(' ')[1:]
        )

    def get_commit_message(commit_hash):
        return subprocess.check_output(['git', 'log', '-n1', '--pretty=short', commit_hash], cwd=REPO_DIR)

    class Commit(object):
        def __init__(self, commit_hash, parents):
            self.commit_hash = commit_hash
            self.parents = parents
            self.log = get_commit_message(commit_hash)

    vertex_map = daglet.transform_vertices(['HEAD'], get_parent_hashes, Commit)
    assert 'HEAD' in vertex_map
    assert all(isinstance(x, basestring) for x in vertex_map.keys())
    assert all(isinstance(x, Commit) for x in vertex_map.values())

    debug = False
    if debug:
        daglet.view(
            vertex_map.values(),
            rankdir=None,
            parent_func=lambda x: x.parents,
            vertex_label_func=lambda x: x.log,
            vertex_color_func=lambda x: 'lightblue',
        )


def test__example__vdom():
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
