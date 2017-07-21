from __future__ import unicode_literals

import daglet
from builtins import str
import tempfile


_RIGHT_ARROW = '\u2192'


#def _get_node_color(node):
#    if isinstance(node, InputNode):
#        color = '#99cc00'
#    elif isinstance(node, OutputNode):
#        color = '#99ccff'
#    elif isinstance(node, FilterNode):
#        color = '#ffcc00'
#    else:
#        color = None
#    return color


def view(vertices, filename=None, rankdir='LR', color_func={}.get, edge_label_func={}.get):
    try:
        import graphviz
    except ImportError:
        raise ImportError('failed to import graphviz; please make sure graphviz is installed (e.g. `pip install '
            'graphviz`)')

    if filename is None:
        filename = tempfile.mktemp()

    sorted_vertices, vertex_child_map = daglet.analyze(vertices)
    graph = graphviz.Digraph()
    graph.attr(rankdir=rankdir)

    for vertex in sorted_vertices:
        #color = _get_vertex_color(vertex)
        color = color_func(vertex) if color_func is not None else None
        print vertex
        graph.node(str(hash(vertex)), vertex.label, shape='box', style='filled', fillcolor=color)
        for parent_vertex in vertex.parents:
            kwargs = {}
            edge_label = edge_label_func((parent_vertex, vertex))
            if edge_label is not None:
                kwargs['label'] = edge_label
            upstream_vertex_id = str(hash(parent_vertex))
            downstream_vertex_id = str(hash(vertex))
            graph.edge(upstream_vertex_id, downstream_vertex_id, **kwargs)

    graph.view(filename)
