from __future__ import unicode_literals

from builtins import str
import daglet
import tempfile


def view(objs, filename=None, rankdir='LR', parent_func=daglet.default_get_parents, vertex_color_func={}.get,
        vertex_label_func={}.get, edge_label_func={}.get):
    try:
        import graphviz
    except ImportError:
        raise ImportError('failed to import graphviz; please make sure graphviz is installed (e.g. `pip install '
            'graphviz`)')

    if filename is None:
        filename = tempfile.mktemp()

    sorted_objs = daglet.toposort(objs, parent_func)
    graph = graphviz.Digraph()
    graph.attr(rankdir=rankdir)

    for child in sorted_objs:
        label = vertex_label_func(child)
        color = vertex_color_func(child) if vertex_color_func is not None else None
        graph.node(str(hash(child)), label, shape='box', style='filled', fillcolor=color)

        for parent in parent_func(child):
            kwargs = {}
            edge_label = edge_label_func((parent, child))
            if edge_label is not None:
                kwargs['label'] = edge_label
            upstream_obj_id = str(hash(parent))
            downstream_obj_id = str(hash(child))
            graph.edge(upstream_obj_id, downstream_obj_id, **kwargs)

    graph.view(filename)
