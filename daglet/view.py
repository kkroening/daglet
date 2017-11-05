from __future__ import unicode_literals

from builtins import str
import daglet
import tempfile


def __import_graphviz():
    try:
        import graphviz
    except ImportError:
        raise ImportError('failed to import graphviz; please make sure graphviz is installed (e.g. `pip install '
            'graphviz`)')
    return graphviz


def __make_graph(objs, parent_func, rankdir, vertex_color_func, vertex_label_func, edge_label_func):
    graphviz = __import_graphviz()
    graph = graphviz.Digraph()
    graph.attr(rankdir=rankdir)

    sorted_objs = daglet.toposort(objs, parent_func)
    for child in sorted_objs:
        id = str(hash(child))
        label = vertex_label_func(child)
        color = vertex_color_func(child) if vertex_color_func is not None else None
        graph.node(id, label, shape='box', style='filled', fillcolor=color)

        for parent in parent_func(child):
            kwargs = {}
            edge_label = edge_label_func((parent, child))
            if edge_label is not None:
                kwargs['label'] = edge_label
            upstream_obj_id = str(hash(parent))
            downstream_obj_id = str(hash(child))
            graph.edge(upstream_obj_id, downstream_obj_id, **kwargs)

    return graph


def render(objs, parent_func, filename=None, rankdir='LR', vertex_color_func={}.get, vertex_label_func={}.get,
        edge_label_func={}.get):
    graph = __make_graph(objs, parent_func, rankdir, vertex_color_func, vertex_label_func, edge_label_func)
    if filename is None:
        filename = tempfile.mktemp()
    graph.render(filename)
    return filename


def view(objs, parent_func, filename=None, rankdir='LR', vertex_color_func={}.get, vertex_label_func={}.get,
        edge_label_func={}.get):
    graph = __make_graph(objs, parent_func, rankdir, vertex_color_func, vertex_label_func, edge_label_func)
    if filename is None:
        filename = tempfile.mktemp()
    graph.view(filename)
    return filename
