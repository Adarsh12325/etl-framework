"""
graph.py – Dependency graph construction, cycle detection, and topological sort.

Uses the `networkx` library to model pipelines as a Directed Acyclic Graph (DAG).
"""

import logging
from typing import Any

import networkx as nx

logger = logging.getLogger(__name__)


def build_graph(pipelines: list[dict]) -> nx.DiGraph:
    """
    Build a directed graph from pipeline definitions.

    Each pipeline is a node.  An edge A → B means "A must run before B"
    (i.e., B depends on A).
    """
    G = nx.DiGraph()

    # Add every pipeline as a node
    for p in pipelines:
        G.add_node(p["pipeline_name"])

    # Add edges for each dependency relationship
    for p in pipelines:
        name = p["pipeline_name"]
        deps = p.get("dependencies") or []
        for dep in deps:
            if dep:                       # skip empty strings
                G.add_edge(dep, name)     # dep must finish before name

    return G


def validate_and_sort(G: nx.DiGraph) -> list[str]:
    """
    Validate the graph for cycles and return a topological execution order.

    Raises:
        ValueError: if a cycle is detected, with a message listing the cycle.

    Returns:
        list[str]: pipeline names in safe execution order.
    """
    try:
        order = list(nx.topological_sort(G))
        logger.info(f"[GRAPH] Execution order: {order}")
        return order
    except nx.NetworkXUnfeasible:
        # Find the actual cycle to report it
        cycles = list(nx.simple_cycles(G))
        cycle_str = " → ".join(cycles[0] + [cycles[0][0]]) if cycles else "unknown"
        raise ValueError(
            f"Error: Cycle detected in dependency graph: {cycle_str}"
        )