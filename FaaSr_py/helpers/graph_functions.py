import json
import logging
import sys
from collections import defaultdict
from pathlib import Path

from jsonschema import validate
from jsonschema.exceptions import ValidationError

logger = logging.getLogger(__name__)


def validate_json(payload):
    """
    Verifies JSON payload is compliant with the FaaSr schema

    Arguments:
        payload: FaaSr payload to validate
    """
    if isinstance(payload, str):
        payload = json.loads(payload)

    schema_path = Path(__file__).parent.parent / "FaaSr.schema.json"
    if not schema_path.exists():
        logger.error(f"FaaSr schema file not found at {schema_path}")
        sys.exit(1)

    # Open FaaSr schema
    with open(schema_path, "r") as f:
        schema = json.load(f)

    # Compare payload against FaaSr schema and except if they do not match
    try:
        validate(instance=payload, schema=schema)
    except ValidationError as e:
        logger.error(f"JSON not compliant with FaaSr schema: {e.message}")
        sys.exit(1)
    return True


def is_cyclic(adj_graph, curr, visited, stack):
    """
    Recursive function that if there is a cycle in a directed
    graph defined by an adjacency list

    Arguments:
        adj_graph: adjacency list for graph (dict)
        curr: current node
        visited: set of visited nodes (set)
        stack: list of nodes in recursion call stack (list)

    Returns:
        bool: True if cycle exists, False otherwise
    """
    # if the current node is in the recursion call
    # stack then there must be a cycle in the graph
    if curr in stack:
        return True

    # add current node to recursion call stack and visited set
    visited.add(curr)
    stack.append(curr)

    # check each successor for cycles, recursively calling is_cyclic()
    for child in adj_graph[curr]:
        if child not in visited and is_cyclic(adj_graph, child, visited, stack):
            logger.error(f"Function loop found from node {curr} to {child}")
            sys.exit(1)
        elif child in stack:
            logger.error(f"Function loop found from node {curr} to {child}")
            sys.exit(1)

    # no more successors to visit for this branch and no cycles found
    # remove current node from recursion call stack
    stack.pop()
    return False


def build_adjacency_graph(payload):
    """
    This function builds an adjacency list for the FaaSr workflow graph and determines
    the ranks of each action

    Arguments:
        payload: FaaSr payload dict
    Returns:
        adj_graph: dict of predecessor: succesor pairs
        rank: dict of each action's rank
    """
    adj_graph = defaultdict(list)

    ranks = dict()

    # Build adjacency list from ActionList
    for func in payload["ActionList"].keys():
        invoke_next = payload["ActionList"][func]["InvokeNext"]
        if isinstance(invoke_next, str):
            invoke_next = [invoke_next]
        for child in invoke_next:

            def process_action(action):
                action_name, action_rank = extract_rank(action)
                if action_name in ranks and ranks[action_name] > 1:
                    err_msg = "Function with rank cannot have multiple predecessors"
                    logger.error(err_msg)
                    sys.exit(1)
                else:
                    adj_graph[func].append(action_name)
                    ranks[action_name] = action_rank

            if isinstance(child, dict):
                for conditional_branch in child.values():
                    for action in conditional_branch:
                        process_action(action)
            else:
                process_action(child)

    for func in adj_graph:
        if func not in ranks:
            ranks[func] = 0

    return (adj_graph, ranks)


def get_ranks(payload):
    """Returns just dict mapping functions to their rank"""
    _, rank = build_adjacency_graph(payload)
    return rank


def check_dag(faasr_payload):
    """
    This method checks for cycles, repeated function names,
    or unreachable nodes in the workflow and aborts if it finds any

    Arguments:
        payload: FaaSr payload dict
    Returns:
        predecessors: dict -- map of function predecessors
    """
    if faasr_payload["FunctionInvoke"] not in faasr_payload["ActionList"]:
        err_msg = "FunctionInvoke does not refer to a valid function"
        logger.error(err_msg)
        sys.exit(1)

    adj_graph, ranks = build_adjacency_graph(faasr_payload)

    # Initialize empty recursion call stack
    stack = []

    # Initialize empty visited set
    visited = set()

    # Find initial function in the graph
    start = False
    for func in faasr_payload["ActionList"]:
        if ranks[func] == 0:
            start = True
            # This function stores the first function with no predecessors
            # In the cases where there is multiple functions with no
            # predecessors, an unreachable state error will occur later
            first_func = func
            break

    # Ensure there is an initial action
    if start is False:
        logger.error("Function loop found: no initial action")
        sys.exit(1)

    # Check for cycles
    is_cyclic(adj_graph, first_func, visited, stack)

    # Check if all of the functions have been visited by the DFS
    # If not, then there is an unreachable state in the graph
    for func in faasr_payload["ActionList"]:
        if func.split(".")[0] not in visited:
            logger.error(f"Unreachable state found: {func}")
            sys.exit(1)

    # Initialize predecessor list
    pre = predecessors_list(adj_graph)

    # Ensure that no ranked function invokes another ranked function
    for func, p in pre.items():
        if ranks[func] > 1:
            for pre_f in p:
                if ranks[pre_f] > 1:
                    logger.error(
                        "Function with rank cannot have predecessor with rank"
                        f" - offending functions: {func}({ranks[func]}) and {pre_f}({ranks[pre_f]})"
                    )
                    sys.exit(1)

    curr_pre = pre[faasr_payload["FunctionInvoke"]]
    real_pre = []
    for p in curr_pre:
        if p in ranks and ranks[p] > 1:
            for i in range(1, ranks[p] + 1):
                real_pre.append(f"{p}.{i}")
        else:
            real_pre.append(p)
    return real_pre


def predecessors_list(adj_graph):
    """This function returns a map of action predecessor pairs

    Arguments:
        adj_graph: adjacency list for graph -- dict(function: successor)
    """
    pre = defaultdict(list)
    for func1 in adj_graph:
        for func2 in adj_graph[func1]:
            pre[func2].append(func1)
    return pre


def extract_rank(str):
    """
    Returns action name and rank of an action with rank (e.g func(7) returns (func, 7))

    Arguments:
        str: function name with rank
    Returns:
        (str, int) -- action name and rank
    """
    parts = str.split("(")
    if len(parts) != 2 or not parts[1].endswith(")"):
        return str, 1
    rank = int(parts[1][:-1])
    action_name = parts[0]
    return (action_name, rank)
