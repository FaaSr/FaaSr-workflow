import ast
import json
import logging
import os
import uuid
from datetime import datetime
from pathlib import Path
from time import sleep

from FaaSr_py import Executor, FaaSrPayload, S3LogSender, global_config
from FaaSr_py.helpers.graph_functions import build_adjacency_graph

logger = logging.getLogger("FaaSr_py")


def store_pat_in_env(dictionary):
    """
    Checks if token is present in dict and stores
    in environment variable "TOKEN" if it is
    """
    for key, val in dictionary.items():
        if isinstance(val, dict):
            if store_pat_in_env(val):
                return True
        elif key.lower().endswith("token"):
            os.environ["TOKEN"] = val
            return True
    return False


def get_payload_from_env():
    """
    Get payload from env
    """
    payload_url = os.getenv("PAYLOAD_URL")
    overwritten = json.loads(os.getenv("OVERWRITTEN"))

    logger.debug(f"Payload URL: {payload_url}")
    faasr_payload = FaaSrPayload(payload_url, overwritten)

    # determine if secrets should be fetched
    # from secret store or overwritten payload
    logger.info("Fetching secrets from secret store")

    # get secrets from env
    secrets = os.getenv("SECRET_PAYLOAD")
    secrets_dict = json.loads(secrets)
    token_present = store_pat_in_env(secrets_dict)
    faasr_payload.faasr_replace_values(secrets_dict)

    if not token_present:
        logger.info("Without a GitHub PAT in your workflow, you may hit rate limits")
    return faasr_payload


def yes_or_no(msg, retry):
    """Prompts user for yes or no input"""
    answer = input(msg)
    while answer.lower() not in {"yes", "y", "no", "n"}:
        answer = input(retry)

    if answer in {"yes", "y"}:
        return True
    else:
        return False


def edit_config(selection):
    """Edits selection base on user input"""
    match (selection):
        case "SKIP_SCHEMA_VALIDATE":
            global_config.SKIP_SCHEMA_VALIDATE = True
            print("\nEnabled SKIP_SCHEMA_VALIDATE")

        case "SKIP_WF_VALIDATE":
            global_config.SKIP_WF_VALIDATE = True
            print("\nEnabled SKIP_WF_VALIDATE")

        case "SKIP_USER_FUNCTION":
            global_config.SKIP_USER_FUNCTION = True
            print("\nEnabled SKIP_USER_FUNCTION")

        case "USE_LOCAL_USER_FUNC":
            global_config.USE_LOCAL_USER_FUNC = True

            func_path = Path(input("\nEnter the path to your function: "))
            while not func_path.is_file():
                print("\nFile does not exist")
                func_path = Path(input("\nEnter the path to your function: "))

            global_config.LOCAL_FUNCTION_PATH = str(func_path)

            func_name = input(
                "\nEnter your function's name: "
            )  # to-do: check if function exists within file
            global_config.LOCAL_FUNCTION_NAME = func_name

            print(
                "\nEnter your function args as a Python dict (e.g {'key': 'val1', 'key2': 'val2'})"
            )
            while True:
                try:
                    args = input("Enter dict: ")
                    args_dict = ast.literal_eval(args)
                    if not isinstance(args_dict, dict):
                        print("Error: input must be a dict")
                        continue
                    break
                except (SyntaxError, ValueError):
                    print("Error: malformed dict")

            global_config.LOCAL_FUNC_ARGS = args_dict

            print("\nEnabled USE_LOCAL_FUNC")

        case "USE_LOCAL_FILE_SYSTEM":
            global_config.USE_LOCAL_FILE_SYSTEM = True

            print(
                "\nEnter the path to the directory you would like to use as your bucket"
            )
            dir_path = Path(input("Enter dir: "))

            while True:
                try:
                    dir_path.mkdir(exist_ok=True, parents=True)
                    break
                except FileExistsError:
                    print("Invalid directory")
                    dir_path = Path(input("Enter dir: "))

            global_config.LOCAL_FILE_SYSTEM_DIR = str(dir_path)

            print("\nEnabled USE_LOCAL_FILE_SYSTEM")


def prompt_configs():
    print("\nWould you like to edit the test configs? (yes or no)")
    set_configs = yes_or_no(msg="", retry="Invalid input (yes or no only): ")

    if set_configs:
        not_finished = True
        while not_finished:
            print(
                "\nPick one of the following options:\n"
                "[1] SKIP_SCHEMA_VALIDATE -- disable schema validation for payload\n"
                "[2] SKIP_WF_VALIDATE -- disable S3 and compute server checks\n"
                "[3] SKIP_USER_FUNCTION -- skip calling invoke user function\n"
                "[4] USE_LOCAL_USER_FUNC -- run a user function from your local filesystem\n"
                "[5] USE_LOCAL_FILE_SYSTEM -- use local directory rather than S3\n"
                "[exit]\n"
            )
            selection = input("Enter one of the above options: ")

            while (
                selection not in {f"{i}" for i in range(1, 6)} and selection != "exit"
            ):
                selection = input("Invalid input (1-5 or exit): ")

            if selection == "exit":
                break

            config_map = {
                "1": "SKIP_SCHEMA_VALIDATE",
                "2": "SKIP_WF_VALIDATE",
                "3": "SKIP_USER_FUNCTION",
                "4": "USE_LOCAL_USER_FUNC",
                "5": "USE_LOCAL_FILE_SYSTEM",
            }

            edit_config(config_map[selection])

            print("\nContinue editing configs? (yes or no)")
            not_finished = yes_or_no(msg="", retry="Invalid input (yes or no only): ")


def main():
    """
    FaaSr test entry point:

    Process payload
    Validate DAG, ensure datastores are accesible
    Initialize log and InvocationID if needed
    Run user functions using BFS
    """
    try:
        prompt_configs()

        start_time = datetime.now()

        # get payload
        faasr_payload = get_payload_from_env()
        if not global_config.SKIP_WF_VALIDATE:
            faasr_payload.start()
        else:
            logger.info("Skipping WF validation")

        global_config.add_s3_log_handler(faasr_payload, start_time)

        faasr_payload["InvocationID"] = str(uuid.uuid4())

        graph, ranks = build_adjacency_graph(faasr_payload)

        for func in graph:
            if ranks[func] == 0:
                first_func = func
                break
        else:
            raise RuntimeError("No start function (no node with zero predecessors)")

        function_executor = Executor(faasr_payload)

        # track function results for conditional branches
        results = dict()

        # queue for bfs traversal
        func_q = [first_func]

        dash_ct = max(len(action) for action in faasr_payload["ActionList"]) + 70

        # trigger all functions in workflow using bfs
        while func_q:
            new_q = []
            for func in func_q:
                if ranks[func] == 1 or ranks[func] == 0:
                    rank = 1
                else:
                    rank = ranks[func]

                for i in range(1, rank + 1):
                    sleep(0.5)
                    if rank > 1:
                        faasr_payload["FunctionRank"] = i
                        func_with_rank = f"{func}.{i}"
                        norm_dash_ct = dash_ct - len(func_with_rank)
                        print(
                            f"\n{func_with_rank.center(norm_dash_ct, '-')}\n",
                            flush=True,
                        )
                    else:
                        faasr_payload.remove("FunctionRank")
                        norm_dash_ct = dash_ct - len(func)
                        print(f"\n{func.center(norm_dash_ct, '-')}\n", flush=True)

                    faasr_payload["FunctionInvoke"] = func

                    # execute function
                    try:
                        faasr_payload.start()
                    except SystemExit:  # to-do: custom exception classes
                        continue

                    function_executor.faasr = faasr_payload

                    function_result = function_executor.run_func(func, start_time)
                    print(f"FUNCTION RESULT: {function_result}")
                    results[func] = function_result
                    logger.debug(
                        f"Finished execution of {func} with result {function_result}"
                    )

                    # add triggers to bfs queue
                    for next_invoke in faasr_payload["ActionList"][func]["InvokeNext"]:
                        # handle conditional branches
                        if isinstance(next_invoke, dict):
                            if results[func] is True:
                                next_invoke = next_invoke["True"]
                            elif results[func] is False:
                                next_invoke = next_invoke["False"]
                            else:
                                raise RuntimeError(
                                    f"Function {func} does not return a value but defines conditional branches"  # noqa E501
                                )

                        if isinstance(next_invoke, list):
                            for f in next_invoke:
                                f = f.split("(")[0]
                                new_q.append(f)
                        else:
                            new_q.append(next_invoke.split("(")[0])

                    print(f"FINISHED EXECUTION OF {func}")

            func_q = new_q

        log_sender = S3LogSender.get_log_sender()
        log_sender.flush_log()

        faasr_msg = (
            f"\nFinished action -- InvocationID: {faasr_payload['InvocationID']}\n"
        )
        logger.info(faasr_msg)
    finally:
        # restore changed config values
        global_config.restore()


if __name__ == "__main__":
    main()
