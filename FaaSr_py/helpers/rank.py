import logging
import sys

from FaaSr_py.helpers.graph_functions import get_ranks

logger = logging.getLogger(__name__)


def faasr_rank(faasr_payload):
    """
    Returns the rank # and total rank of the current function

    Returns:
        namedtuple with elements MaxRank and Rank | None if rank is not set
    """
    # get current function name
    curr_func_name = faasr_payload["FunctionInvoke"]

    # get rank info
    ranks = get_ranks(faasr_payload)
    max_rank = ranks.get(curr_func_name)

    if max_rank and max_rank > 1:
        instance_rank = faasr_payload.get("FunctionRank")

        if not instance_rank:
            logger.error(
                "Internal error: ranked function but FunctionRank is missing; "
                "please report this so that we can fix it"
            )
            sys.exit(1)

        return {"max_rank": max_rank, "rank": instance_rank}
    else:
        return {"max_rank": 1, "rank": 1}
