import argparse
import sys
from enum import StrEnum

from lib import broadcast, datomic, echo, g_counter, g_set, pn_counter


class Workbench(StrEnum):
    BROADCAST = "broadcast"
    DATOMIC = "datomic"
    ECHO = "echo"
    G_COUNTER = "g_counter"
    G_SET = "g_set"
    PN_COUNTER = "pn_counter"


def make_parser():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "-w",
        type=str,
        required=True,
        help="The workbench to run: echo | broadcast | ",
    )

    return parser


def main(workbench: Workbench):
    match workbench:
        case Workbench.BROADCAST:
            broadcast.run()
        case Workbench.DATOMIC:
            datomic.run()
        case Workbench.ECHO:
            echo.run()
        case Workbench.G_COUNTER:
            g_counter.run()
        case Workbench.G_SET:
            g_set.run()
        case Workbench.PN_COUNTER:
            pn_counter.run()


if __name__ == "__main__":
    parser = make_parser()
    args = parser.parse_args()

    try:
        workbench = Workbench(args.w)
    except ValueError as e:
        print(
            f"Unknown workbench option '{args.w}'. Valid options: {Workbench.ECHO} | {Workbench.BROADCAST}",
            file=sys.stderr,
        )
        exit()

    main(workbench=Workbench(args.w))
