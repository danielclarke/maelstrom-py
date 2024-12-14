import argparse
import sys
from enum import StrEnum

from src import echo


class Workbench(StrEnum):
    ECHO = "echo"
    BROADCAST = "broadcast"


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
    print(f"running {workbench}")
    match workbench:
        case Workbench.ECHO:
            echo.run()
        case Workbench.BROADCAST:
            pass


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
