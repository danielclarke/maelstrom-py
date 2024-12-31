#!/usr/bin/env python

if __name__ == "__main__":

    from main import Workbench, main

    try:
        main(Workbench.PN_COUNTER)
    except KeyboardInterrupt:
        exit()
