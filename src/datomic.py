#!/usr/bin/env python

if __name__ == "__main__":

    from main import Workbench, main

    try:
        main(Workbench.DATOMIC)
    except KeyboardInterrupt:
        exit()
