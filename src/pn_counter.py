#!/usr/bin/env python

if __name__ == "__main__":

    from main import main

    try:
        main("pn_counter")
    except KeyboardInterrupt:
        exit()
