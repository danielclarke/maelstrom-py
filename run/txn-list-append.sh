# ../maelstrom/maelstrom test -w txn-list-append --bin ./src/datomic.py --time-limit 20 --node-count 1 --log-stderr
# ../maelstrom/maelstrom test -w txn-list-append --bin ./src/datomic.py --time-limit 10 --node-count 2 --log-stderr
../maelstrom/maelstrom test -w txn-list-append --bin ./src/datomic.py --time-limit 10 --node-count 2 --log-stderr --rate 100