# ../maelstrom/maelstrom test -w broadcast --bin ./src/broadcast.py --nodes n1 --time-limit 10 --log-stderr
# ../maelstrom/maelstrom test -w broadcast --bin ./src/broadcast.py --nodes n1,n2 --time-limit 10 --log-stderr
# ../maelstrom/maelstrom test -w broadcast --bin ./src/broadcast.py --time-limit 20 --rate 100 --node-count 25 --log-stderr
# ../maelstrom/maelstrom test -w broadcast --bin ./src/broadcast.py --time-limit 20 --nemesis partition
../maelstrom/maelstrom test -w broadcast --bin ./src/broadcast.py --time-limit 20 --rate 100 --nemesis partition