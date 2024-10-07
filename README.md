# My solutions for the fly.io Distributed Systems Challenge
This one has been on my TODO list for a long time.
I'm diverging from the original challenge a bit by using Rust instead of Go.
Visit the [challenge page on fly.io](https://fly.io/dist-sys/) if you want to learn more.

## Progress
> [!WARNING]
> A lot of the code is seriously janky. There is a lot of room for improvement in error handling,
> ergonomics, performance, and so on. However, these solutions do pass the Maelstrom tests.
> I might improve on the quality of the code at some point, but for now the focus is simply
> on learning these distributed systems principles and solving the challenges.

1. **Echo** challenge: solved ✅, solution in [echo.rs](src/bin/echo.rs).
2. **Unique ID Generation** challenge: solved ✅, solution in [guid.rs](src/bin/guid.rs).
3. Broadcast
   1. **Single-Node Broadcast** challenge: solved ✅, solution in [broadcast.rs](src/bin/broadcast.rs).
   2. **Multi-Node Broadcast** challenge: solved ✅, solution in [broadcast.rs](src/bin/broadcast.rs).
   3. **Fault Tolerant Broadcast** challenge: solved ✅, solution in [broadcast.rs](src/bin/broadcast.rs).
   4. **Efficient Broadcast, Part 1** challenge: solved ✅, solution in [broadcast.rs](src/bin/broadcast.rs).
   5. **Efficient Broadcast, Part 2** challenge: solved ✅, solution in [broadcast-e.rs](src/bin/broadcast-e.rs).
4. **Grow-Only Counter** challenge: solved ✅, solution in [g-counter.rs](src/bin/g-counter.rs).
5. Kafka-Style Log
   1. **Single-Node Kafka-Style Log** challenge: solved ✅, solution in [kafka.rs](src/bin/kafka.rs).

## Building and running the solutions
You will need to be able to compile Rust and run the Maelstrom tool to test the resulting binaries.
See the [Rust documentation](https://www.rust-lang.org/learn/get-started) to learn how to set up a Rust development environment.
See the [Maelstrom documentation](https://github.com/jepsen-io/maelstrom/blob/8b9e94c75e59250b82d1730d923f9f8e088ee227/doc/01-getting-ready/index.md) to learn how to install Maelstrom.

> [!TIP]
> This repository contains a **Nix flake** with a **dev shell** that contains the Rust toolchain and Maelstrom!
> If you have [Nix](https://nixos.org/) installed (with flakes enabled!) you can just run `nix develop` in this repository to enter the dev shell and start playing around.
> There is also a `.envrc` file which enables you to automatically enter the dev shell when entering the repository directory if you have [direnv](https://direnv.net/) installed.

Once you have all the tools installed you can build my solutions with `cargo build` and then run Maelstrom against the binaries, which will be placed in `target/debug/<name of the binary>`
(or in `target/release/<name of the binary>` if you ran cargo with `--release`).

The Maelstrom commands are as follows:
1. **Echo** challenge
```shell
maelstrom test -w echo --bin target/debug/echo --node-count 1 --time-limit 10
```
2. **Unique ID Generation** challenge
```shell
maelstrom test -w unique-ids --bin target/debug/guid --time-limit 30 --rate 1000 --node-count 3 --availability total --nemesis partition
```
3a. **Single-Node Broadcast** challenge
```shell
maelstrom test -w broadcast --bin target/debug/broadcast --node-count 1 --time-limit 20 --rate 10
```
3b. **Multi-Node Broadcast** challenge
```shell
maelstrom test -w broadcast --bin target/debug/broadcast --node-count 5 --time-limit 20 --rate 10
```
3c. **Fault Tolerant Broadcast** challenge
```shell
maelstrom test -w broadcast --bin target/debug/broadcast --node-count 5 --time-limit 20 --rate 10 --nemesis partition
```
3d. **Efficient Broadcast, Part 1** challenge
```shell
maelstrom test -w broadcast --bin target/debug/broadcast --node-count 25 --time-limit 20 --rate 100 --latency 100
```
Target metrics: messages-per-op < 30, median latency < 400 ms, maximum latency < 600 ms

3e. **Efficient Broadcast, Part 2** challenge
```shell
maelstrom test -w broadcast --bin target/debug/broadcast-e --node-count 25 --time-limit 20 --rate 100 --latency 100
```
Target metrics: messages-per-op: < 20, median latency < 1000 ms, maximum latency < 2000 ms

4. **Grow-Only Counter** challenge
```shell
maelstrom test -w g-counter --bin target/debug/g-counter --node-count 3 --rate 100 --time-limit 20 --nemesis partition
```
5a. **Single-Node Kafka-Style Log** challenge
```shell
maelstrom test -w kafka --bin target/debug/kafka --node-count 1 --concurrency 2n --time-limit 20 --rate 1000
```
