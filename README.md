# shmipc-cpp

A production-grade C++23 translation of [shmipc-rs](https://github.com/cloudwego/shmipc-rs),
CloudWeGo's high-performance shared-memory IPC library. The Rust crate is the
single source of truth for architecture, memory ownership, and thread-safety
model; this port follows it module-for-module.

Shmipc connects two processes over a Unix socket (or TCP loopback), exchanges
shared-memory metadata, and then communicates via:

- **A shared-memory IO queue** — a lock-free ring of
  `(seq_id, shm_offset, status)` elements for batched wake-ups.
- **A shared-memory buffer pool** — size-classed free lists over one `mmap`
  region, with linked multi-slice buffers and a UDS **fallback path** when
  shared memory is exhausted.
- **Stream multiplexing** — yamux-style sessions multiplexing logical streams,
  with read/write loops, event dispatch, stream pooling, a circuit breaker,
  and periodic session rebuild.

## Building

Requires CMake ≥ 3.20, a C++23 compiler (GCC ≥ 13), and Linux. GoogleTest is
vendored in `third_party/` (no network needed).

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=RelWithDebInfo
cmake --build build -j
```

Sanitizer builds:

```bash
cmake -S . -B build-asan -DCMAKE_BUILD_TYPE=Debug -DSHMIPC_ENABLE_ASAN=ON
cmake -S . -B build-tsan -DCMAKE_BUILD_TYPE=Debug -DSHMIPC_ENABLE_TSAN=ON
```

Options: `SHMIPC_BUILD_TESTS` (default ON), `SHMIPC_BUILD_EXAMPLES` (ON),
`SHMIPC_BUILD_BENCH` (ON), `SHMIPC_ENABLE_ASAN`, `SHMIPC_ENABLE_TSAN`.

## Testing

```bash
cd build && ctest --output-on-failure
```

Nine suites mirror the Rust `#[cfg(test)]` modules and `tests/test.rs`:

| Suite | Rust source |
|---|---|
| `test_config` | `src/config.rs` |
| `test_slice` | `src/buffer/slice.rs` |
| `test_buffer_list` | `src/buffer/list.rs` |
| `test_buffer_manager` | `src/buffer/manager.rs` |
| `test_linked_buffer` | `src/buffer/linked.rs` |
| `test_queue` | `src/queue.rs` |
| `test_util` | `src/util/*.rs` |
| `test_protocol` | handshake codec + V2/V3 over a socketpair |
| `test_integration` | `tests/test.rs` end-to-end |

## Layout

```
include/shmipc/     public headers (umbrella: shmipc/shmipc.hpp)
  buffer/           slice/list/manager/linked/buf + layout constants
  protocol/         header/event/block_io/initializer/adapter/protocol
  sync/             channel + event (tokio mpsc / Notify equivalents)
  util/             dev_shm, buf_reader, shmbuf_reader
src/                implementations (mirror the include tree)
tests/              GoogleTest suites
examples/           greeter_{server,client}{,_tokio}
bench/              shmipc-vs-UDS parallel ping-pong
third_party/        vendored googletest
```

## Concurrency model

The Rust crate is built on tokio (async tasks, mpsc channels, `Notify`,
oneshot). C++ has no equivalent runtime, so this port uses a **thread-mirrored
model**:

| Rust (tokio) | C++ |
|---|---|
| read/write loop tasks | dedicated `std::thread` per session |
| `mpsc::channel` | `shmipc::Channel`/`Sender`/`Receiver` (mutex + condvar) |
| `oneshot` | `std::promise<void>` / `std::future<void>` |
| `Notify` (shutdown signaling) | `shmipc::Event` (manual-reset) |
| stream `recv_notify`/`close_notify` | one condition variable per stream |
| blocking handshake (`spawn_blocking` + timeout) | socket-level `SO_RCVTIMEO`/`SO_SNDTIMEO` |

The public API is blocking: `Stream::read_bytes`, `flush`, `close`, etc. block
the caller until complete.

## Key translation decisions

- `Result<T, E>` → `std::expected<T, Error>`; `Option<T>` → `std::optional<T>`;
  data-carrying enums → `std::variant` / tagged classes.
- `Box<T>` → `std::unique_ptr<T>`; `Arc<T>` → `std::shared_ptr<T>`.
- `bytes::Bytes` → `shmipc::Bytes` (immutable refcounted buffer); `Buf<'shm>` →
  `shmipc::buffer::Buf` (variant of a shared-memory view and owned bytes).
- Raw shared-memory layouts (`Header`, `BufferHeader`, queue head/tail,
  `BufferList`) are kept bit-compatible with the Rust wire/shm format,
  including big-endian frame fields and the (intentionally unaligned) x86_64
  queue head/tail offsets.
- RAII replaces the manual `Vec::from_raw_parts` / `mem::forget` ownership
  juggling for fallback (non-shm) slices, eliminating a class of leaks.

## Intentional fixes vs. upstream

Two upstream defects were corrected here (with approval), each marked in the
source:

1. `BufferList::mapping` read the `counter` field from header offset `+24`
   while `create` wrote it at `+20` (`buffer/list.rs`). Both now use `+20`.
2. `Stream::flush` returned `Ok(())` after 10 queue-full retries, silently
   dropping and leaking the send buffer (`stream.rs`). The C++ port recycles
   the buffer and reports `QueueFull` so the caller can retry.

In addition, the self-join hazard in `~SessionShared` (a backend loop thread
holding the last `shared_ptr`) is handled by detaching instead of joining the
calling thread.

## Benchmark

`bench/shmipc_bench.cpp` runs the same parallel ping-pong as the Rust
criterion bench (shmipc vs UDS, 64 B – 4 MiB). It reproduces shmipc's
signature behavior: latency stays nearly flat with message size while UDS grows
linearly, so shmipc dominates for large messages. Small-message latency is
higher than the Rust async build — an inherent trade-off of the blocking
thread model versus an event loop.

```bash
./build/shmipc_bench [concurrency] [base_iters]
```

## License

Apache-2.0, matching the upstream project.
