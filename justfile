test:
    zig build test

# Don't know why 'zig build kv' is not installing to zig-out
kv-mem-check:
    zig build -Drelease-fast && valgrind --leak-check=full zig-out/bin/kv
