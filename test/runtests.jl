using Test
using BenchmarkTools

using PersistentCollections: Environment, LMDB, load!

const TEST_DIR="test.lmdb"
# clean previous runs
rm(TEST_DIR, force=true, recursive=true)
mkdir(TEST_DIR)

@test string(LMDB.LMDBError(-30798)) == "MDB_NOTFOUND: No matching key/data pair found (-30798)" 

env = Environment(TEST_DIR)
@test isopen(env)
@test env == Environment(TEST_DIR)
@test close(env)
@test !isopen(env)
@test !close(env) # nothing bad should happen

env = Environment(TEST_DIR)

struct Immutable
    a::Int
    b::Float64
    c::Tuple{Int,Float64}
end
Base.:(==)(a::Immutable, b::Immutable) = a.a == b.a && a.b == b.b && a.c == b.c

mutable struct Mutable
    a::Int
    b::Float64
    c::Tuple{Int,Float64}
end
Base.:(==)(a::Mutable, b::Mutable) = a.a == b.a && a.b == b.b && a.c == b.c

ro = Immutable(1, 2.5, (2, 3.4))
@assert !ismutable(ro)

rw = Mutable(1, 2.5, (2, 3.4))
@assert ismutable(rw)

fastkey = LMDB.MDBValue("fastkey")
fastval = LMDB.MDBValue("fastval")

write(env) do txn, dbi
    put!(txn, dbi, fastkey, fastval)
    put!(txn, dbi, "stringkey", "stringval")
    put!(txn, dbi, "byteskey", Vector{UInt8}("bytesval"))
    put!(txn, dbi, "intkey", 1234)
    put!(txn, dbi, "floatkey", 2.5)
    put!(txn, dbi, "tuplekey", (1, 2.5))
    put!(txn, dbi, "immutable_struct_key", ro)
    put!(txn, dbi, "mutable_struct_key", rw)
    # Set use-case
    put!(txn, dbi, "nothing_key", nothing)
end

read(env) do txn, dbi
    @test get(txn, dbi, fastkey, "") == "fastval"
    @test get(txn, dbi, "stringkey", "") == "stringval"
    @test get(txn, dbi, "byteskey", UInt8[]) == Vector{UInt8}("bytesval")
    @test get(txn, dbi, "intkey", 0) == 1234
    @test get(txn, dbi, "floatkey", 0.0) == 2.5
    @test get(txn, dbi, "tuplekey", (0, 0.0)) == (1, 2.5)
    @test get(txn, dbi, "immutable_struct_key", Immutable(0, 0.0, (0, 0.0))) == ro
    @test get(txn, dbi, "immutable_struct_key", Mutable(0, 0.0, (0, 0.0))) == rw
    # Set use-case
    notfound = UInt8[0x1]
    @test get(txn, dbi, "nothing_key", notfound) != notfound
end

# delete
write(env) do txn, dbi
    @test delete!(txn, dbi, fastkey)
    @test delete!(txn, dbi, "stringkey")
    # idempotency
    @test !delete!(txn, dbi, fastkey)
end

# check deleted
read(env) do txn, dbi
    @test get(txn, dbi, fastkey, "") == ""
    @test get(txn, dbi, "stringkey", "") == ""
end

# iteration
key, val = LMDB.MDBValue(), LMDB.MDBValue()
sorted_keys = String[]
foreach(env, key, val) do i
    push!(sorted_keys, convert(String, key))
    return LMDB.MDB_NEXT
end
# should be sorted lexically by default
@test !isempty(sorted_keys)
@test sorted_keys == sort(sorted_keys)
@test length(env) == length(sorted_keys)

# multi-threading - don't crash or deadlock while re-writing key/val
@assert Threads.nthreads() > 1 "not possible to run multi-threading tests (re-run with JULIA_NUM_THREADS=999999)"
@info "doing $(Threads.nthreads()) parallel writes for ~10s (should not crash or deadlock) ..."
randvals = [rand(Int) for _ in 1:Threads.nthreads()]
deadline = time() + 10
writes = [zero(Int) for _ in 1:Threads.nthreads()]
Threads.@threads for i in 1:Threads.nthreads()
    while time() < deadline
        write(env) do txn, dbi
            put!(txn, dbi, fastkey, randvals[i])
            writes[i] += 1
        end
    end
end
@info "... $(sum(writes)) writes completed!"

if get(ENV, "BENCH", "") == "true"
    @info "Benchmarking write(::Environment) ..."
    Threads.@threads for i in 1:Threads.nthreads()
        @btime write(env) do txn, dbi
            put!(txn, dbi, fastkey, randvals[$i]);
        end
    end

    @info "Benchmarking read(::Environment) ..."
    Threads.@threads for _ in 1:Threads.nthreads()
        @btime read(env) do txn, dbi
            load!(txn, dbi, fastkey, fastval);
        end
    end

    @info "Benchmarking foreach(::Environment, ::MDBValue, ::MDBValue) ($(length(env)) entries) ..."
    Threads.@threads for _ in 1:Threads.nthreads()    
        @btime foreach(env, fastkey, fastval) do _
            return nothing
        end
    end
end
