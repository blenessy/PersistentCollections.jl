using Test
using BenchmarkTools

using PersistentCollections: LMDB, PersistentDict

const ENV_DIR = "env.lmdb"
const UNSAFE_ENV_DIR = "unsafe_env.lmdb"
const TEST_DIRS = [ENV_DIR, UNSAFE_ENV_DIR]
# clean previous runs
for d in TEST_DIRS
    rm(d, force=true, recursive=true)
    mkdir(d)
end

@test string(LMDB.LMDBError(-30798)) == "MDB_NOTFOUND: No matching key/data pair found (-30798)" 

env = LMDB.Environment(ENV_DIR)
@test isopen(env)
@test env == LMDB.Environment(ENV_DIR)
@test close(env)
@test !isopen(env)
@test !close(env) # nothing bad should happen

env = LMDB.Environment(ENV_DIR, maxdbs=1)

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
@assert isimmutable(ro)

rw = Mutable(1, 2.5, (2, 3.4))
@assert !isimmutable(rw)

fastkey = LMDB.MDBValue("fastkey")
fastval = LMDB.MDBValue("fastval")


# Open up default database as a Dict
d = PersistentDict{Any,Any}(env, id="foo")

d["stringkey"] = "stringval"
d["byteskey"] = Vector{UInt8}("bytesval")
d["intkey"] = 1234
d["floatkey"] = 2.5
d["tuplekey"] = (1, 2.5)
d["immutable_struct_key"] = ro
d["mutable_struct_key"] = rw
d[fastkey] = fastval
# Set use-case
d["nothing_key"] = nothing

@test get(d, "stringkey", "") == "stringval"
@test get(d, "byteskey", UInt8[]) == Vector{UInt8}("bytesval")
@test get(d, "intkey", 0) == 1234
@test get(d, "floatkey", 0.0) == 2.5
@test get(d, "tuplekey", (0, 0.0)) == (1, 2.5)
@test get(d, "immutable_struct_key", Immutable(0, 0.0, (0, 0.0))) == ro
@test get(d, "immutable_struct_key", Mutable(0, 0.0, (0, 0.0))) == rw
@test get(d, fastkey, "") == "fastval"
# Set use-case
notfound = UInt8[0x1]
@test get(d, "nothing_key", notfound) != notfound

# delete
@test delete!(d, fastkey)
@test delete!(d, "stringkey")
# idempotency
@test !delete!(d, fastkey)

# check deleted
@test_throws KeyError d[fastkey]
@test_throws KeyError d["stringkey"]

@test convert(Int, d["intkey"]) == 1234
@test length(collect(keys(d))) > 0
@test length(collect(values(d))) == length(collect(keys(d)))
@test !isempty(collect(d))
@test (d["dictkey"] = "dictval") == "dictval"

# manual sync
unsafe_env = LMDB.Environment(UNSAFE_ENV_DIR, maxdbs=1, flags=LMDB.MDB_NOSYNC)
unsafe_dict = PersistentDict{String,Vector{UInt8}}(unsafe_env, id="foo")
flush(unsafe_env) do
    unsafe_dict["unsafe_key1"] = Vector{UInt8}("unsafe_val1")
    unsafe_dict["unsafe_key2"] = Vector{UInt8}("unsafe_val2")
end
# flushed when the flush block exits
unsafe_dict["unsafe_key1"] == Vector{UInt8}("unsafe_val1")
unsafe_dict["unsafe_key2"] == Vector{UInt8}("unsafe_val1")

# multi-threading - don't crash or deadlock while re-writing key/val
if Threads.nthreads() > 1 
    @info "doing $(Threads.nthreads()) parallel writes for ~10s (should not crash or deadlock) ..."
    randvals = [rand(Int) for _ in 1:Threads.nthreads()]
    deadline = time() + 10
    writes = [zero(Int) for _ in 1:Threads.nthreads()]
    Threads.@threads for i in 1:Threads.nthreads()
        while time() < deadline
            d[fastkey] = randvals[i]
        end
    end
    @info "... $(sum(writes)) writes completed!"
else
    @warn "skipping multi-threading tests (re-run with JULIA_NUM_THREADS=999999 to enable)"
end

# == Benchmarks ==

if get(ENV, "BENCH", "") == "y"
    longkey = "012345678901234567890123456789012345678901234567890123456789"

    @info "Benchmarking setindex!(::PersistentDict) ..."
    @btime setindex!(d, v, longkey) setup=(v=rand(UInt8, 500))

    @info "Benchmarking getindex(::PersistentDict) ..."
    @btime getindex(d, longkey)

    n = length(collect(keys(d)))
    @info "Benchmarking itertion: keys(::PersistentDict)) ($n entries) ..."
    @btime for _ in keys(d) end

    @info "Benchmarking itertion: iterated(::PersistentDict)) ($n entries) ..."
    @btime for _ in d end

    @info "Benchmarking MDB_NOSYNC + setindex!(::PersistentDict) ..."
    @btime setindex!(unsafe_dict, v, longkey) setup=(v=rand(UInt8, 500))
end
