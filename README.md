# PersistentCollections.jl

Julia `Dict` and `Set` data structures safely persisted to disk.

All collections are backed by [LMDB](https://en.wikipedia.org/wiki/Lightning_Memory-Mapped_Database) - a super fast B-Tree based embedded KV database with ACID guaranties.
As with other B-Tree based databases reads are generally faster than writes. LMDB is not an exception, although write performance is relatively good to (expect 1k-10k TPS).

Care was taken to make the datastructures thread-safe. LMDB handles most of the locking well, we just have to serialise the writes to an LMDB Environment in julia so that
multiple threads do not attempt to write at once (deadlock will occur).

## Quick Start

1. Install this package:
   ```julia
   import Pkg
   Pkg.add("https://github.com/blenessy/PersistentCollections.jl.git")
   ```
1. Create an `LMDB.Environment` in a directory called `data` (in your current working directory):
   ```julia
   using PersistentCollections
   env = LMDB.Environment("data")
   ```
1. Create an `AbstractDict` in your LMDB environment:
   ```julia
   dict = PersistentDict{String,String}(env)
   ```
1. Use it as any other dict:
   ```julia
   dict["foo"] = "bar"
   @assert dict["foo"] == "bar"
   @assert collect(keys(dict)) == ["foo"]
   @assert collect(values(dict)) == ["bar"]
   ```
1. (Optional) note the asymetric performance characteristic of LMDB (B-Tree) based database:
   ```julia
   @time dict["bar"] = "baz";  # Writes to LMDB (B-Tree) are relatively slow
   @time dict["bar"];          # Reads are very fast though :)
   ```

## User Guide

### Dynamic types

It is possible to create persistent collection of `Any` type although some methods will not be able to convert the value to the correct type because no metadata is stored for this in DB.
Most notably the `getindex` method (e.g. `dict["foo"]`) will not return a converted value. To mitigate this limitation, use the `get` method, which includes a default value.
The type of the default value (if other than `nothing`) will be used to convert the value to the desired type.

```julia
env = LMDB.Environment("data")
dict = PersistentDict{Any,Any}(env)
dict["foo"] == "bar"
dict["foo"]                  # PersistentCollections.LMDB.MDBValue{Nothing}(0x0000000000000003, Ptr{Nothing} @0x000000012c806ffd, nothing)
get(dict, "foo", "")         # "bar"
convert(String, dict["foo"]) # "bar"
```

### Multiple persistent collections in the same LMDB Environment

It is possible if you need transactional consistency between multiple persistent collections:

1. Create your `LMDB.Environment` with "named database" support by specifying the number of persistent collections yoy want with the `maxdbs` keyword argument:
   ```julia
   env = LMDB.Environment("data", maxdbs=2)
   ```
2. Instantiate your persistent collections with a unique (within LMDB env.) id:
   ```julia
   dict1 = PersistentDict{String,String}(env, id="mydict1")
   dict2 = PersistentDict{String,Int}(env, id="mydict2")
   ```

### Danger Zone: Manual sync writes to disc

Yes, you can expect significant increase with write throughput if you are willing to risk loosing your last written transactions.
Please note that database integrity (risk of curruption) is not in danger here.

```julia
unsafe_env = LMDB.Environment("data", flags=LMDB.MDB_NOSYNC)
unsafe_dict = PersistentDict{String,String}(unsafe_env)
flush(unsafe_env) do 
    unsafe_dict["foo"] = "bar"
    unsafe_dict["foo"] = "baz"
end # <== data is flushed to disk here
```

This is equvalent to: 

```julia
unsafe_env = LMDB.Environment("data", flags=LMDB.MDB_NOSYNC)
unsafe_dict = PersistentDict{String,String}(unsafe_env)
try
    unsafe_dict["foo"] = "bar"
    unsafe_dict["foo"] = "baz"
finally
    flush(unsafe_env)
end
```

## Running Tests

```julia
make test
```

### Analyzing Code Coverage

```julia
make coverage
```

## Benchmarks

```julia
make bench
```

## Status

### CI/CD

- [x] Travis CI integration
- [ ] Coveralls integration (when public)
- [ ] All platforms supported
- [ ] Part of Julia Registry

### PersistentDict

- [x] Optimised implementation
- [x] Thread Safe
- [x] MDB_NOSYNC support
- [x] Named database support
- [x] Manual flush (sync) to disk

### PersistentSet

- [ ] Implemented
- [ ] Thread Safe
- [ ] MDB_NOSYNC support
- [ ] Named database support
- [ ] Manual flush (sync) to disk

## Credits

Lots of LMDB wrapping magic was pinched from [wildart/LMDB.jl](https://github.com/wildart/LMDB.jl) - who deserves lots of credits.

