# PersistentCollections.jl

Julia AbstractDict and AbstractSet data structures persisted (ACID) to disk.

## Quick Start

TODO

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

- [ ] Travis CI integration
- [ ] Code Coverage integration
- [ ] All platforms supported
- [ ] Part of Julia Registry

### PersistentDict

- [x] Optimised implementation
- [x] Thread Safe
- [ ] MDB_NOSYNC support
- [x] Named database support
- [ ] Manual flush (sync) to disk

### PersistentSet

- [ ] Implemented
- [ ] Thread Safe
- [ ] MDB_NOSYNC support
- [ ] Named database support
- [ ] Manual flush (sync) to disk

## Credits

Lots of LMDB wrapping magic was pinched from [wildart/LMDB.jl](https://github.com/wildart/LMDB.jl) - who deserves lots of credits.

