module PersistentCollections
    export LMDB, PersistentDict

    module LMDB
        include(joinpath(@__DIR__, "lmdb.jl"))
    end

    struct PersistentDict{K,V} <: AbstractDict{K,V}
        env::LMDB.Environment
        id::String
        PersistentDict{K,V}(env; id="") where {K,V} = new{K,V}(env, id)
    end

    Base.show(io::IO, d::PersistentDict) = print(io, typeof(d), "(", isempty(d.id) ? "" : repr(e.id), ")")

    function rotxn(func::Function, d::PersistentDict)
        isopen(d.env) || error("Environment is closed")
        txn = d.env.rotxn[Threads.threadid()]
        LMDB.mdb_txn_renew(txn)
        try
            dbi = LMDB.mdb_dbi_open(txn, d.id, zero(Cuint))
            return func(txn, dbi)
        finally
            LMDB.mdb_txn_reset(txn)
        end
    end

    function rwtxn(func::Function, d::PersistentDict, sync::Bool)
        isopen(d.env) || error("Environment is closed")
        txn, commit = C_NULL, false
        lock(d.env.wlock) # need this lock otherwise it will deadlock
        try
            txn = LMDB.mdb_txn_begin(d.env.handle, sync ? zero(Cuint) : LMDB.MDB_NOSYNC)
            dbi = LMDB.mdb_dbi_open(txn, d.id, LMDB.MDB_CREATE)
            commit = func(txn, dbi)
            commit == false || LMDB.mdb_txn_commit(txn)
            return commit
        finally
            (commit || txn == C_NULL) || LMDB.mdb_txn_abort(txn)
            unlock(d.env.wlock)
        end
    end

    function cursor(func::Function, dict::PersistentDict)
        isopen(dict.env) || error("Environment is closed")
        txn, cur = LMDB.mdb_txn_begin(dict.env.handle, LMDB.DEFAULT_ROTXN_FLAGS), C_NULL
        try
            dbi = LMDB.mdb_dbi_open(txn, dict.id, zero(Cuint))
            cur = LMDB.mdb_cursor_open(txn, dbi)
            func(cur)
        catch e
            e.code == LMDB.MDB_NOTFOUND || rethrow(e)
            return false
        finally
            cur == C_NULL || LMDB.mdb_cursor_close(cur)
            LMDB.mdb_txn_abort(txn)
        end
    end

    function Base.get(d::PersistentDict{K,V}, key::K, default::D) where {K,V,D}
        mdbkey, mdbval = convert(LMDB.MDBValue, key), LMDB.MDBValue()
        found = rotxn(d) do txn, dbi
            GC.@preserve mdbkey LMDB.mdb_get!(txn, dbi, pointer(mdbkey), pointer(mdbval))
        end
        found || return default
        # try converting if possible
        V == Any || return convert(V, mdbval)
        D == Nothing || return convert(D, mdbval)
        return mdbval # return unconverted
    end

    function Base.getindex(d::PersistentDict{K,V}, key::K) where {K,V}
        val = get(d, key, nothing)
        isnothing(val) || return val
        throw(KeyError(key))
    end

    function Base.setindex!(d::PersistentDict{K,V}, val::V, key::K; sync=true) where {K,V}
        mdbkey, mdbval = convert(LMDB.MDBValue, key), convert(LMDB.MDBValue, val)
        rwtxn(d, sync) do txn, dbi
            GC.@preserve mdbkey mdbval LMDB.mdb_put(txn, dbi, pointer(mdbkey), pointer(mdbval), zero(Cuint))
            true
        end
        return val
    end

    function Base.delete!(d::PersistentDict{K,V}, key::K; sync=true) where {K,V}
        mdbkey = convert(LMDB.MDBValue, key)
        return rwtxn(d, sync) do txn, dbi
            GC.@preserve mdbkey LMDB.mdb_del(txn, dbi, pointer(mdbkey), C_NULL)
        end
    end

    abstract type AbstractMDBCursor end
    mutable struct MDBPairCursor{K,V} <: AbstractMDBCursor
        cur::Threads.Atomic{UInt}
    end
    mutable struct MDBKeyCursor{K,V} <: AbstractMDBCursor
        cur::Threads.Atomic{UInt}
    end
    mutable struct MDBValCursor{K,V} <: AbstractMDBCursor
        cur::Threads.Atomic{UInt}
    end

    function create_atomic_cursor(dict::PersistentDict)
        isopen(dict.env) || error("Environment is closed")
        txn = LMDB.mdb_txn_begin(dict.env.handle, LMDB.DEFAULT_ROTXN_FLAGS)
        cur = C_NULL
        try
            dbi = LMDB.mdb_dbi_open(txn, dict.id, zero(Cuint))
            cur = LMDB.mdb_cursor_open(txn, dbi)
        catch e
            LMDB.mdb_txn_abort(txn)
            # return C_NULL cur if database is empty
            e.code == LMDB.MDB_NOTFOUND || rethrow(e)
        end
        atomic = Threads.Atomic{UInt}(convert(UInt, cur))
        finalizer(close_atomic_cursor, atomic)
        return atomic
    end

    function close_atomic_cursor(atomic_cursor::Threads.Atomic{UInt})
        # Make sure that two threads do not close the same handles
        # by using Atomic exchange operation
        cur = convert(Ptr{Cvoid}, Threads.atomic_xchg!(atomic_cursor, convert(UInt, C_NULL)))
        if cur != C_NULL
            txn = LMDB.mdb_cursor_txn(cur)
            LMDB.mdb_cursor_close(cur)
            LMDB.mdb_txn_abort(txn)
        end
        return nothing
    end
   
    Base.close(iter::AbstractMDBCursor) = close_atomic_cursor(iter.cur)

    # multi-threading might introduce race-condition where wrong length is reported
    Base.IteratorSize(d::PersistentDict) = Base.SizeUnknown()
    Base.IteratorSize(iter::AbstractMDBCursor) = Base.SizeUnknown()
    Base.eltype(iter::MDBPairCursor{K,V}) where {K,V} = Pair{K,V}
    Base.eltype(iter::MDBKeyCursor{K,V}) where {K,V} = K
    Base.eltype(iter::MDBValCursor{K,V}) where {K,V} = V

    function Base.iterate(iter::MDBPairCursor{K,V}, op=LMDB.MDB_FIRST) where {K,V}
        mdbkey, mdbval = LMDB.MDBValue(), LMDB.MDBValue()
        finished = !LMDB.mdb_cursor_get!(convert(Ptr{Cvoid}, iter.cur[]), pointer(mdbkey), pointer(mdbval), op)
        finished || return (convert(K, mdbkey) => convert(V, mdbval), LMDB.MDB_NEXT)
        close(iter)
        return nothing
    end
    function Base.iterate(iter::MDBKeyCursor{K,V}, op=LMDB.MDB_FIRST) where {K,V}
        mdbkey, mdbval = LMDB.MDBValue(), LMDB.MDBValue()
        finished = !LMDB.mdb_cursor_get!(convert(Ptr{Cvoid}, iter.cur[]), pointer(mdbkey), pointer(mdbval), op)
        finished || return (convert(K, mdbkey), LMDB.MDB_NEXT)
        close(iter)
        return nothing        
    end
    function Base.iterate(iter::MDBValCursor{K,V}, op=LMDB.MDB_FIRST) where {K,V}
        mdbkey, mdbval = LMDB.MDBValue(), LMDB.MDBValue()
        finished = !LMDB.mdb_cursor_get!(convert(Ptr{Cvoid}, iter.cur[]), pointer(mdbkey), pointer(mdbval), op)
        finished || return (convert(V, mdbval), LMDB.MDB_NEXT)
        close(iter)
        return nothing        
    end

    function Base.iterate(d::PersistentDict{K,V}, iter=nothing) where {K,V}
        op = isnothing(iter) ? LMDB.MDB_FIRST : LMDB.MDB_NEXT
        if isnothing(iter)
            iter = MDBPairCursor{K,V}(create_atomic_cursor(d))
        end
        next = iterate(iter, op)
        return isnothing(next) ? nothing : (next[1], iter)
    end

    Base.keys(d::PersistentDict{K,V}) where {K,V} = MDBKeyCursor{K,V}(create_atomic_cursor(d))
    Base.values(d::PersistentDict{K,V}) where {K,V} = MDBValCursor{K,V}(create_atomic_cursor(d))

    function Base.length(d::PersistentDict)
        return rotxn(d) do txn, dbi
            convert(Int, LMDB.mdb_stat(txn, dbi).ms_entries)
        end
    end

    function Base.first(d::PersistentDict{K,V}) where {K,V}
        mdbkey, mdbval = LMDB.MDBValue(), LMDB.MDBValue()
        found = cursor(d) do cur
            LMDB.mdb_cursor_get!(cur, pointer(mdbkey), pointer(mdbval), LMDB.MDB_FIRST)
        end
        found || throw(BoundsError())
        return convert(K, mdbkey) => convert(V, mdbval)
    end

    function Base.last(d::PersistentDict{K,V}) where {K,V}
        mdbkey, mdbval = LMDB.MDBValue(), LMDB.MDBValue()
        found = cursor(d) do cur
            LMDB.mdb_cursor_get!(cur, pointer(mdbkey), pointer(mdbval), LMDB.MDB_LAST)
        end
        found || throw(BoundsError())
        return convert(K, mdbkey) => convert(V, mdbval)
    end

end # module
