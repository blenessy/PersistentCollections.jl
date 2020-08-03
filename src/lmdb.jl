using LMDB_jll

# Error codes (TODO: define more)
const MDB_SUCCESS = Cint(0)
const MDB_NOTFOUND = Cint(-30798)

# Environment flags (TODO: define more)
const MDB_NOSUBDIR = Cuint(0x4000)
const MDB_NOSYNC = Cuint(0x10000)
const MDB_RDONLY = Cuint(0x20000)
const MDB_NOTLS = Cuint(0x200000)

# DBI flags
const MDB_CREATE = Cuint(0x40000)

# Cursor operation constants (TODO: define more)
@enum MDBCursorOp::Cint begin
    MDB_FIRST
    MDB_FIRST_DUP
    MDB_GET_BOTH
    MDB_GET_BOTH_RANGE
    MDB_GET_CURRENT
    MDB_GET_MULTIPLE
    MDB_LAST
    MDB_LAST_DUP
    MDB_NEXT
    MDB_NEXT_DUP
    MDB_NEXT_MULTIPLE
    MDB_NEXT_NODUP
    MDB_PREV
    MDB_PREV_DUP
    MDB_PREV_NODUP
    MDB_SET
    MDB_SET_KEY
    MDB_SET_RANGE
    MDB_PREV_MULTIPLE
end

# Common flag combo
const DEFAULT_ROTXN_FLAGS = MDB_RDONLY | MDB_NOTLS

const Cmode_t = Cushort

macro fieldoffset(T, num)
    return :(fieldoffset($T, $(esc(num))))
end

# mutable struct MDBValue{T}
#     size::Csize_t    # size of the data item
#     ptr::Ptr{Cvoid}  # address of the data item
#     ref::T           # keep a reference to the object we are pointing to
#     MDBValue(size, ptr, ref::T) where {T} = new{T}(size, ptr, ref)
# end

mutable struct MDBValue{T}
    size::Csize_t    # size of the data item
    ptr::Ptr{Cvoid}  # address of the data item
    data::T
    MDBValue() = new{Nothing}(zero(Csize_t), C_NULL)
    MDBValue{T}() where {T} = new{T}(zero(Csize_t), C_NULL)
    MDBValue(v::String) = new{String}(sizeof(v), pointer(v), v)
    MDBValue(v::Array) = new{Array}(sizeof(v), pointer(v), v)
    function MDBValue(v::T) where {T}
        # alloc new object; set size; copy primitive value or reference
        o = new{T}(sizeof(v), C_NULL, v)
        # mutable struct have static pointer address so its safe to just return that
        # primitive types and immutable structs have been copied into o - point to that instead
        o.ptr = ismutable(v) ? pointer_from_objref(v) : pointer_from_objref(o) + @fieldoffset(MDBValue{0}, 3)
        return o
    end
end
Base.pointer(val::MDBValue) = pointer_from_objref(val)

struct MDBStat
    ms_psize::Cuint
    ms_depth::Cuint
    ms_branch_pages::Csize_t
    ms_leaf_pages::Csize_t
    ms_overflow_pages::Csize_t
    ms_entries::Csize_t
    MDBStat() = new(zero(Cuint), zero(Cuint), zero(Csize_t), zero(Csize_t), zero(Csize_t), zero(Csize_t))
end

# MDBValue (output) -> T
Base.convert(::Type{String}, val::MDBValue{Nothing}) = unsafe_string(convert(Ptr{UInt8}, val.ptr), val.size)
Base.convert(::Type{Array{T,N}}, val::MDBValue{Nothing}) where {T,N} = unsafe_wrap(Array, convert(Ptr{T}, val.ptr), val.size)
Base.convert(::Type{T}, val::MDBValue{Nothing}) where {T} = unsafe_load(convert(Ptr{T}, val.ptr))
Base.convert(::Type{Any}, val::MDBValue{Nothing}) = val
Base.convert(::Type{MDBValue}, val::MDBValue{Nothing}) = val
Base.convert(::Type{MDBValue{Nothing}}, val::MDBValue{Nothing}) = val

# T -> MDBValue (input)
Base.convert(::Type{MDBValue}, val::MDBValue) = val
Base.convert(::Type{MDBValue}, val) = MDBValue(val)
Base.convert(::Type{T}, val::MDBValue{T}) where {T} = val.data

struct LMDBError <: Exception
    code::Cint
end
Base.show(io::IO, err::LMDBError) = print(io, "$(mdb_strerror(err.code)) ($(err.code))")

macro chkres(res)
    return :(iszero($(esc(res))) || throw(LMDBError($(esc(res)))))
end

function mdb_strerror(err::Cint)
    errstr = ccall((:mdb_strerror, liblmdb), Cstring, (Cint,), err)
    return unsafe_string(errstr)
end

function mdb_env_create()
    env_ref = Ref{Ptr{Cvoid}}(C_NULL)
    @chkres ccall((:mdb_env_create, liblmdb), Cint, (Ptr{Ptr{Cvoid}},), env_ref)
    return env_ref[]
end

function mdb_env_close(env::Ptr{Cvoid})
    ccall((:mdb_env_close, liblmdb), Cvoid, (Ptr{Cvoid},), env)
    return nothing
end

function mdb_env_open(env::Ptr{Cvoid}, path::String, flags::Cuint, mode::Cmode_t)
    @chkres ccall((:mdb_env_open, liblmdb), Cint, (Ptr{Cvoid}, Cstring, Cuint, Cmode_t), env, path, flags, mode)
    return nothing
end

function mdb_version()
    str = ccall((:mdb_version, liblmdb), Cstring, (Ptr{Cint}, Ptr{Cint}, Ptr{Cint}), C_NULL, C_NULL, C_NULL)
    return unsafe_string(str)
end

function mdb_txn_begin(env::Ptr{Cvoid}, flags::Cuint)
    txn_ref = Ref{Ptr{Cvoid}}(C_NULL)
    @chkres ccall((:mdb_txn_begin, liblmdb), Cint, (Ptr{Cvoid}, Ptr{Cvoid}, Cuint, Ptr{Ptr{Cvoid}}), env, C_NULL, flags, txn_ref)
    return txn_ref[]
end

function mdb_txn_abort(txn::Ptr{Cvoid})
    ccall((:mdb_txn_abort, liblmdb), Cvoid, (Ptr{Cvoid},), txn)
    return nothing
end

function mdb_txn_reset(txn::Ptr{Cvoid})
    ccall((:mdb_txn_reset, liblmdb), Cvoid, (Ptr{Cvoid},), txn)
    return nothing
end

function mdb_txn_renew(txn::Ptr{Cvoid})
    @chkres ccall((:mdb_txn_renew, liblmdb), Cint, (Ptr{Cvoid},), txn)
    return nothing
end

function mdb_txn_commit(txn::Ptr{Cvoid})
    @chkres ccall((:mdb_txn_commit, liblmdb), Cint, (Ptr{Cvoid},), txn)
    return
end

function mdb_dbi_open(txn::Ptr{Cvoid}, dbname::String, flags::Cuint)
    handle = Cuint[0]
    cdbname = isempty(dbname) ? convert(Cstring, Ptr{UInt8}(C_NULL)) : dbname
    @chkres ccall((:mdb_dbi_open, liblmdb), Cint, (Ptr{Cvoid}, Cstring, Cuint, Ptr{Cuint}), txn, cdbname, flags, handle)
    return handle[1]
end

function mdb_dbi_close(txn::Ptr{Cvoid}, dbi::Cuint)
    ccall((:mdb_dbi_close, liblmdb), Cvoid, (Ptr{Cvoid}, Cuint), txn, dbi)
    return nothing
end        

function mdb_env_set_maxreaders(env::Ptr{Cvoid}, readers::Cuint)
    @chkres ccall((:mdb_env_set_maxreaders, liblmdb), Cint, (Ptr{Cvoid}, Cuint), env, readers)
    return nothing
end

function mdb_env_set_mapsize(env::Ptr{Cvoid}, size::Csize_t)
    @chkres ccall((:mdb_env_set_mapsize, liblmdb), Cint, (Ptr{Cvoid}, Csize_t), env, size)
    return nothing
end

function mdb_env_set_maxdbs(env::Ptr{Cvoid}, dbs::Cuint)
    @chkres ccall((:mdb_env_set_maxdbs, liblmdb), Cint, (Ptr{Cvoid}, Cuint), env, dbs)
    return nothing
end

function mdb_env_sync(env::Ptr{Cvoid}, force::Cint)
    @chkres ccall((:mdb_env_sync, liblmdb), Cint, (Ptr{Cvoid}, Cint), env, force)
    return nothing
end


function mdb_put(txn::Ptr{Cvoid}, dbi::Cuint, key::Ptr{Cvoid}, val::Ptr{Cvoid}, flags::Cuint)
    @chkres ccall((:mdb_put, liblmdb), Cint, (Ptr{Cvoid}, Cuint, Ptr{Cvoid}, Ptr{Cvoid}, Cuint), txn, dbi, key, val, flags)
    return nothing
end        

function mdb_del(txn::Ptr{Cvoid}, dbi::Cuint, key::Ptr{Cvoid}, val::Ptr{Cvoid})
    res = ccall((:mdb_del, liblmdb), Cint, (Ptr{Cvoid}, Cuint, Ptr{Cvoid}, Ptr{Cvoid}), txn, dbi, key, val)
    (res == MDB_SUCCESS || res == MDB_NOTFOUND) || throw(LMDBError(res))
    return res != MDB_NOTFOUND
end        

function mdb_get!(txn::Ptr{Cvoid}, dbi::Cuint, key::Ptr{Cvoid}, val::Ptr{Cvoid})
    res = ccall((:mdb_get, liblmdb), Cint, (Ptr{Cvoid}, Cuint, Ptr{Cvoid}, Ptr{Cvoid}), txn, dbi, key, val)
    (res == MDB_SUCCESS || res == MDB_NOTFOUND) || throw(LMDBError(res))
    return res != MDB_NOTFOUND
end        

function mdb_cursor_open(txn::Ptr{Cvoid}, dbi::Cuint)
    handle = Ptr{Cvoid}[C_NULL]
    @chkres ccall((:mdb_cursor_open, liblmdb), Cint, (Ptr{Cvoid}, Cuint, Ptr{Ptr{Cvoid}}), txn, dbi, handle)
    return handle[1]
end

function mdb_cursor_close(cur::Ptr{Cvoid})
    ccall((:mdb_cursor_close, liblmdb), Cvoid, (Ptr{Cvoid},), cur)
    return nothing
end

function mdb_cursor_txn(cur::Ptr{Cvoid})
    return ccall((:mdb_cursor_txn, liblmdb), Ptr{Cvoid}, (Ptr{Cvoid},), cur)
end

function mdb_cursor_get!(cur::Ptr{Cvoid}, key::Ptr{Cvoid}, val::Ptr{Cvoid}, op::MDBCursorOp)
    res = ccall((:mdb_cursor_get, liblmdb), Cint, (Ptr{Cvoid}, Ptr{Cvoid}, Ptr{Cvoid}, Cint), cur, key, val, op)
    (res == MDB_SUCCESS || res == MDB_NOTFOUND) || throw(LMDBError(res))
    return res != MDB_NOTFOUND
end

function mdb_env_stat(env::Ptr{Cvoid})
    statref = Ref(MDBStat())
    @chkres ccall((:mdb_env_stat, liblmdb), Cint, (Ptr{Cvoid}, Ptr{MDBStat}), env, statref)
    return statref[]
end

mutable struct Environment
    handle::Ptr{Cvoid}
    path::String
    rotxn::Vector{Ptr{Cvoid}}
    wlock::ReentrantLock
end

const OPENED_ENVS = Dict{String,Environment}()

function Environment(path::String; flags::Cuint=zero(Cuint), mode::Cmode_t=0o755, maxdbs=0, mapsize=10485760, maxreaders=126, rotxnflags::Cuint=DEFAULT_ROTXN_FLAGS)
    env = get(OPENED_ENVS, path, nothing)
    if isnothing(env)
        rotxn = [C_NULL for i in 1:Threads.nthreads()]
        env = Environment(mdb_env_create(), path, rotxn, ReentrantLock())
        mdb_env_set_maxdbs(env.handle, convert(Cuint, maxdbs))
        mdb_env_set_mapsize(env.handle,  convert(Csize_t, mapsize))
        mdb_env_set_maxreaders(env.handle, convert(Cuint, maxreaders))
        try
            mdb_env_open(env.handle, path, flags, mode)
        catch e
            mdb_env_close(env.handle)
            rethrow(e)
        end
        OPENED_ENVS[path] = env
        # create read-only transaction handles, which can be used quickly (mdb_txn_renew/mdb_txn_reset use-case)
        for i in 1:Threads.nthreads()
            env.rotxn[i] = mdb_txn_begin(env.handle, rotxnflags)
            mdb_txn_reset(env.rotxn[i])
        end
        # register finalizer for lazy cleanup
        finalizer(close, env)
    end
    return env
end

Base.isopen(env::Environment) = env.handle != C_NULL

function Base.close(env::Environment)
    if isopen(env)
        for handle in env.rotxn
            handle == C_NULL || mdb_txn_abort(handle)
        end
        @assert get(OPENED_ENVS, env.path, nothing) == env "OPENED_ENVS does not contain env for $(path) - smells like threading issue ..."
        mdb_env_close(env.handle)
        env.handle = C_NULL
        delete!(OPENED_ENVS, env.path)
        return true
    end
    return false
end

function Base.flush(func::Function, env::Environment)
    try
        func()
    finally
        mdb_env_sync(env.handle, one(Cint))
    end
end

function __init__()
    @info mdb_version()
    # this will catch changes in struct layout
    @assert fieldname(MDBValue{Nothing}, 3) == :data
end