-module(eNifLmdb).

%% NIF function declarations
-export([test/0, test/1, test1/0, test/2, test/3]).
%% 环境管理函数
-export([
	nif_env_open/5,            %% 打开LMDB环境
	nif_env_close/1,           %% 关闭LMDB环境
	nif_env_set_mapsize/2,     %% 设置内存映射大小
	nif_env_info/1,            %% 获取环境信息
	nif_env_stat/1,            %% 获取环境统计信息
	nif_env_sync/2,            %% 同步数据到磁盘
	nif_env_copy/3             %% 复制环境到文件
]).

%% 数据库操作函数
-export([
	nif_dbi_open/3,            %% 打开数据库
	nif_dbi_stat/2,            %% 获取数据库统计
	nif_dbi_flags/2            %% 获取数据库标志
]).

%% 数据操作函数
-export([
	nif_put/6,                 %% 存储数据
	nif_get/3,                 %% 获取数据
	nif_del/4,				   %% 删除数据
	nif_drop/3                 %% 清空或删除数据库
]).

%% 批量操作函数
-export([
    nif_get_multi/2,           %% 批量获取数据
    nif_writes/4,               %% 批量存储/删除数据
    nif_traversal/6				   %% 批量遍历数据库
]).

-type env_handle() :: binary().
-type dbi_handle() :: non_neg_integer().
-type key() :: term().
-type value() :: term().

-on_load(init/0).

init() ->
	SoName = case code:priv_dir(?MODULE) of
		{error, _} ->
			case code:which(?MODULE) of
				Filename when is_list(Filename) ->
					filename:join([filename:dirname(Filename), "../priv", "eNifLmdb"]);
				_ ->
					filename:join(["priv", "eNifLmdb"])
			end;
		Dir ->
			filename:join(Dir, "eNifLmdb")
	end,
	erlang:load_nif(SoName, 0).

%%%==============================测试函数 start =====================================
test1() ->
	erlang:nif_error({not_loaded, [{module, ?MODULE}, {line, ?LINE}]}).

test() ->
	erlang:nif_error({not_loaded, [{module, ?MODULE}, {line, ?LINE}]}).
test(_A) ->
	erlang:nif_error({not_loaded, [{module, ?MODULE}, {line, ?LINE}]}).
test(_A, _B) ->
	erlang:nif_error({not_loaded, [{module, ?MODULE}, {line, ?LINE}]}).
test(_A, _B, _C) ->
	erlang:nif_error({not_loaded, [{module, ?MODULE}, {line, ?LINE}]}).

%%%==============================环境管理函数 start =====================================
%% @doc 打开LMDB环境
%% @param Path 数据库路径
%% @param MapsSize 内存映射大小（字节）
%% @param MaxDbs 最大数据库数量
%% @param MaxReaders 最大读线程数
%% @param Flags 环境标志位
%%          - 0: 默认模式
%%          - MDB_NOSUBDIR (1): 不使用子目录，Path直接作为数据库文件
%%          - MDB_RDONLY (2): 只读模式
%%          - MDB_WRITEMAP (4): 使用写内存映射
%%          - MDB_NOMETASYNC (8): 不强制元数据同步
%%          - MDB_NOSYNC (16): 不强制数据同步
%%          - MDB_MAPASYNC (32): 异步内存映射
%%          - MDB_NOTLS (64): 不使用线程本地存储
%%          - MDB_NOLOCK (128): 不使用锁文件
%%          - MDB_NORDAHEAD (256): 不预读
%%          - MDB_PREVSNAPSHOT (512): 使用前一个快照
%% @returns {ok, EnvRef} | {error, Reason}
-spec nif_env_open(Path :: string(), MapsSize :: integer(), MaxDbs :: integer(), MaxReaders :: integer(), Flags :: integer()) -> {ok, reference()} | {error, term()}.
nif_env_open(_Path, _MapsSize, _MaxDbs, _MaxReaders, _Flags) ->
	erlang:nif_error({not_loaded, [{module, ?MODULE}, {line, ?LINE}]}).

%% @doc 关闭LMDB环境
%% 关闭指定的LMDB环境，释放相关资源
%% @param EnvHandle 环境句柄
%% @returns ok | {error, term()}
-spec nif_env_close(EnvHandle :: env_handle()) -> ok | {error, term()}.
nif_env_close(_EnvHandle) ->
	erlang:nif_error({not_loaded, [{module, ?MODULE}, {line, ?LINE}]}).

%% @doc 设置内存映射大小
%% 设置LMDB环境的内存映射文件大小
%% @param EnvHandle 环境句柄
%% @param Size 内存映射大小（字节）
%% @returns ok | {error, term()}
-spec nif_env_set_mapsize(EnvHandle :: env_handle(), Size :: integer()) -> ok | {error, term()}.
nif_env_set_mapsize(_EnvHandle, _Size) ->
	erlang:nif_error({not_loaded, [{module, ?MODULE}, {line, ?LINE}]}).



%% @doc 获取环境信息 
 %% 获取LMDB环境的详细信息 
 %% @param EnvHandle 环境句柄 
 %% @returns {ok, tuple()} | {error, term()}
 %%          返回格式：{envInfo, MapAddr, MapSize, LastPgno, LastTxnId, MaxReaders, NumReaders, QueueSize}
 -spec nif_env_info(EnvHandle :: env_handle()) -> {ok, tuple()} | {error, term()}. 
 nif_env_info(_EnvHandle) -> 
 	 erlang:nif_error({not_loaded, [{module, ?MODULE}, {line, ?LINE}]}).

%% @doc 获取环境统计信息
%% 获取LMDB环境的统计信息
%% @param EnvHandle 环境句柄
%% @returns {ok, tuple()} | {error, term()}
%%          返回格式：{envStat, PSize, Depth, BranchPages, LeafPages, OverflowPages, Entries}
-spec nif_env_stat(EnvHandle :: env_handle()) -> {ok, tuple()} | {error, term()}.
nif_env_stat(_EnvHandle) ->
    erlang:nif_error({not_loaded, [{module, ?MODULE}, {line, ?LINE}]}).

%% @doc 同步数据到磁盘
%% 强制将LMDB环境的数据同步到磁盘
%% @param EnvHandle 环境句柄
%% @returns ok | {error, term()}
-spec nif_env_sync(EnvHandle :: env_handle(), Force :: integer()) -> ok | {error, term()}.
nif_env_sync(_EnvHandle, _Force) ->
	erlang:nif_error({not_loaded, [{module, ?MODULE}, {line, ?LINE}]}).

%% @doc 复制环境到文件
%% 将LMDB环境复制到指定文件，支持压缩选项
%% @param EnvHandle 环境句柄
%% @param Path 目标文件路径
%% @param Flags 复制标志
%%          - 0: 普通复制
%%          - ?MDB_CP_COMPACT (1): 压缩复制 - 省略空闲空间，顺序重新编号所有页面
%% @returns ok | {error, term()}
-spec nif_env_copy(EnvHandle :: env_handle(), Path :: string(), Flags :: integer()) -> ok | {error, term()}.
nif_env_copy(_EnvHandle, _Path, _Flags) ->
	erlang:nif_error({not_loaded, [{module, ?MODULE}, {line, ?LINE}]}).



%%%==============================环境管理函数 end =====================================

%%%==============================数据库操作函数 start ==================================
%% @doc 打开数据库
%% 在LMDB环境中打开或创建数据库
%% @param EnvHandle 环境句柄
%% @param DbName 数据库名称（字符串或空字符串表示默认数据库）
%% @param Flags 数据库标志位
%%          - 0: 默认模式
%%          - MDB_REVERSEKEY (16#02): 键按逆序存储
%%          - MDB_DUPSORT (16#04): 允许重复键
%%          - MDB_INTEGERKEY (16#08): 键为整数
%%          - MDB_DUPFIXED (16#10): 重复键使用固定大小值
%%          - MDB_INTEGERDUP (16#20): 重复键为整数
%%          - MDB_REVERSEDUP (16#40): 重复键按逆序存储
%%          - MDB_CREATE (16#40000): 如果数据库不存在则创建
%% @returns {ok, dbi_handle()} | {error, term()}
-spec nif_dbi_open(EnvHandle :: env_handle(), DbName :: string(), Flags :: integer()) -> {ok, dbi_handle()} | {error, term()}.
nif_dbi_open(_EnvHandle, _DbName, _Flags) ->
	erlang:nif_error({not_loaded, [{module, ?MODULE}, {line, ?LINE}]}).

%% @doc 获取数据库统计信息 
 %% 获取指定数据库的统计信息 
 %% @param EnvHandle 环境句柄 
 %% @param DbiHandle 数据库句柄 
 %% @returns {ok, tuple()} | {error, term()}
 %%          返回格式：{dbiStat, PSize, Depth, BranchPages, LeafPages, OverflowPages, Entries}
 -spec nif_dbi_stat(EnvHandle :: env_handle(), DbiHandle :: dbi_handle()) -> {ok, tuple()} | {error, term()}. 
 nif_dbi_stat(_EnvHandle, _DbiHandle) -> 
 	 erlang:nif_error({not_loaded, [{module, ?MODULE}, {line, ?LINE}]}).

%% @doc 获取数据库标志
%% 获取指定数据库的标志设置
%% @param EnvHandle 环境句柄
%% @param DbiHandle 数据库句柄
%% @returns {ok, integer()} | {error, term()}
-spec nif_dbi_flags(EnvHandle :: env_handle(), DbiHandle :: dbi_handle()) -> {ok, integer()} | {error, term()}.
nif_dbi_flags(_EnvHandle, _DbiHandle) ->
	erlang:nif_error({not_loaded, [{module, ?MODULE}, {line, ?LINE}]}).


%%%==============================数据库操作函数 end ====================================

%%%==============================数据操作函数 start ====================================
%% @doc 插入或更新键值对
%% @param EnvHandle 环境句柄
%% @param Dbi 数据库索引
%% @param Key 键
%% @param Value 值
%% @param Flags 操作标志位
%%          - 0: 默认模式
%%          - MDB_NOOVERWRITE (16#10): 如果键已存在则不覆盖
%%          - MDB_NODUPDATA (16#20): 不插入重复数据
%%          - MDB_CURRENT (16#40): 更新当前数据
%%          - MDB_RESERVE (16#10000): 预留空间
%%          - MDB_APPEND (16#20000): 追加数据
%%          - MDB_APPENDDUP (16#40000): 追加重复数据
%% @param Mode 执行模式
%%          - 0: 异步无结果 - 发送到写进程，不等待结果
%%          - 1: 异步等待结果 - 发送到写进程，等待结果返回
%%          - 2: 同步执行 - 在当前NIF函数内直接执行
%% @returns ok | {ok, ok} | {error, term()} | {wait_write, integer()}
-spec nif_put(EnvHandle :: env_handle(), Dbi :: integer(),
	Key :: key(), Value :: value(), Flags :: integer(), Mode :: integer()) -> ok | {ok, ok} | {error, term()} | {wait_write, integer()}.
nif_put(_EnvHandle, _DbiIndex, _Key, _Value, _Flags, _Mode) ->
	erlang:nif_error({not_loaded, [{module, ?MODULE}, {line, ?LINE}]}).

%% @doc 获取数据
%% 从数据库中获取指定键对应的值
%% @param EnvHandle 环境句柄
%% @param DbiIndex 数据库索引
%% @param Key 键
%% @param Mode 获取模式
%% @returns {ok, value()} | {error, notFound | term()}
-spec nif_get(EnvHandle :: env_handle(), DbiIndex :: integer(), Key :: key()) -> {ok, value()} | {error, notFound | term()}.
nif_get(_EnvHandle, _DbiIndex, _Key) ->
	erlang:nif_error({not_loaded, [{module, ?MODULE}, {line, ?LINE}]}).


%% @doc 删除键值对
%% @param EnvHandle 环境句柄
%% @param Dbi 数据库索引
%% @param Key 键
%% @param Mode 执行模式
%%          - 0: 异步无结果 - 发送到写进程，不等待结果
%%          - 1: 异步等待结果 - 发送到写进程，等待结果返回
%%          - 2: 同步执行 - 在当前NIF函数内直接执行
%%          - 3: 同步执行并返回值 - 在单个事务中获取值并删除，确保原子性
%% @returns ok | {ok, ok} | {error, notFound | term()} | {wait_write, integer()}
%%          模式3（同步执行并返回值）时，如果键不存在会返回
-spec nif_del(EnvHandle :: env_handle(), Dbi :: integer(), Key :: key(), Mode :: integer()) -> ok | {ok, ok} | {error, notFound | term()} | {wait_write, integer()} | notFound.
nif_del(_EnvHandle, _DbiIndex, _Key, _Mode) ->
	erlang:nif_error({not_loaded, [{module, ?MODULE}, {line, ?LINE}]}).

%% @doc 清空或删除数据库
%% 清空指定数据库的所有数据或删除数据库
%% 注意：DROP操作只支持同步模式，不支持异步操作
%% @param EnvHandle 环境句柄
%% @param DbiIndex 数据库索引
%% @param Mode 操作模式
%%          - 0: 清空数据库 - 删除所有数据但保留数据库结构
%%          - 1: 删除数据库 - 从环境中删除数据库并关闭句柄
%% @returns ok | {error, term()}
-spec nif_drop(EnvHandle :: env_handle(), DbiIndex :: integer(), Mode :: integer()) -> ok | {error, term()}.
nif_drop(_EnvHandle, _DbiIndex, _Mode) ->
	erlang:nif_error({not_loaded, [{module, ?MODULE}, {line, ?LINE}]}).
%%%==============================数据操作函数 end ======================================
%%%==============================批量操作函数 start ====================================
%% @doc 批量查询多个键值对
%% @param EnvHandle 环境句柄
%% @param QueryList 查询列表，格式为[{Dbi, Key}, ...]
%% @returns {ok, list()} | {error, term()}
%%          list()中每个元素格式为：{Key, ok, Value} | {Key, error, Reason}
%%          其中Reason可能为notFound（当键不存在时）
-spec nif_get_multi(EnvHandle :: env_handle(), QueryList :: list()) -> {ok, list()} | {error, term()}.
nif_get_multi(_EnvHandle, _QueryList) ->
    erlang:nif_error({not_loaded, [{module, ?MODULE}, {line, ?LINE}]}).

%% @doc 批量写入操作
%% @param EnvHandle 环境句柄
%% @param OperationList 操作列表，格式为[{Dbi, Key, Value}, {Dbi, Key} ...]
%% @param Mode 执行模式
%%          - 0: 异步无结果 - 发送到写进程，不等待结果
%%          - 1: 异步等待结果 - 发送到写进程，等待结果返回
%%          - 2: 同步执行 - 在当前NIF函数内直接执行
%% @param SameTransaction 事务模式
%%          - 0: 不同事务 - 每个操作使用独立事务
%%          - 1: 相同事务 - 所有操作使用同一个事务
%% @returns ok | {ok, ok} | {error, term()} | {wait_write, integer()}
-spec nif_writes(EnvHandle :: env_handle(), OperationList :: list(), Mode :: integer(), SameTransaction :: integer()) ->
    ok | {ok, ok} | {error, term()} | {wait_write, integer()}.
nif_writes(_EnvHandle, _OperationList, _Mode, _SameTransaction) ->
    erlang:nif_error({not_loaded, [{module, ?MODULE}, {line, ?LINE}]}).

%% @doc 遍历数据库
%% 遍历指定数据库中的数据，支持正序和反序遍历
%% @param EnvHandle 环境句柄
%% @param DbiIndex 数据库索引
%% @param StartKey 起始键（可选，可以是'$TvsBegin'表示从头开始）
%% @param BatchSize 批量大小（可选，默认100）
%% @param Direction 遍历方向（0-正序，1-反序）
%% @param ReturnType 返回类型（0-键值对，1-仅键）
%% @returns {ok, list(), key() | '$TvsOver'} | {error, term()}
%%         Results: 遍历结果列表
%%         NextKey: 下一个键，'$TvsOver'表示遍历结束
-spec nif_traversal(EnvHandle :: env_handle(), DbiIndex :: integer(), StartKey :: key() | '$TvsBegin', BatchSize :: integer(), Direction :: integer(), ReturnType :: integer()) ->
	{ok, list(), key() | '$TvsOver'} | {error, term()}.
nif_traversal(_EnvHandle, _DbiIndex, _StartKey, _BatchSize, _Direction, _ReturnType) ->
	erlang:nif_error({not_loaded, [{module, ?MODULE}, {line, ?LINE}]}).

