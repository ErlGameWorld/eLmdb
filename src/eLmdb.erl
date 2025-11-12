
-module(eLmdb).

%% 导入LMDB常量定义
-include_lib("eLmdb/include/eLmdb.hrl").

-export([
	env_open/5,
	env_close/1,
	env_set_mapsize/2,
	env_info/1,
	env_stat/1,
	env_sync/1,
	env_copy/3,

	dbi_open/3,
	dbi_stat/2,
	dbi_flags/2,

	put/6,
	get/3,
	del/4,
	drop/2,
	
	get_multi/2,
	put_multi/4,
	del_multi/4,
	
	get_database_count/1,
	get_database_size/2,
	get_total_database_size/1,
	get_database_stats/2,
	writes/4
]).

%% 遍历辅助函数
-export([
	traversal_forward/4,        %% 正序遍历键值对
	traversal_reverse/4,        %% 反序遍历键值对
	traversal_keys_forward/4,   %% 正序遍历仅键
	traversal_keys_reverse/4,   %% 反序遍历仅键
	traversal_all/2,           %% 遍历整个数据库
	is_traversal_over/1         %% 检查遍历是否结束
]).

-type env_ref() :: reference().
-type dbi_ref() :: reference().
-type env_handle() :: reference().
-type key() :: binary() | integer() | atom().
-type value() :: binary() | integer() | atom().

%% @doc Open an LMDB environment with options.
-spec env_open(Path :: string(), MapsSize ::  integer(), MaxDbs :: integer(), MaxReaders :: integer(), Flags :: integer()) -> {ok, env_ref()} | {error, term()}.
env_open(Path, MapsSize, MaxDbs,  MaxReaders, Flags) ->
	%% eNifLmdb:nif_env_open requires 5 parameters: Path, MapsSize, MaxDbs, MaxReaders, Flags
	LPath = filename:absname(Path),
	ok = filelib:ensure_dir(filename:join([LPath, "xxx"])),
	eNifLmdb:nif_env_open(LPath, MapsSize, MaxDbs, MaxReaders, Flags).

%% @doc Close an LMDB environment.
-spec env_close(EnvRef :: env_ref()) -> ok | {error, term()}.
env_close(EnvRef) ->
	eNifLmdb:nif_env_close(EnvRef).

%% @doc Set the memory map size for an LMDB environment.
-spec env_set_mapsize(EnvRef :: env_ref(), MapSize :: integer()) -> ok | {error, term()}.
env_set_mapsize(EnvRef, MapSize) ->
    eNifLmdb:nif_env_set_mapsize(EnvRef, MapSize).

%% @doc Get environment information.
-spec env_info(EnvRef :: env_ref()) -> {ok, map()} | {error, term()}.
env_info(EnvRef) ->
 	eNifLmdb:nif_env_info(EnvRef).


%% @doc Get environment statistics.
-spec env_stat(EnvRef :: env_ref()) -> {ok, map()} | {error, term()}.
env_stat(EnvRef) ->
	eNifLmdb:nif_env_stat(EnvRef).

%% @doc Flush environment data to disk.
-spec env_sync(EnvRef :: env_ref()) -> ok | {error, term()}.
env_sync(EnvRef) ->
	eNifLmdb:nif_env_sync(EnvRef, 0).

%% @doc Copy environment to a file.
-spec env_copy(EnvRef :: env_ref(), Path :: string(), Flags :: integer()) -> ok | {error, term()}.
env_copy(EnvRef, Path, Flags) ->
	LPath = filename:absname(Path),
	ok = filelib:ensure_dir(filename:join([LPath, "xxx"])),
	eNifLmdb:nif_env_copy(EnvRef, LPath, Flags).

%% @doc Open a database in the environment.
-spec dbi_open(EnvRef :: env_ref(), DbName :: string(), Flags :: integer()) -> {ok, dbi_ref()} | {error, term()}.
dbi_open(EnvRef, DbName, Flags) ->
	%% eNifLmdb:nif_dbi_open requires 3 parameters: EnvRef, DbName, Flags
	%% For now, we'll use default database name
	eNifLmdb:nif_dbi_open(EnvRef, DbName, Flags) .

%% @doc Get database statistics.
-spec dbi_stat(EnvRef :: env_ref(), DbiRef :: dbi_ref()) -> {ok, map()} | {error, term()}.
dbi_stat(EnvRef, DbiRef) ->
	eNifLmdb:nif_dbi_stat(EnvRef, DbiRef).

%% @doc Get flags for a database.
-spec dbi_flags(EnvRef :: env_ref(), DbiRef :: dbi_ref()) -> {ok, integer()} | {error, term()}.
dbi_flags(EnvRef, DbiRef) ->
	eNifLmdb:nif_dbi_flags(EnvRef, DbiRef).

%% @doc Store a key/value pair in the database (write transaction).
-spec put(EnvRef :: env_ref(), DbiIndex :: integer(), Key :: key(), Value :: value(), Flags :: integer(), Mode :: integer()) -> ok | {ok, ok} | {error, term()} | {wait_write, integer()}.
put(EnvRef, DbiIndex, Key, Value, Flags, Mode) ->
	eNifLmdb:nif_put(EnvRef, DbiIndex, Key, Value, Flags, Mode).

%%%% @doc Retrieve a value by key from the database.
-spec get(EnvRef :: env_ref(), DbiIndex :: integer(),
	Key :: key()) -> {ok, value()} | {error, term()}.
get(EnvRef, DbiIndex, Key) ->
    eNifLmdb:nif_get(EnvRef, DbiIndex, Key).

%% @doc Delete a key/value pair from the database.
-spec del(EnvRef :: env_ref(), DbiIndex :: integer(),
	Key :: key(), Mode :: integer()) -> ok | {ok, ok} | {error, term()} | {wait_write, integer()} | notFound.
del(EnvRef, DbiIndex, Key, Mode) ->
    eNifLmdb:nif_del(EnvRef, DbiIndex, Key, Mode).

%% @doc Drop (delete) all data in the database.
-spec drop(EnvRef :: env_ref(), DbiIndex :: integer()) -> ok | {error, term()}.
drop(EnvRef, DbiIndex) ->
    %% C NIF 期望 3 个参数：Env、Dbi、Mode；默认为 1 表示删除数据库（非仅清空）
    DefaultMode = 1,
    eNifLmdb:nif_drop(EnvRef, DbiIndex, DefaultMode).

%% @doc Get multiple values for a key in a multi-value database.
-spec get_multi(EnvRef :: env_ref(), QueryList :: list()) -> {ok, list()} | {error, term()}.
get_multi(EnvRef, QueryList) ->
    eNifLmdb:nif_get_multi(EnvRef, QueryList).

%% @doc 遍历数据库
%% 遍历指定数据库中的数据，支持正序和反序遍历
%% @param EnvHandle 环境句柄
%% @param DbiIndex 数据库索引
%% @param StartKey 起始键（可选，可以是'$TvsBegin'表示从头开始）
%% @param BatchSize 批量大小（可选，默认100）
%% @param Direction 遍历方向（0-正序，1-反序）
%% @param ReturnType 返回类型（0-键值对，1-仅键）
%% @returns {ok, Results, NextKey} | {error, Reason}
-spec nif_traversal(EnvHandle :: env_handle(), DbiIndex :: integer(), StartKey :: key() | '$TvsBegin', BatchSize :: integer(), Direction :: integer(), ReturnType :: integer()) ->
	{ok, list(), key() | '$TvsOver'} | {error, term()}.
nif_traversal(EnvHandle, DbiIndex, StartKey, BatchSize, Direction, ReturnType) ->
	eNifLmdb:nif_traversal(EnvHandle, DbiIndex, StartKey, BatchSize, Direction, ReturnType).

%% @doc Put multiple values for a key in a multi-value database.
-spec put_multi(EnvRef :: env_ref(), OperationList :: list(), Mode :: integer(), SameTransaction :: integer()) ->
    ok | {error, term()}.
put_multi(EnvRef, OperationList, Mode, SameTransaction) ->
    eNifLmdb:nif_put_multi(EnvRef, OperationList, Mode, SameTransaction).

%% @doc Delete multiple values for a key in a multi-value database.
-spec del_multi(EnvRef :: env_ref(), OperationList :: list(), Mode :: integer(), SameTransaction :: integer()) ->
    ok | {ok, ok} | {error, term()} | {wait_write, integer()}.
del_multi(EnvRef, OperationList, Mode, SameTransaction) ->
    eNifLmdb:nif_del_multi(EnvRef, OperationList, Mode, SameTransaction).

%% @doc 批量写入操作（统一接口）
%% 使用统一的批量操作接口处理存储和删除操作
%% @param EnvRef 环境句柄
%% @param OperationList 操作列表，格式为[{Dbi, Key, Value}, {Dbi, Key} ...]
%% @param Mode 执行模式
%%          - 0: 异步无结果 - 发送到写进程，不等待结果
%%          - 1: 异步等待结果 - 发送到写进程，等待结果返回
%%          - 2: 同步执行 - 在当前NIF函数内直接执行
%% @param SameTransaction 事务模式
%%          - 0: 不同事务 - 每个操作使用独立事务
%%          - 1: 相同事务 - 所有操作使用同一个事务
%% @returns ok | {ok, ok} | {error, term()} | {wait_write, integer()}
-spec writes(EnvRef :: env_ref(), OperationList :: list(), Mode :: integer(), SameTransaction :: integer()) ->
    ok | {ok, ok} | {error, term()} | {wait_write, integer()}.
writes(EnvRef, OperationList, Mode, SameTransaction) ->
    eNifLmdb:nif_writes(EnvRef, OperationList, Mode, SameTransaction).

%%%==============================遍历辅助函数 start ====================================

%% 遍历方向常量
-define(TVS_FORWARD, 0).  %% 正序遍历
-define(TVS_REVERSE, 1).  %% 反序遍历

%% 返回类型常量
-define(RETURN_KEY_VALUE, 0).  %% 返回键值对
-define(RETURN_KEY_ONLY, 1).   %% 仅返回键

%% 默认批量大小
-define(DEFAULT_BATCH_SIZE, 100).

%% 最大批量大小
-define(MAX_BATCH_SIZE, 1000).

%% @doc 创建遍历标志
%% 根据方向和返回类型创建标志位
%% @param Direction 遍历方向（?TVS_FORWARD 或 ?TVS_REVERSE）
%% @param ReturnType 返回类型（?RETURN_KEY_VALUE 或 ?RETURN_KEY_ONLY）
%% @returns 标志位整数
-spec make_traversal_flags(Direction :: integer(), ReturnType :: integer()) -> integer().
make_traversal_flags(Direction, ReturnType) ->
	Direction bor (ReturnType bsl 1).

%% @doc 简化遍历函数 - 正序遍历键值对
%% @param EnvHandle 环境句柄
%% @param DbiIndex 数据库索引
%% @param StartKey 起始键（可选，'$TvsBegin'表示从头开始）
%% @param BatchSize 批量大小（可选，默认100）
%% @returns {ok, Results, NextKey} | {error, Reason}
-spec traversal_forward(EnvHandle :: env_handle(), DbiIndex :: integer(), StartKey :: key() | '$TvsBegin', BatchSize :: integer()) ->
	{ok, list(), key() | '$TvsOver'} | {error, term()}.
traversal_forward(EnvHandle, DbiIndex, StartKey, BatchSize) ->
	nif_traversal(EnvHandle, DbiIndex, StartKey, BatchSize, ?TVS_FORWARD, ?RETURN_KEY_VALUE).

%% @doc 简化遍历函数 - 反序遍历键值对
%% @param EnvHandle 环境句柄
%% @param DbiIndex 数据库索引
%% @param StartKey 起始键（可选，'$TvsBegin'表示从头开始）
%% @param BatchSize 批量大小（可选，默认100）
%% @returns {ok, Results, NextKey} | {error, Reason}
-spec traversal_reverse(EnvHandle :: env_handle(), DbiIndex :: integer(), StartKey :: key() | '$TvsBegin', BatchSize :: integer()) ->
	{ok, list(), key() | '$TvsOver'} | {error, term()}.
traversal_reverse(EnvHandle, DbiIndex, StartKey, BatchSize) ->
	nif_traversal(EnvHandle, DbiIndex, StartKey, BatchSize, ?TVS_REVERSE, ?RETURN_KEY_VALUE).

%% @doc 简化遍历函数 - 正序遍历仅键
%% @param EnvHandle 环境句柄
%% @param DbiIndex 数据库索引
%% @param StartKey 起始键（可选，'$TvsBegin'表示从头开始）
%% @param BatchSize 批量大小（可选，默认100）
%% @returns {ok, Keys, NextKey} | {error, Reason}
-spec traversal_keys_forward(EnvHandle :: env_handle(), DbiIndex :: integer(), StartKey :: key() | '$TvsBegin', BatchSize :: integer()) ->
	{ok, list(), key() | '$TvsOver'} | {error, term()}.
traversal_keys_forward(EnvHandle, DbiIndex, StartKey, BatchSize) ->
	nif_traversal(EnvHandle, DbiIndex, StartKey, BatchSize, ?TVS_FORWARD, ?RETURN_KEY_ONLY).

%% @doc 简化遍历函数 - 反序遍历仅键
%% @param EnvHandle 环境句柄
%% @param DbiIndex 数据库索引
%% @param StartKey 起始键（可选，'$TvsBegin'表示从头开始）
%% @param BatchSize 批量大小（可选，默认100）
%% @returns {ok, Keys, NextKey} | {error, Reason}
-spec traversal_keys_reverse(EnvHandle :: env_handle(), DbiIndex :: integer(), StartKey :: key() | '$TvsBegin', BatchSize :: integer()) ->
	{ok, list(), key() | '$TvsOver'} | {error, term()}.
traversal_keys_reverse(EnvHandle, DbiIndex, StartKey, BatchSize) ->
	nif_traversal(EnvHandle, DbiIndex, StartKey, BatchSize, ?TVS_REVERSE, ?RETURN_KEY_ONLY).

%% @doc 遍历整个数据库（正序键值对）
%% 使用默认批量大小遍历整个数据库
%% @param EnvHandle 环境句柄
%% @param DbiIndex 数据库索引
%% @returns {ok, AllResults} | {error, Reason}
-spec traversal_all(EnvHandle :: env_handle(), DbiIndex :: integer()) ->
	{ok, list()} | {error, term()}.
traversal_all(EnvHandle, DbiIndex) ->
	traversal_all(EnvHandle, DbiIndex, '$TvsBegin', []).

%% @private
%% 递归遍历整个数据库的辅助函数
traversal_all(EnvHandle, DbiIndex, StartKey, Acc) ->
	case traversal_forward(EnvHandle, DbiIndex, StartKey, ?DEFAULT_BATCH_SIZE) of
		{ok, Results, NextKey} when NextKey =/= '$TvsOver' ->
			traversal_all(EnvHandle, DbiIndex, NextKey, Acc ++ Results);
		{ok, Results, '$TvsOver'} ->
			{ok, Acc ++ Results};
		{error, Reason} ->
			{error, Reason}
	end.

%% @doc 检查遍历是否结束
%% @param NextKey 下一个键
%% @returns true（遍历结束）| false（还有更多数据）
-spec is_traversal_over(NextKey :: key() | '$TvsOver') -> boolean().
is_traversal_over('$TvsOver') -> true;
is_traversal_over(_) -> false.

%%%==============================遍历辅助函数 end ====================================

%%%==============================数据库信息查询函数 start ====================================

%% @doc 获取环境中打开的数据库数量
%% 注意：LMDB没有官方枚举DB的API，此函数通过尝试访问数据库句柄来统计数量
%% 该方法在某些情况下可能不准确，建议在应用层维护数据库注册表
%% @param EnvRef 环境句柄
%% @returns {ok, Count} | {error, Reason}
-spec get_database_count(EnvRef :: env_ref()) -> {ok, integer()} | {error, term()}.
get_database_count(EnvRef) ->
    get_database_count(EnvRef, 0, 0).

%% @private
%% 递归统计数据库数量
%% 最大尝试256个数据库句柄，避免无限循环
get_database_count(_EnvRef, Count, 256) ->
    {ok, Count};
get_database_count(EnvRef, Count, Index) ->
    case eNifLmdb:nif_dbi_stat(EnvRef, Index) of
        {ok, _DbiStat} ->
            %% 数据库存在，继续检查下一个
            get_database_count(EnvRef, Count + 1, Index + 1);
        {error, _Reason} ->
            %% 数据库不存在，检查是否还有更多
            case Index > Count of
                true ->
                    %% 没有更多数据库
                    {ok, Count};
                false ->
                    %% 继续检查下一个
                    get_database_count(EnvRef, Count, Index + 1)
            end
    end.

%% @doc 获取单个数据库的大小（字节）
%% 基于数据库统计信息计算实际占用空间
%% 计算公式：实际使用空间 = (分支页 + 叶子页 + 溢出页) × 页面大小
%% @param EnvRef 环境句柄
%% @param DbiRef 数据库句柄
%% @returns {ok, SizeInBytes} | {error, Reason}
-spec get_database_size(EnvRef :: env_ref(), DbiRef :: dbi_ref()) -> {ok, integer()} | {error, term()}.
get_database_size(EnvRef, DbiRef) ->
    case eNifLmdb:nif_dbi_stat(EnvRef, DbiRef) of
        {ok, DbiStat} ->
            %% 从dbiStat元组中提取页面大小和页面数量
            %% 格式：{dbiStat, PSize, Depth, BranchPages, LeafPages, OverflowPages, Entries}
            {dbiStat, PSize, _Depth, BranchPages, LeafPages, OverflowPages, _Entries} = DbiStat,
            
            %% 计算该数据库实际使用的页面总数
            TotalPages = BranchPages + LeafPages + OverflowPages,
            Size = TotalPages * PSize,
            {ok, Size};
        Error ->
            Error
    end.

%% @doc 获取整个数据库环境的总大小（字节）
%% 基于环境信息计算总占用空间
%% 计算公式：总使用空间 = (最后使用的页面ID + 1) × 页面大小
%% @param EnvRef 环境句柄
%% @returns {ok, TotalSizeInBytes} | {error, Reason}
-spec get_total_database_size(EnvRef :: env_ref()) -> {ok, integer()} | {error, term()}.
get_total_database_size(EnvRef) ->
    case env_info(EnvRef) of
        {ok, EnvInfo} ->
            %% 从envInfo记录中提取映射大小和最后页面ID
            #envInfo{me_mapsize = MapSize, me_last_pgno = LastPgno} = EnvInfo,
            
            %% 获取环境统计信息来获取页面大小
            case env_stat(EnvRef) of
                {ok, EnvStat} ->
                    %% 从envStat记录中提取页面大小
                    #envStat{ms_psize = PSize} = EnvStat,
                    
                    %% 计算总大小：实际使用空间 = (最后使用的页面ID + 1) × 页面大小
                    %% 同时返回映射大小作为参考
                    UsedSize = (LastPgno + 1) * PSize,
                    {ok, #{used_size => UsedSize, mapped_size => MapSize}};
                Error ->
                    Error
            end;
        Error ->
            Error
    end.

%% @doc 获取数据库详细统计信息
%% 返回数据库的详细统计信息，包括大小、条目数等
%% @param EnvRef 环境句柄
%% @param DbiRef 数据库句柄
%% @returns {ok, DetailedStats} | {error, Reason}
-spec get_database_stats(EnvRef :: env_ref(), DbiRef :: dbi_ref()) -> {ok, map()} | {error, term()}.
get_database_stats(EnvRef, DbiRef) ->
    case eNifLmdb:nif_dbi_stat(EnvRef, DbiRef) of
        {ok, DbiStat} ->
            {dbiStat, PSize, Depth, BranchPages, LeafPages, OverflowPages, Entries} = DbiStat,
            
            %% 计算该数据库的实际使用大小
            TotalPages = BranchPages + LeafPages + OverflowPages,
            UsedSize = TotalPages * PSize,
            
            %% 计算平均记录大小
            AvgRecordSize = case Entries of
                0 -> 0;
                _ -> UsedSize div Entries
            end,
            
            %% 计算索引比率
            IndexRatio = case TotalPages of
                0 -> 0.0;
                _ -> BranchPages / TotalPages
            end,
            
            Stats = #{
                page_size => PSize,
                tree_depth => Depth,
                branch_pages => BranchPages,
                leaf_pages => LeafPages,
                overflow_pages => OverflowPages,
                total_pages => TotalPages,
                total_entries => Entries,
                used_size_bytes => UsedSize,
                average_record_size => AvgRecordSize,
                index_ratio => IndexRatio,
                has_many_large_objects => (OverflowPages > (LeafPages * 0.1))
            },
            {ok, Stats};
        Error ->
            Error
    end.

%%%==============================数据库信息查询函数 end ====================================
