-module(tcLmdb).

-compile([export_all]).

-include("eLmdb.hrl").

%% 定义不同大小的二进制常量值
-define(SMALL_VALUE, <<"small_value">>).
-define(MEDIUM_VALUE, binary:copy(<<"x">>, 100)).
-define(LARGE_VALUE, binary:copy(<<"x">>, 1000)).
-define(XLARGE_VALUE, binary:copy(<<"x">>, 10000)).

%% 默认选项打开环境
doe() ->
	Flags = 0,
	{ok, Ref} = eLmdb:envOpen("./dbtest", 5 * 1024 * 1024 * 1024, 10, 126, Flags),
	Ref.

%% 最快速选项打开环境 可接受崩溃丢失数据
foe() ->
	Flags = ?MDB_NOSYNC bor ?MDB_NOMETASYNC bor ?MDB_WRITEMAP bor ?MDB_MAPASYNC bor ?MDB_NOMEMINIT bor ?MDB_NORDAHEAD,
	{ok, Ref} = eLmdb:envOpen("./dbtest", 5 * 1024 * 1024 * 1024, 10, 126, Flags),
	Ref.

%% 平衡配置（生产推荐）
poe() ->
	Flags = ?MDB_NOMETASYNC bor ?MDB_WRITEMAP bor ?MDB_NORDAHEAD,
	{ok, Ref} = eLmdb:envOpen("./dbtest", 5 * 1024 * 1024 * 1024, 10, 126, Flags),
	Ref.

od() ->
	Ref = foe(),
	{ok, Dbi} = eLmdb:dbOpen(Ref, "testdb", 16#40000),
	{Ref, Dbi}.

od(Ref) ->
	{ok, Dbi} = eLmdb:dbOpen(Ref, "testdb", 16#40000),
	Dbi.

put(Ref, Dbi, Key, Value) ->
	eLmdb:put(Ref, Dbi, Key, Value, 0, 2).

get(Ref, Dbi, Key) ->
	eLmdb:get(Ref, Dbi, Key).

close(Ref, _Dbi) ->
	eLmdb:envSync(Ref),
	eLmdb:envClose(Ref),
	%% 删除数据库文件
	delete_database_files("./dbtest").

%% 删除指定路径的数据库文件
delete_database_files(DbPath) ->
	DataFile = DbPath ++ "/data.mdb",
	LockFile = DbPath ++ "/lock.mdb",
	
	%% 删除数据文件
	case file:delete(DataFile) of
		ok -> ok;
		{error, enoent} -> ok;  %% 文件不存在也没关系
		DataError -> io:format("警告: 删除数据文件失败: ~p~n", [DataError])
	end,
	
	%% 删除锁文件
	case file:delete(LockFile) of
		ok -> ok;
		{error, enoent} -> ok;  %% 文件不存在也没关系
		LockError -> io:format("警告: 删除锁文件失败: ~p~n", [LockError])
	end,
	case file:del_dir(DbPath) of
		ok ->
			ok;
		{error, Reason} ->
			io:format("删除失败: ~p~n", [Reason]),
			{error, Reason}
	end.

ti(Cnt) ->
	Ref = foe(),
	Dbi = od(Ref),
	ti(Cnt, Ref, Dbi),
	close(Ref, Dbi).

ti(0, Ref, Dbi) ->
	io:format("IMY**************dbi_stat ~p~n", [eLmdb:dbStat(Ref, Dbi)]),
	io:format("IMY**************env_stat ~p~n", [eLmdb:envStat(Ref)]),
	io:format("IMY**************env_info ~p~n", [eLmdb:envInfo(Ref)]),
	eLmdb:envSync(Ref),
	timer:sleep(10000),
	ok;
ti(Cnt, Ref, Dbi) ->
	ok = eLmdb:put(Ref, Dbi, Cnt, Cnt, 0, 2),
	ti(Cnt - 1, Ref, Dbi).

tf(Type) ->
	tf(Type, 1).

tf(Type, ProcessCnt) ->
	Ref = case Type of
			  d ->
				  doe();
			  f ->
				  foe();
			  p ->
				  poe()
		  end,
	Dbi = od(Ref),
	utTc:tc(ProcessCnt, 2000, ?MODULE, put, [Ref, Dbi, 1, 2]),
	close(Ref, Dbi).

%% ==================== 批量操作性能测试 ====================
%% 生成批量操作参数
generate_batch_args({BatchSize, Ref, Dbi}) ->
	%% 生成测试数据
	Operations = [{Dbi, erlang:unique_integer([positive, monotonic]), ?MEDIUM_VALUE} || _O <- lists:seq(1, BatchSize)],
	[Ref, Operations, ?DbTxnSame].

%% 测试不同批量大小对性能的影响
batch_performance_test() ->
	batch_performance_test(1, 1000).

batch_performance_test(ProcessCnt, LoopCnt) ->
	io:format("=== 批量操作性能测试 (进程数: ~p, 循环次数: ~p) ===~n", [ProcessCnt, LoopCnt]),
	
	EnvList = [{"最快速选项", fun foe/0}, {"平衡配置", fun poe/0}, {"默认选项", fun doe/0}],
	BatchSizes = [1, 10, 100, 1000, 5000],
	Functions = [{"同步等待", writes}, {"同步", writesSync}, {"异步", writesAsync}],
	
	[
		begin
			io:format("批量操作性能测试: ~ts ~p ~ts ---~n", [EnvFlag, BatchSize, Mode]),
			Ref = OpenFunc(),
			Dbi = od(Ref),
			%% 使用utTc测试批量写入性能
			Result = utTc:tc(ProcessCnt, LoopCnt, eLmdb, TestFun, {fun ?MODULE:generate_batch_args/1, [{BatchSize, Ref, Dbi}]}),
			io:format("结果: ~p~n", [Result]),
			close(Ref, Dbi),
			ok
		end
		|| {EnvFlag, OpenFunc} <- EnvList, BatchSize <- BatchSizes, {Mode, TestFun} <- Functions
	],
	io:format("=== 批量操作性能测试完成 ===~n").

%% ==================== 并发写入性能测试 ====================
%% 生成并发写入参数
generate_concurrent_write_args({Ref, Dbi}) ->
	Key = erlang:unique_integer([positive, monotonic]),
	Value = ?MEDIUM_VALUE,
	[Ref, Dbi, Key, Value, 0].

%% 测试并发写入性能
concurrency_write_performance_test() ->
	concurrency_write_performance_test(1000).

concurrency_write_performance_test(LoopCnt) ->
	io:format("=== 并发写入性能测试 (循环次数: ~p) ===~n", [LoopCnt]),
	
	EnvList = [{"最快速选项", fun foe/0}, {"平衡配置", fun poe/0}, {"默认选项", fun doe/0}],
	ProcessCounts = [1, 2, 4, 8, 16, 32, 64, 100],
	Functions = [{"同步等待", put}, {"同步", putSync}, {"异步", putAsync}],
	
	[
		begin
			io:format("并发写入性能测试: ~ts 进程数: ~p ~ts调用方式 ---~n", [EnvFlag, ProcessCnt, Mode]),
			Ref = OpenFunc(),
			Dbi = od(Ref),
			Result = utTc:tc(ProcessCnt, LoopCnt, eLmdb, TestFun, {fun ?MODULE:generate_concurrent_write_args/1, [{Ref, Dbi}]}),
			io:format("结果: ~p~n", [Result]),
			close(Ref, Dbi),
			ok
		end
		|| {EnvFlag, OpenFunc} <- EnvList, ProcessCnt <- ProcessCounts, {Mode, TestFun} <- Functions
	],
	io:format("=== 并发写入性能测试完成 ===~n").

%% ==================== 随机并发写入性能测试 ====================
%% 生成随机并发写入参数
generate_concurrent_rand_write_args({Ref, Dbi}) ->
	Key = rand:uniform(100000),
	Value = ?MEDIUM_VALUE,
	[Ref, Dbi, Key, Value, 0].

%% 测试随机并发写入性能
concurrency_write_rand_performance_test() ->
	concurrency_write_rand_performance_test(1000).

concurrency_write_rand_performance_test(LoopCnt) ->
	io:format("=== 随机并发写入性能测试 (循环次数: ~p) ===~n", [LoopCnt]),
	
	EnvList = [{"最快速选项", fun foe/0}, {"平衡配置", fun poe/0}, {"默认选项", fun doe/0}],
	ProcessCounts = [1, 2, 4, 8, 16, 32, 64, 100],
	Functions = [{"同步等待", put}, {"同步", putSync}, {"异步", putAsync}],
	
	[
		begin
			io:format("随机并发写入性能测试: ~ts 进程数: ~p ~ts调用方式 ---~n", [EnvFlag, ProcessCnt, Mode]),
			Ref = OpenFunc(),
			Dbi = od(Ref),
			Result = utTc:tc(ProcessCnt, LoopCnt, eLmdb, TestFun, {fun ?MODULE:generate_concurrent_write_args/1, [{Ref, Dbi}]}),
			io:format("结果: ~p~n", [Result]),
			close(Ref, Dbi),
			ok
		end
		|| {EnvFlag, OpenFunc} <- EnvList, ProcessCnt <- ProcessCounts, {Mode, TestFun} <- Functions
	],
	io:format("=== 随机并发写入性能测试完成 ===~n").

%% 生成并发读取参数
generate_concurrent_read_args({Ref, Dbi}) ->
	Key = rand:uniform(10000),
	[Ref, Dbi, Key].

%% 测试并发读取性能
concurrent_read_performance_test() ->
	concurrent_read_performance_test(1000).

concurrent_read_performance_test(LoopCnt) ->
	io:format("=== 并发读取性能测试 (循环次数: ~p) ===~n", [LoopCnt]),
	
	EnvList = [{"最快速选项", fun foe/0}, {"平衡配置", fun poe/0}, {"默认选项", fun doe/0}],
	ProcessCounts = [1, 2, 4, 8, 16, 32, 64, 100],
	
	[
		begin
			io:format("并发读取性能测试: ~ts 进程数: ~p ---~n", [ModeName, ProcessCnt]),
			Ref = OpenFunc(),
			Dbi = od(Ref),
			
			%% 预先插入测试数据，供读取测试使用
			PreInsertData = lists:map(fun(I) -> {Dbi, I, ?MEDIUM_VALUE} end, lists:seq(1, 10000)),
			ok = eLmdb:writes(Ref, PreInsertData),
			
			Result = utTc:tc(ProcessCnt, LoopCnt, eLmdb, get, {fun ?MODULE:generate_concurrent_read_args/1, [{Ref, Dbi}]}),
			io:format("结果: ~p~n", [Result]),
			close(Ref, Dbi),
			ok
		end
		|| {ModeName, OpenFunc} <- EnvList, ProcessCnt <- ProcessCounts
	],
	io:format("=== 并发读取性能测试完成 ===~n").

%% ==================== 内存映射大小影响测试 ====================
%% ==================== 内存映射大小影响测试 ====================
%% 根据模式名称获取对应的flags
get_flags_by_mode("默认选项") -> 0;
get_flags_by_mode("最快速选项") ->
	?MDB_NOSYNC bor ?MDB_NOMETASYNC bor ?MDB_WRITEMAP bor ?MDB_MAPASYNC bor ?MDB_NOMEMINIT bor ?MDB_NORDAHEAD;
get_flags_by_mode("平衡配置") ->
	?MDB_NOMETASYNC bor ?MDB_WRITEMAP bor ?MDB_NORDAHEAD.
%% 生成内存映射大小写入参数
generate_mapsize_write_args({Ref, Dbi}) ->
	Key = erlang:unique_integer([positive, monotonic]),
	Value = ?MEDIUM_VALUE,
	[Ref, Dbi, Key, Value, 0].

%% 测试内存映射大小对性能的影响
mapsize_performance_test() ->
	mapsize_performance_test(1, 1000).

mapsize_performance_test(ProcessCnt, LoopCnt) ->
	io:format("=== 内存映射大小性能测试 (循环次数: ~p) ===~n", [LoopCnt]),
	EnvList = [{"最快速选项", fun foe/0}, {"平衡配置", fun poe/0}, {"默认选项", fun doe/0}],
	MapSizes = [
		{small, "小映射", 1024 * 1024 * 10},  %% 10MB
		{medium, "中映射", 1024 * 1024 * 100}, %% 100MB
		{large, "大映射", 1024 * 1024 * 1024}  %% 1GB
	],
	
	[
		begin
			io:format("内存映射大小性能测试: ~ts ~ts (~p MB) ---~n", [ModeName, SizeName, MapSize div (1024 * 1024)]),
			%% 为每个映射大小创建独立的数据库路径
			DbPath = "./mapsize_test_" ++ atom_to_list(SizeType),
			
			%% 创建特定大小的环境
			{ok, Ref} = eLmdb:envOpen(DbPath, MapSize, 10, 126, get_flags_by_mode(ModeName)),
			{ok, Dbi} = eLmdb:dbOpen(Ref, "mapsize_db", 16#40000),
			WriteResult = utTc:tc(ProcessCnt, LoopCnt, eLmdb, put, {fun ?MODULE:generate_mapsize_write_args/1, [{Ref, Dbi}]}),
			io:format("   写入性能: ~p~n", [WriteResult]),
			%% 关闭并删除数据库
			eLmdb:envClose(Ref),
			delete_database_files(DbPath),
			ok
		end
		|| {ModeName, _OpenFunc} <- EnvList, {SizeType, SizeName, MapSize} <- MapSizes
	],
	io:format("=== 内存映射大小性能测试完成 ===~n").

%% ==================== 数据大小影响测试 ====================
%% 生成数据大小写入参数
generate_data_size_write_args({Ref, Dbi, Value}) ->
	Key = erlang:unique_integer([positive, monotonic]),
	[Ref, Dbi, Key, Value, 0].
%% 测试不同数据大小对性能的影响
data_size_performance_test() ->
	data_size_performance_test(1, 1000).

data_size_performance_test(ProcessCnt, LoopCnt) ->
	io:format("=== 数据大小性能测试 (循环次数: ~p) ===~n", [LoopCnt]),
	EnvList = [{"最快速选项", fun foe/0}, {"平衡配置", fun poe/0}, {"默认选项", fun doe/0}],
	DataSizes = [
		{small, ?SMALL_VALUE},
		{medium, ?MEDIUM_VALUE},
		{large, ?LARGE_VALUE},
		{xlarge, ?XLARGE_VALUE}
	],
	
	[
		begin
			io:format("数据大小性能测试: ~ts (大小类型:~p) ---~n", [ModeName, SizeType]),
			Ref = OpenFunc(),
			Dbi = od(Ref),
			
			WriteResult = utTc:tc(ProcessCnt, LoopCnt, eLmdb, put, {fun ?MODULE:generate_data_size_write_args/1, [{Ref, Dbi, Value}]}),
			io:format("   写入性能: ~p~n", [WriteResult]),
			
			%% 预先插入测试数据，供读取测试使用
			PreInsertData = lists:map(fun(I) -> {Dbi, I, Value} end, lists:seq(1, 10000)),
			ok = eLmdb:writes(Ref, PreInsertData),
			
			ReadResult = utTc:tc(ProcessCnt, LoopCnt, eLmdb, get, {fun ?MODULE:generate_concurrent_read_args/1, [{Ref, Dbi}]}),
			io:format("   读取性能: ~p~n", [ReadResult]),
			close(Ref, Dbi),
			ok
		end
		|| {ModeName, OpenFunc} <- EnvList, {SizeType, Value} <- DataSizes
	],
	io:format("=== 数据大小性能测试完成 ===~n").

%% ==================== 遍历操作性能测试 ====================
%% 生成遍历参数
generate_traversal_params({Ref, Dbi}) ->
	[Ref, Dbi, '$TvsBegin', 1000].
%% 测试遍历操作的性能
traversal_performance_test() ->
	traversal_performance_test(1, 1000).

traversal_performance_test(ProcessCnt, LoopCnt) ->
	io:format("=== 遍历操作性能测试 (循环次数: ~p) ===~n", [LoopCnt]),
	
	EnvList = [{"最快速选项", fun foe/0}, {"平衡配置", fun poe/0}, {"默认选项", fun doe/0}],
	
	[
		begin
			io:format("遍历操作性能测试: ~ts 进程数: ~p ---~n", [ModeName, ProcessCnt]),
			Ref = OpenFunc(),
			Dbi = od(Ref),
			PreInsertData = lists:map(fun(I) -> {Dbi, I, ?MEDIUM_VALUE} end, lists:seq(1, 10000)),
			ok = eLmdb:writes(Ref, PreInsertData, ?DbTxnSame),
			
			%% 测试单进程遍历性能
			io:format("   [~ts] 单进程遍历测试:~n", [ModeName]),
			AscResult = utTc:tc(1, LoopCnt, eLmdb, tvsAsc, {fun generate_traversal_params/1, [{Ref, Dbi}]}),
			io:format("   正序遍历性能: ~p~n", [AscResult]),
			DescResult = utTc:tc(1, LoopCnt, eLmdb, tvsDesc, {fun generate_traversal_params/1, [{Ref, Dbi}]}),
			io:format("   反序遍历性能: ~p~n", [DescResult]),
			
			io:format("   [~ts] 多进程遍历测试 (进程数: ~p):~n", [ModeName, ProcessCnt]),
			AscResult = utTc:tc(ProcessCnt, LoopCnt, eLmdb, tvsAsc, {fun generate_traversal_params/1, [{Ref, Dbi}]}),
			io:format("   正序遍历性能: ~p~n", [AscResult]),
			DescResult = utTc:tc(ProcessCnt, LoopCnt, eLmdb, tvsDesc, {fun generate_traversal_params/1, [{Ref, Dbi}]}),
			io:format("   反序遍历性能: ~p~n", [DescResult]),
			close(Ref, Dbi),
			ok
		end
		|| {ModeName, OpenFunc} <- EnvList
	],
	io:format("=== 遍历操作性能测试完成 ===~n").

%% ==================== 综合性能测试 ====================

%% 运行所有性能测试
main() ->
	main(false, 1, 1000).

main(FileName, ProcessCnt, LoopCnt) ->
	%% 提示：
	%%在重定向期间新创建的子进程会继承当前进程的 group_leader，也会写到文件。
	%%恢复顺序：先恢复 group_leader，再关文件。
	{GL, Fd} =
		case FileName of
			"" ->
				{undefined, undefined};
			_ ->
				GL0 = group_leader(),                              % 记住原来的 group leader（通常是 shell）
				{ok, F} = file:open(FileName, [append, {encoding, utf8}]),
				group_leader(F, self()),                           % 重定向
				{GL0, F}
		end,
	
	try
		io:format("========================================~n"),
		io:format("        eLmdb 综合性能测试套件         ~n"),
		io:format("========================================~n~n"),
		
		Tests = [
			{batch_performance_test, "批量操作性能测试"},
			{concurrency_write_performance_test, "并发写性能测试"},
			{concurrency_write_rand_performance_test, "随机并发写性能测试"},
			{concurrent_read_performance_test, "并发读性能测试"},
			{mapsize_performance_test, "内存映射大小影响测试"},
			{data_size_performance_test, "数据大小影响测试"},
			{traversal_performance_test, "遍历操作性能测试"}
		],
		
		lists:foreach(
			fun({TestFun, TestName}) ->
				io:format("运行测试: ~ts~n", [TestName]),
				try
					case TestFun of
						batch_performance_test ->
							tcLmdb:batch_performance_test(ProcessCnt, LoopCnt);
						concurrency_write_performance_test ->
							tcLmdb:concurrency_write_performance_test(LoopCnt);
						concurrency_write_rand_performance_test ->
							tcLmdb:concurrency_write_rand_performance_test(LoopCnt);
						concurrent_read_performance_test ->
							tcLmdb:concurrent_read_performance_test(LoopCnt);
						mapsize_performance_test ->
							tcLmdb:mapsize_performance_test(ProcessCnt, LoopCnt);
						data_size_performance_test ->
							tcLmdb:data_size_performance_test(ProcessCnt, LoopCnt);
						traversal_performance_test ->
							tcLmdb:traversal_performance_test(ProcessCnt, LoopCnt)
					end,
					io:format("✓ ~ts 完成~n~n", [TestName])
				catch
					Error:Reason ->
						io:format("✗ ~ts 失败: ~p:~p~n~n", [TestName, Error, Reason])
				end
			end,
			Tests),
		
		io:format("========================================~n"),
		io:format("       所有性能测试运行完成           ~n"),
		io:format("========================================~n")
	after
		case FileName of
			"" ->
				ignore;
			_ ->
				group_leader(GL, self()),
				file:close(Fd)
		end
	end.