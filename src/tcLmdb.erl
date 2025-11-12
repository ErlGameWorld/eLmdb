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
	{ok, Ref} = eLmdb:env_open("./dbtest", 5 * 1024 * 1024 * 1024, 10, 126, Flags),
	Ref.

%% 最快速选项打开环境 可接受崩溃丢失数据
foe() ->
	Flags = ?MDB_NOSYNC bor ?MDB_NOMETASYNC bor ?MDB_WRITEMAP bor ?MDB_MAPASYNC bor ?MDB_NOMEMINIT bor ?MDB_NORDAHEAD,
	{ok, Ref} = eLmdb:env_open("./dbtest", 5 * 1024 * 1024 * 1024, 10, 126, Flags),
	Ref.

%% 平衡配置（生产推荐）
poe() ->
	Flags = ?MDB_NOMETASYNC bor ?MDB_WRITEMAP bor ?MDB_NORDAHEAD,
	{ok, Ref} = eLmdb:env_open("./dbtest", 5 * 1024 * 1024 * 1024, 10, 126, Flags),
	Ref.

od() ->
	Ref = foe(),
	{ok, Dbi} = eLmdb:dbi_open(Ref, "testdb", 16#40000),
	{Ref, Dbi}.

od(Ref) ->
	{ok, Dbi} = eLmdb:dbi_open(Ref, "testdb", 16#40000),
	Dbi.

put(Ref, Dbi, Key, Value) ->
	eLmdb:put(Ref, Dbi, Key, Value, 0, 2).

get(Ref, Dbi, Key) ->
	eLmdb:get(Ref, Dbi, Key).

close(Ref, _Dbi) ->
	eLmdb:env_sync(Ref),
	eLmdb:env_close(Ref),
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
    end.

ti(Cnt) ->
	Ref = foe(),
	Dbi = od(Ref),
	ti(Cnt, Ref, Dbi),
	close(Ref, Dbi).

ti(0, Ref, Dbi) ->
	io:format("IMY**************dbi_stat ~p~n", [eLmdb:dbi_stat(Ref, Dbi)]),
	io:format("IMY**************env_stat ~p~n", [eLmdb:env_stat(Ref)]),
	io:format("IMY**************env_info ~p~n", [eLmdb:env_info(Ref)]),
	eLmdb:env_sync(Ref),
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

%% 测试不同批量大小对性能的影响
batch_performance_test() ->
    batch_performance_test(1, 1000).

batch_performance_test(ProcessCnt, LoopCnt) ->
    io:format("=== 批量操作性能测试 (进程数: ~p, 循环次数: ~p) ===~n", [ProcessCnt, LoopCnt]),
    
    %% 测试三种不同flags模式
    lists:foreach(fun({ModeName, OpenFunc}) ->
        io:format("~n--- ~ts模式测试 ---~n", [ModeName]),
        Ref = OpenFunc(),
        Dbi = od(Ref),
        
        BatchSizes = [1, 10, 100, 1000, 5000],
        
        lists:foreach(fun(BatchSize) ->
            io:format("   [~ts] 批量大小: ~p 条~n", [ModeName, BatchSize]),
            
            %% 生成测试数据
            Operations = lists:map(fun(I) ->
                %% 使用数字作为键，预定义常量值
                Key = I,
                Value = ?MEDIUM_VALUE,
                {Dbi, Key, Value}
            end, lists:seq(1, BatchSize)),
            
            %% 使用utTc测试批量写入性能
            Result = utTc:tc(ProcessCnt, LoopCnt, eLmdb, writes, [Ref, Operations, 2, 1]),
            io:format("   结果: ~p~n", [Result])
        end, BatchSizes),
        
        close(Ref, Dbi)
    end, [
        {"最快速选项", fun foe/0},
        {"平衡配置", fun poe/0},
        {"默认选项", fun doe/0}
    ]),
    
    io:format("=== 批量操作性能测试完成 ===~n").

%% ==================== 并发读写性能测试 ====================

%% 测试多进程并发访问LMDB的性能
concurrency_performance_test() ->
    concurrency_performance_test(1000).

concurrency_performance_test(LoopCnt) ->
    io:format("=== 并发读写性能测试 (循环次数: ~p) ===~n", [LoopCnt]),
    
    ProcessCounts = [1, 2, 4, 8, 16, 32, 64, 100],
    
    %% 测试三种不同flags模式
    lists:foreach(fun({ModeName, OpenFunc}) ->
        io:format("~n--- ~ts模式测试 ---~n", [ModeName]),
        
        lists:foreach(fun(ProcessCnt) ->
            io:format("   [~ts] 进程数: ~p~n", [ModeName, ProcessCnt]),
            
            Ref = OpenFunc(),
            Dbi = od(Ref),
            
            %% 测试并发写入性能
            WriteResult = utTc:tc(ProcessCnt, LoopCnt, ?MODULE, concurrent_write, [Ref, Dbi]),
            io:format("   并发写入性能: ~p~n", [WriteResult]),
            
            %% 测试并发读取性能
            ReadResult = utTc:tc(ProcessCnt, LoopCnt, ?MODULE, concurrent_read, [Ref, Dbi]),
            io:format("   并发读取性能: ~p~n", [ReadResult]),
            
            close(Ref, Dbi)
        end, ProcessCounts)
    end, [
        {"最快速选项", fun foe/0},
        {"平衡配置", fun poe/0},
        {"默认选项", fun doe/0}
    ]),
    
    io:format("=== 并发读写性能测试完成 ===~n").

%% 并发写入测试函数
concurrent_write(Ref, Dbi) ->
    %% 获取当前进程ID作为键
    ProcessId = erlang:system_info(process_count),
    Key = ProcessId,
    %% 使用预定义的常量值
    Value = ?MEDIUM_VALUE,
    eLmdb:put(Ref, Dbi, Key, Value, 0, 2).

%% 并发读取测试函数
concurrent_read(Ref, Dbi) ->
    %% 获取当前进程ID作为键
    ProcessId = erlang:system_info(process_count),
    Key = ProcessId,
    eLmdb:get(Ref, Dbi, Key).

%% ==================== 内存映射大小影响测试 ====================

%% 测试不同内存映射大小对性能的影响
mapsize_performance_test() ->
    mapsize_performance_test(1, 1000).

mapsize_performance_test(ProcessCnt, LoopCnt) ->
    io:format("=== 内存映射大小影响测试 (进程数: ~p, 循环次数: ~p) ===~n", [ProcessCnt, LoopCnt]),
    
    MapSizes = [
        {small, 100 * 1024 * 1024},      %% 100MB
        {medium, 1 * 1024 * 1024 * 1024}, %% 1GB
        {large, 5 * 1024 * 1024 * 1024}   %% 5GB
    ],
    
    %% 测试三种不同flags模式
    lists:foreach(fun({ModeName, _OpenFunc}) ->
        io:format("~n--- ~ts模式测试 ---~n", [ModeName]),
        
        lists:foreach(fun({SizeLabel, MapSize}) ->
            io:format("   [~ts] 映射大小: ~p (~pMB)~n", [ModeName, SizeLabel, MapSize div (1024 * 1024)]),
            
            %% 为每个映射大小创建独立的数据库路径
            DbPath = "./mapsize_test_" ++ atom_to_list(SizeLabel),
            
            %% 创建特定大小的环境
            {ok, Ref} = eLmdb:env_open(DbPath, MapSize, 10, 126, get_flags_by_mode(ModeName)),
            {ok, Dbi} = eLmdb:dbi_open(Ref, "mapsize_db", 16#40000),
            
            %% 测试写入性能
            Result = utTc:tc(ProcessCnt, LoopCnt, eLmdb, put, [Ref, Dbi, 1, ?MEDIUM_VALUE, 0, 2]),
            io:format("   写入性能: ~p~n", [Result]),
            
            %% 关闭并删除数据库
            eLmdb:env_close(Ref),
            delete_database_files(DbPath)
        end, MapSizes)
    end, [
        {"最快速选项", fun foe/0},
        {"平衡配置", fun poe/0},
        {"默认选项", fun doe/0}
    ]),
    
    io:format("=== 内存映射大小影响测试完成 ===~n").

%% 根据模式名称获取对应的flags
get_flags_by_mode("默认选项") -> 0;
get_flags_by_mode("最快速选项") -> 
    ?MDB_NOSYNC bor ?MDB_NOMETASYNC bor ?MDB_WRITEMAP bor ?MDB_MAPASYNC bor ?MDB_NOMEMINIT bor ?MDB_NORDAHEAD;
get_flags_by_mode("平衡配置") -> 
    ?MDB_NOMETASYNC bor ?MDB_WRITEMAP bor ?MDB_NORDAHEAD.

%% ==================== 数据大小影响测试 ====================

%% 测试不同键值大小对性能的影响
data_size_performance_test() ->
    data_size_performance_test(1, 1000).

data_size_performance_test(ProcessCnt, LoopCnt) ->
    io:format("=== 数据大小影响测试 (进程数: ~p, 循环次数: ~p) ===~n", [ProcessCnt, LoopCnt]),
    
    %% 测试三种不同flags模式
    lists:foreach(fun({ModeName, OpenFunc}) ->
        io:format("~n--- ~ts模式测试 ---~n", [ModeName]),
        
        Ref = OpenFunc(),
        Dbi = od(Ref),
        
        lists:foreach(fun({SizeLabel, Value}) ->
            io:format("   [~ts] 数据大小: ~p (~p字节)~n", [ModeName, SizeLabel, byte_size(Value)]),
            
            Key = 1,
            
            %% 测试写入性能
            Result = utTc:tc(ProcessCnt, LoopCnt, eLmdb, put, [Ref, Dbi, Key, Value, 0, 2]),
            io:format("   写入性能: ~p~n", [Result])
        end, [{small, ?SMALL_VALUE}, {medium, ?MEDIUM_VALUE}, {large, ?LARGE_VALUE}, {xlarge, ?XLARGE_VALUE}]),
        
        close(Ref, Dbi)
    end, [
        {"最快速选项", fun foe/0},
        {"平衡配置", fun poe/0},
        {"默认选项", fun doe/0}
    ]),
    
    io:format("=== 数据大小影响测试完成 ===~n").

%% ==================== 遍历操作性能测试 ====================

%% 测试正序、反序遍历的性能差异
traversal_performance_test() ->
    traversal_performance_test(1, 100).

traversal_performance_test(ProcessCnt, LoopCnt) ->
    io:format("=== 遍历操作性能测试 (进程数: ~p, 循环次数: ~p) ===~n", [ProcessCnt, LoopCnt]),
    
    %% 测试三种不同flags模式
    lists:foreach(fun({ModeName, OpenFunc}) ->
        io:format("~n--- ~ts模式测试 ---~n", [ModeName]),
        
        Ref = OpenFunc(),
        Dbi = od(Ref),
        
        %% 先插入测试数据
        TestData = lists:map(fun(I) ->
            %% 使用数字作为键，预定义常量值
            Key = I,
            Value = ?MEDIUM_VALUE,
            {Dbi, Key, Value}
        end, lists:seq(1, 1000)),
        
        ok = eLmdb:writes(Ref, TestData, 2, 1),
        
        %% 测试单进程遍历性能
        io:format("   [~ts] 单进程遍历测试:~n", [ModeName]),
        ForwardResult = utTc:tc(1, LoopCnt, eLmdb, traversal_forward, [Ref, Dbi, '$TvsBegin', 100]),
        io:format("   正序遍历性能: ~p~n", [ForwardResult]),
        
        ReverseResult = utTc:tc(1, LoopCnt, eLmdb, traversal_reverse, [Ref, Dbi, '$TvsBegin', 100]),
        io:format("   反序遍历性能: ~p~n", [ReverseResult]),
        
        %% 测试多进程遍历性能
        io:format("   [~ts] 多进程遍历测试 (进程数: ~p):~n", [ModeName, ProcessCnt]),
        MultiForwardResult = utTc:tc(ProcessCnt, LoopCnt, ?MODULE, concurrent_traversal_forward, [Ref, Dbi]),
        io:format("   多进程正序遍历性能: ~p~n", [MultiForwardResult]),
        
        MultiReverseResult = utTc:tc(ProcessCnt, LoopCnt, ?MODULE, concurrent_traversal_reverse, [Ref, Dbi]),
        io:format("   多进程反序遍历性能: ~p~n", [MultiReverseResult]),
        
        close(Ref, Dbi)
    end, [
        {"最快速选项", fun foe/0},
        {"平衡配置", fun poe/0},
        {"默认选项", fun doe/0}
    ]),
    
    io:format("=== 遍历操作性能测试完成 ===~n").

%% 并发正序遍历测试函数
concurrent_traversal_forward(Ref, Dbi) ->
    eLmdb:traversal_forward(Ref, Dbi, '$TvsBegin', 100).

%% 并发反序遍历测试函数
concurrent_traversal_reverse(Ref, Dbi) ->
    eLmdb:traversal_reverse(Ref, Dbi, '$TvsBegin', 100).

%% ==================== 事务模式性能测试 ====================

%% 测试同步/异步模式的性能差异
transaction_mode_performance_test() ->
    transaction_mode_performance_test(1, 1000).

transaction_mode_performance_test(ProcessCnt, LoopCnt) ->
    io:format("=== 事务模式性能测试 (进程数: ~p, 循环次数: ~p) ===~n", [ProcessCnt, LoopCnt]),
    
    Modes = [
        {sync, 2},      %% 同步模式
        {async, 0}      %% 异步模式
    ],
    
    %% 测试三种不同flags模式
    lists:foreach(fun({ModeName, OpenFunc}) ->
        io:format("~n--- ~ts模式测试 ---~n", [ModeName]),
        
        Ref = OpenFunc(),
        Dbi = od(Ref),
        
        lists:foreach(fun({ModeLabel, Mode}) ->
            io:format("   [~ts] 事务模式: ~p~n", [ModeName, ModeLabel]),
            
            %% 测试写入性能
            Result = utTc:tc(ProcessCnt, LoopCnt, eLmdb, put, [Ref, Dbi, 1, ?MEDIUM_VALUE, 0, Mode]),
            io:format("   性能结果: ~p~n", [Result])
        end, Modes),
        
        close(Ref, Dbi)
    end, [
        {"最快速选项", fun foe/0},
        {"平衡配置", fun poe/0},
        {"默认选项", fun doe/0}
    ]),
    
    io:format("=== 事务模式性能测试完成 ===~n").

%% ==================== 综合性能测试 ====================

%% 运行所有性能测试
run_all_performance_tests() ->
    run_all_performance_tests(1, 1000).

run_all_performance_tests(ProcessCnt, LoopCnt) ->
    io:format("========================================~n"),
    io:format("        eLmdb 综合性能测试套件         ~n"),
    io:format("========================================~n~n"),
    
    Tests = [
        {batch_performance_test, "批量操作性能测试"},
        {concurrency_performance_test, "并发读写性能测试"},
        {mapsize_performance_test, "内存映射大小影响测试"},
        {data_size_performance_test, "数据大小影响测试"},
        {traversal_performance_test, "遍历操作性能测试"},
        {transaction_mode_performance_test, "事务模式性能测试"}
    ],
    
    lists:foreach(fun({TestFun, TestName}) ->
        io:format("运行测试: ~ts~n", [TestName]),
        try
            case TestFun of
                batch_performance_test ->
                    batch_performance_test(ProcessCnt, LoopCnt);
                concurrency_performance_test ->
                    concurrency_performance_test(LoopCnt);
                mapsize_performance_test ->
                    mapsize_performance_test(ProcessCnt, LoopCnt);
                data_size_performance_test ->
                    data_size_performance_test(ProcessCnt, LoopCnt);
                traversal_performance_test ->
                    traversal_performance_test(ProcessCnt, LoopCnt);
                transaction_mode_performance_test ->
                    transaction_mode_performance_test(ProcessCnt, LoopCnt)
            end,
            io:format("✓ ~ts 完成~n~n", [TestName])
        catch
            Error:Reason ->
                io:format("✗ ~ts 失败: ~p:~p~n~n", [TestName, Error, Reason])
        end
    end, Tests),
    
    io:format("========================================~n"),
    io:format("       所有性能测试运行完成           ~n"),
    io:format("========================================~n").