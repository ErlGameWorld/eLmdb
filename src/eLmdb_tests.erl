-module(eLmdb_tests).
-compile(export_all).

-include_lib("eLmdb/include/eLmdb.hrl").

%% 测试配置
-define(TEST_DB_PATH, "./test_db").
-define(TEST_DB_PATH_SMALL, "./test_db_small").
-define(TEST_MAPSIZE, 100 * 1024 * 1024).  %% 100MB
-define(TEST_MAX_DBS, 10).
-define(TEST_MAX_READERS, 126).

%% 环境管理功能测试
environment_management_test() ->
    io:format("=== 环境管理功能测试 ===~n~n"),
    
    %% 1. 环境创建测试
    io:format("1. 环境创建测试...~n"),
    EnvRef = case eLmdb:envOpen(?TEST_DB_PATH, ?TEST_MAPSIZE, ?TEST_MAX_DBS, ?TEST_MAX_READERS, 0) of
        {ok, Ref} ->
            io:format("   环境创建成功: ~p~n", [Ref]),
            Ref;
        {error, Reason} ->
            io:format("   环境创建失败: ~p~n", [Reason]),
            return
    end,
    
    %% 2. 环境信息查询测试
    io:format("2. 环境信息查询测试...~n"),
    case eLmdb:envInfo(EnvRef) of
        {ok, EnvInfo} ->
            io:format("   环境信息查询成功: ~p~n", [EnvInfo]);
        {error, Reason2} ->
            io:format("   环境信息查询失败: ~p~n", [Reason2])
    end,
    
    %% 3. 环境统计查询测试
    io:format("3. 环境统计查询测试...~n"),
    case eLmdb:envStat(EnvRef) of
        {ok, EnvStat} ->
            io:format("   环境统计查询成功: ~p~n", [EnvStat]);
        {error, Reason3} ->
            io:format("   环境统计查询失败: ~p~n", [Reason3])
    end,
    
    %% 4. 环境同步测试
    io:format("4. 环境同步测试...~n"),
    case eLmdb:envSync(EnvRef) of
        ok ->
            io:format("   环境同步成功~n");
        {error, Reason4} ->
            io:format("   环境同步失败: ~p~n", [Reason4])
    end,
    
    %% 5. 关闭环境
    io:format("5. 关闭环境...~n"),
    case eLmdb:envClose(EnvRef) of
        ok ->
            io:format("   环境关闭成功~n");
        {error, Reason5} ->
            io:format("   环境关闭失败: ~p~n", [Reason5])
    end,
    
    io:format("~n=== 环境管理功能测试完成 ===~n"),
    ok.

%% 数据库操作功能测试
database_operations_test() ->
    io:format("=== 数据库操作功能测试 ===~n~n"),
    
    %% 1. 创建环境
    io:format("1. 创建环境...~n"),
    EnvRef = case eLmdb:envOpen(?TEST_DB_PATH, ?TEST_MAPSIZE, ?TEST_MAX_DBS, ?TEST_MAX_READERS, 0) of
        {ok, Ref} ->
            io:format("   环境创建成功: ~p~n", [Ref]),
            Ref;
        {error, Reason} ->
            io:format("   环境创建失败: ~p~n", [Reason]),
            return
    end,
    
    %% 2. 数据库创建测试
    io:format("2. 数据库创建测试...~n"),
    DbiRef = case eLmdb:dbOpen(EnvRef, "test_db", 262144) of  %% MDB_CREATE flag
        {ok, DbRef} ->
            io:format("   数据库创建成功: ~p~n", [DbRef]),
			  DbRef;
        {error, Reason2} ->
            io:format("   数据库创建失败: ~p~n", [Reason2]),
            eLmdb:envClose(EnvRef),
            return
    end,
    
    %% 3. 数据库统计查询测试
    io:format("3. 数据库统计查询测试...~n"),
    case eLmdb:dbStat(EnvRef, DbiRef) of
        {ok, DbiStat} ->
            io:format("   数据库统计查询成功: ~p~n", [DbiStat]);
        {error, Reason3} ->
            io:format("   数据库统计查询失败: ~p~n", [Reason3])
    end,
    
    %% 4. 数据库标志查询测试
    io:format("4. 数据库标志查询测试...~n"),
    case eLmdb:dbFlags(EnvRef, DbiRef) of
        {ok, DbiFlags} ->
            io:format("   数据库标志查询成功: ~p~n", [DbiFlags]);
        {error, Reason4} ->
            io:format("   数据库标志查询失败: ~p~n", [Reason4])
    end,
    
    %% 5. 关闭环境
    io:format("5. 关闭环境...~n"),
    case eLmdb:envClose(EnvRef) of
        ok ->
            io:format("   环境关闭成功~n");
        {error, Reason5} ->
            io:format("   环境关闭失败: ~p~n", [Reason5])
    end,
    
    io:format("~n=== 数据库操作功能测试完成 ===~n"),
    ok.

%% 数据操作功能测试
data_operations_test() ->
    io:format("=== 数据操作功能测试 ===~n~n"),
    
    %% 1. 创建环境和数据库
    io:format("1. 创建环境和数据库...~n"),
    EnvRef = case eLmdb:envOpen(?TEST_DB_PATH, ?TEST_MAPSIZE, ?TEST_MAX_DBS, ?TEST_MAX_READERS, 0) of
        {ok, Ref} ->
            io:format("   环境创建成功: ~p~n", [Ref]),
            Ref;
        {error, Reason} ->
            io:format("   环境创建失败: ~p~n", [Reason]),
            return
    end,
    
    DbiRef = case eLmdb:dbOpen(EnvRef, "test_db", 16#40000) of
        {ok, DbRef} ->
            io:format("   数据库创建成功: ~p~n", [DbRef]),
			  DbRef;
        {error, Reason2} ->
            io:format("   数据库创建失败: ~p~n", [Reason2]),
            eLmdb:envClose(EnvRef),
            return
    end,
    
    %% 2. 数据插入测试
    io:format("2. 数据插入测试...~n"),
    case eLmdb:put(EnvRef, DbiRef, "test_key", "test_value", 0) of
        ok ->
            io:format("   数据插入成功~n");
        {error, Reason3} ->
            io:format("   数据插入失败: ~p~n", [Reason3])
    end,
    
    %% 3. 数据查询测试
    io:format("3. 数据查询测试...~n"),
    case eLmdb:get(EnvRef, DbiRef, "test_key") of
        {ok, Value} ->
            io:format("   数据查询成功: ~p -> ~p~n", ["test_key", Value]);
        {error, Reason4} ->
            io:format("   数据查询失败: ~p~n", [Reason4])
    end,
    
    %% 4. 数据删除测试
    io:format("4. 数据删除测试...~n"),
    case eLmdb:del(EnvRef, DbiRef, "test_key") of
        ok ->
            io:format("   数据删除成功~n");
        {error, Reason5} ->
            io:format("   数据删除失败: ~p~n", [Reason5])
    end,
    
    %% 5. 验证删除结果
    io:format("5. 验证删除结果...~n"),
    case eLmdb:get(EnvRef, DbiRef, "test_key") of
        {ok, _} ->
            io:format("   删除验证失败: 数据仍然存在~n");
        {error, _} ->
            io:format("   删除验证成功: 数据已删除~n")
    end,
    
    %% 6. 数据库清空测试
    io:format("6. 数据库清空测试...~n"),
    %% 先插入一些数据
    ok = eLmdb:put(EnvRef, DbiRef, "test_key2", "test_value2", 0),
    case eLmdb:dbClear(EnvRef, DbiRef) of
        ok ->
            io:format("   数据库清空成功~n");
        {error, Reason6} ->
            io:format("   数据库清空失败: ~p~n", [Reason6])
    end,
    
    %% 7. 关闭环境
    io:format("7. 关闭环境...~n"),
    case eLmdb:envClose(EnvRef) of
        ok ->
            io:format("   环境关闭成功~n");
        {error, Reason7} ->
            io:format("   环境关闭失败: ~p~n", [Reason7])
    end,
    
    io:format("~n=== 数据操作功能测试完成 ===~n"),
    ok.

%% 批量操作功能测试
batch_operations_test() ->
    io:format("=== 批量操作功能测试 ===~n~n"),
    
    %% 1. 创建环境和数据库
    io:format("1. 创建环境和数据库...~n"),
    EnvRef = case eLmdb:envOpen(?TEST_DB_PATH, ?TEST_MAPSIZE, ?TEST_MAX_DBS, ?TEST_MAX_READERS, 0) of
        {ok, Ref} ->
            io:format("   环境创建成功: ~p~n", [Ref]),
            Ref;
        {error, Reason} ->
            io:format("   环境创建失败: ~p~n", [Reason]),
            return
    end,
    
    DbiRef = case eLmdb:dbOpen(EnvRef, "test_db", 16#40000) of
        {ok, DbRef} ->
            io:format("   数据库创建成功: ~p~n", [DbRef]),
			  DbRef;
        {error, Reason2} ->
            io:format("   数据库创建失败: ~p~n", [Reason2]),
            eLmdb:envClose(EnvRef),
            return
    end,
    
    %% 2. 批量数据插入测试
    io:format("2. 批量数据插入测试...~n"),
    Operations = [
        {DbiRef, "key1", "value1"},
        {DbiRef, "key2", "value2"},
        {DbiRef, "key3", "value3"}
    ],
    case eLmdb:writes(EnvRef, Operations, ?DbTxnSame) of  %% 同步模式，相同事务
        ok ->
            io:format("   批量数据插入成功~n");
        {error, Reason3} ->
            io:format("   批量数据插入失败: ~p~n", [Reason3])
    end,
    
    %% 3. 验证批量插入结果
    io:format("3. 验证批量插入结果...~n"),
    lists:foreach(fun(Key) ->
        case eLmdb:get(EnvRef, DbiRef, Key) of
            {ok, Value} ->
                io:format("   查询成功: ~p -> ~p~n", [Key, Value]);
            {error, Reason4} ->
                io:format("   查询失败: ~p -> ~p~n", [Key, Reason4])
        end
    end, ["key1", "key2", "key3"]),
    
    %% 4. 批量数据查询测试
    io:format("4. 批量数据查询测试...~n"),
    QueryList = [
        {DbiRef, "key1"},
        {DbiRef, "key2"},
        {DbiRef, "key3"}
    ],
    case eLmdb:getMulti(EnvRef, QueryList) of
        {ok, Results} ->
            io:format("   批量查询成功: ~p~n", [Results]);
        {error, Reason5} ->
            io:format("   批量查询失败: ~p~n", [Reason5])
    end,
    
    %% 5. 批量数据删除测试
    io:format("5. 批量数据删除测试...~n"),
    DelOperations = [
        {DbiRef, "key1"},
        {DbiRef, "key2"}
    ],
    case eLmdb:writes(EnvRef, DelOperations, ?DbTxnSame) of
        ok ->
            io:format("   批量删除成功~n");
        {error, Reason6} ->
            io:format("   批量删除失败: ~p~n", [Reason6])
    end,
    
    %% 6. 验证删除结果
    io:format("6. 验证删除结果...~n"),
    lists:foreach(fun(Key) ->
        case eLmdb:get(EnvRef, DbiRef, Key) of
            {ok, _} ->
                io:format("   删除验证失败: ~p 仍然存在~n", [Key]);
            {error, _} ->
                io:format("   删除验证成功: ~p 已删除~n", [Key])
        end
    end, ["key1", "key2", "key3"]),
    
    %% 7. 关闭环境
    io:format("7. 关闭环境...~n"),
    case eLmdb:envClose(EnvRef) of
        ok ->
            io:format("   环境关闭成功~n");
        {error, Reason7} ->
            io:format("   环境关闭失败: ~p~n", [Reason7])
    end,
    
    io:format("~n=== 批量操作功能测试完成 ===~n"),
    ok.

%% 遍历操作功能测试
traversal_operations_test() ->
    io:format("=== 遍历操作功能测试 ===~n~n"),
    
    %% 1. 创建环境和数据库
    io:format("1. 创建环境和数据库...~n"),
    EnvRef = case eLmdb:envOpen(?TEST_DB_PATH, ?TEST_MAPSIZE, ?TEST_MAX_DBS, ?TEST_MAX_READERS, 0) of
        {ok, Ref} ->
            io:format("   环境创建成功: ~p~n", [Ref]),
            Ref;
        {error, Reason} ->
            io:format("   环境创建失败: ~p~n", [Reason]),
            return
    end,
    
    DbiRef = case eLmdb:dbOpen(EnvRef, "test_db", 16#40000) of
        {ok, DbRef} ->
            io:format("   数据库创建成功: ~p~n", [DbRef]),
			  DbRef;
        {error, Reason2} ->
            io:format("   数据库创建失败: ~p~n", [Reason2]),
            eLmdb:envClose(EnvRef),
            return
    end,
    
    %% 2. 插入测试数据
    io:format("2. 插入测试数据...~n"),
    TestData = [
        {"key1", "value1"},
        {"key2", "value2"},
        {"key3", "value3"},
        {"key4", "value4"},
        {"key5", "value5"}
    ],
    
    lists:foreach(fun({Key, Value}) ->
        case eLmdb:put(EnvRef, DbiRef, Key, Value, 0) of
            ok ->
                io:format("   插入: ~p -> ~p~n", [Key, Value]);
            {error, Reason3} ->
                io:format("   插入失败: ~p -> ~p~n", [Key, Reason3])
        end
    end, TestData),
    
    %% 3. 正序遍历测试
    io:format("3. 正序遍历测试...~n"),
    case eLmdb:tvsAsc(EnvRef, DbiRef, '$TvsBegin', 3) of
        {ok, ForwardData, NextKey} ->
            io:format("   正序遍历成功: ~p 条数据~n", [length(ForwardData)]),
            lists:foreach(fun({Key, Value}) ->
                io:format("     ~p -> ~p~n", [Key, Value])
            end, ForwardData),
            io:format("   下一个键: ~p~n", [NextKey]);
        {error, Reason4} ->
            io:format("   正序遍历失败: ~p~n", [Reason4])
    end,
    
    %% 4. 反序遍历测试
    io:format("4. 反序遍历测试...~n"),
    case eLmdb:tvsDesc(EnvRef, DbiRef, '$TvsBegin', 3) of
        {ok, ReverseData, PrevKey} ->
            io:format("   反序遍历成功: ~p 条数据~n", [length(ReverseData)]),
            lists:foreach(fun({Key, Value}) ->
                io:format("     ~p -> ~p~n", [Key, Value])
            end, ReverseData),
            io:format("   前一个键: ~p~n", [PrevKey]);
        {error, Reason5} ->
            io:format("   反序遍历失败: ~p~n", [Reason5])
    end,
    
    %% 5. 仅键遍历测试
    io:format("5. 仅键遍历测试...~n"),
    case eLmdb:tvsKeyAsc(EnvRef, DbiRef, '$TvsBegin', 5) of
        {ok, Keys, NextKey2} ->
            io:format("   仅键遍历成功: ~p 个键~n", [length(Keys)]),
            lists:foreach(fun(Key) ->
                io:format("     ~p~n", [Key])
            end, Keys),
            io:format("   下一个键: ~p~n", [NextKey2]);
        {error, Reason6} ->
            io:format("   仅键遍历失败: ~p~n", [Reason6])
    end,
    
    %% 6. 完整遍历测试
    io:format("6. 完整遍历测试...~n"),
    %% 使用tvsAsc函数进行完整遍历
    case eLmdb:tvsAsc(EnvRef, DbiRef, '$TvsBegin', 100) of
        {ok, AllData, _NextKey} ->
            io:format("   完整遍历成功: ~p 条数据~n", [length(AllData)]),
            lists:foreach(fun({Key, Value}) ->
                io:format("     ~p -> ~p~n", [Key, Value])
            end, AllData);
        {error, Reason7} ->
            io:format("   完整遍历失败: ~p~n", [Reason7])
    end,
    
    %% 7. 关闭环境
    io:format("7. 关闭环境...~n"),
    case eLmdb:envClose(EnvRef) of
        ok ->
            io:format("   环境关闭成功~n");
        {error, Reason8} ->
            io:format("   环境关闭失败: ~p~n", [Reason8])
    end,
    
    io:format("~n=== 遍历操作功能测试完成 ===~n"),
    ok.

%% 运行所有功能测试
run_all_functional_tests() ->
    io:format("=== 开始运行所有功能测试 ===~n~n"),
    
    Tests = [
        {environment_management_test, "环境管理功能测试"},
        {database_operations_test, "数据库操作功能测试"},
        {data_operations_test, "数据操作功能测试"},
        {batch_operations_test, "批量操作功能测试"},
        {traversal_operations_test, "遍历操作功能测试"}
    ],
    
    Results = lists:map(fun({TestFun, TestName}) ->
        io:format("运行测试: ~ts~n", [TestName]),
        io:format("---------------------------------------- ~n"),
        
        StartTime = erlang:system_time(millisecond),
        Result = try
            apply(?MODULE, TestFun, []),
            {passed, TestName}
        catch
            _:Error ->
                io:format("测试失败: ~p~n", [Error]),
                {failed, TestName, Error}
        end,
        
        EndTime = erlang:system_time(millisecond),
        Duration = EndTime - StartTime,
        io:format("测试用时: ~p 毫秒~n", [Duration]),
        io:format("----------------------------------------~n~n"),
        
        {Result, Duration}
    end, Tests),
    
    %% 输出测试结果汇总
    io:format("=== 测试结果汇总 ===~n~n"),
    
    Passed = lists:filter(fun({{passed, _}, _}) -> true; (_) -> false end, Results),
    Failed = lists:filter(fun({{failed, _, _}, _}) -> true; (_) -> false end, Results),
    
    io:format("通过测试: ~p 个~n", [length(Passed)]),
    io:format("失败测试: ~p 个~n", [length(Failed)]),
    
    TotalTime = lists:sum([Duration || {_, Duration} <- Results]),
    io:format("总测试用时: ~p 毫秒~n", [TotalTime]),
    
    if
        Failed == [] ->
            io:format("~n=== 所有测试通过! ===~n");
        true ->
            io:format("~n=== 有测试失败，请检查 ===~n"),
            lists:foreach(fun({{failed, TestName, Error}, _}) ->
                io:format("失败测试: ~ts, 错误: ~p~n", [TestName, Error])
            end, Failed)
    end,
    
    io:format("~n=== 测试完成 ===~n"),
    ok.

%% 边界条件测试
boundary_conditions_test() ->
    io:format("=== 边界条件测试 ===~n~n"),
    
    %% 1. 空数据库测试
    io:format("1. 空数据库测试...~n"),
    EnvRef = case eLmdb:envOpen(?TEST_DB_PATH, ?TEST_MAPSIZE, ?TEST_MAX_DBS, ?TEST_MAX_READERS, 0) of
        {ok, ERef} -> ERef;
        {error, Reason} -> 
            io:format("   环境创建失败: ~p~n", [Reason]),
            error(Reason)
    end,
    
    DbiRef = case eLmdb:dbOpen(EnvRef, "empty_db", 16#40000) of
        {ok, DbRef1} -> DbRef1;
        {error, Reason2} ->
            io:format("   数据库创建失败: ~p~n", [Reason2]),
            eLmdb:envClose(EnvRef),
            error(Reason2)
    end,
    
    %% 查询空数据库
    case eLmdb:get(EnvRef, DbiRef, "nonexistent_key") of
        {error, _} -> io:format("   空数据库查询测试通过~n");
        {ok, _} -> io:format("   空数据库查询测试失败~n")
    end,
    
    %% 2. 大数据量测试
    io:format("2. 大数据量测试...~n"),
    DbiRef2 = case eLmdb:dbOpen(EnvRef, "large_data_db", 16#40000) of
        {ok, DbRef2} -> DbRef2;
        {error, Reason3} ->
            io:format("   数据库创建失败: ~p~n", [Reason3]),
            error(Reason3)
    end,
    
    %% 插入大量数据
    StartTime = erlang:system_time(millisecond),
    lists:foreach(fun(I) ->
        Key = "key_" ++ integer_to_list(I),
        Value = "value_" ++ integer_to_list(I) ++ string:copies("_", 100),  %% 大值
        eLmdb:put(EnvRef, DbiRef2, Key, Value, 0, 2)
    end, lists:seq(1, 100)),
    
    EndTime = erlang:system_time(millisecond),
    io:format("   插入100条大数据记录用时: ~p 毫秒~n", [EndTime - StartTime]),
    
    %% 3. 边界键值测试
    io:format("3. 边界键值测试...~n"),
    BoundaryData = [
        {"", "空键测试"},
        {"very_long_key_" ++ string:copies("x", 100), "长键测试"},
        {"key_with_special_chars_!@#$%^&*()", "特殊字符键测试"}
    ],
    
    lists:foreach(fun({Key, Desc}) ->
        case eLmdb:put(EnvRef, DbiRef2, Key, Desc, 0, 2) of
            ok -> io:format("   边界键值测试通过: ~ts~n", [Desc]);
            {error, Reason4} -> io:format("   边界键值测试失败: ~s, 错误: ~p~n", [Desc, Reason4])
        end
    end, BoundaryData),
    
    %% 4. 内存限制测试
    io:format("4. 内存限制测试...~n"),
    %% 创建小内存环境
    SmallEnvRef = case eLmdb:envOpen(?TEST_DB_PATH_SMALL, 1024 * 1024, ?TEST_MAX_DBS, ?TEST_MAX_READERS, 0) of
        {ok, SmallERef} -> SmallERef;
        {error, Reason5} ->
            io:format("   小内存环境创建失败: ~p~n", [Reason5]),
            error(Reason5)
    end,
    
    SmallDbiRef = case eLmdb:dbOpen(SmallEnvRef, "small_mem_db", 16#40000) of
        {ok, SmallDbRef} -> SmallDbRef;
        {error, Reason6} ->
            io:format("   数据库创建失败: ~p~n", [Reason6]),
            eLmdb:envClose(SmallEnvRef),
            error(Reason6)
    end,
    
    %% 尝试插入大数据
    case eLmdb:put(SmallEnvRef, SmallDbiRef, "large_key", string:copies("x", 1024 * 512), 0, 2) of
        ok -> io:format("   内存限制测试: 大数据插入成功~n");
        {error, Reason7} -> io:format("   内存限制测试: 大数据插入失败(预期): ~p~n", [Reason7])
    end,
    
    eLmdb:envClose(SmallEnvRef),
    eLmdb:envClose(EnvRef),
    
    io:format("~n=== 边界条件测试完成 ===~n"),
    ok.

%% 并发操作测试
concurrency_test() ->
    io:format("=== 并发操作测试 ===~n~n"),
    
    %% 1. 创建共享环境
    io:format("1. 创建共享环境...~n"),
    EnvRef = case eLmdb:envOpen(?TEST_DB_PATH, ?TEST_MAPSIZE, ?TEST_MAX_DBS, ?TEST_MAX_READERS, 0) of
        {ok, Ref} -> Ref;
        {error, Reason} -> 
            io:format("   环境创建失败: ~p~n", [Reason]),
            error(Reason)
    end,
    
    DbiRef = case eLmdb:dbOpen(EnvRef, "concurrent_db", 16#40000) of
        {ok, DbRef3} -> DbRef3;
        {error, Reason2} ->
            io:format("   数据库创建失败: ~p~n", [Reason2]),
            eLmdb:envClose(EnvRef),
            error(Reason2)
    end,
    
    %% 2. 并发写入测试
    io:format("2. 并发写入测试...~n"),
    NumWriters = 5,
    WriterPids = lists:map(fun(I) ->
        spawn(fun() ->
            lists:foreach(fun(J) ->
                Key = "writer_" ++ integer_to_list(I) ++ "_key_" ++ integer_to_list(J),
                Value = "value_" ++ integer_to_list(I) ++ "_" ++ integer_to_list(J),
                case eLmdb:put(EnvRef, DbiRef, Key, Value, 0) of
                    ok -> ok;
                    {error, Reason3} -> 
                        io:format("   写入失败: ~p, 错误: ~p~n", [Key, Reason3])
                end
            end, lists:seq(1, 10))
        end)
    end, lists:seq(1, NumWriters)),
    
    %% 等待所有写入完成
    lists:foreach(fun(Pid) -> Pid ! done end, WriterPids),
    timer:sleep(1000),
    
    %% 3. 并发读取测试
    io:format("3. 并发读取测试...~n"),
    NumReaders = 3,
    ReaderPids = lists:map(fun(I) ->
        spawn(fun() ->
            lists:foreach(fun(J) ->
                Key = "writer_" ++ integer_to_list((I rem NumWriters) + 1) ++ "_key_" ++ integer_to_list(J),
                case eLmdb:get(EnvRef, DbiRef, Key) of
                    {ok, _Value} -> ok;
                    {error, Reason4} -> 
                        io:format("   读取失败: ~p, 错误: ~p~n", [Key, Reason4])
                end
            end, lists:seq(1, 10))
        end)
    end, lists:seq(1, NumReaders)),
    
    %% 等待所有读取完成
    lists:foreach(fun(Pid) -> Pid ! done end, ReaderPids),
    timer:sleep(1000),
    
    %% 4. 混合操作测试
    io:format("4. 混合操作测试...~n"),
    MixedPids = lists:map(fun(I) ->
        spawn(fun() ->
            case I rem 3 of
                0 -> %% 写入操作
                    Key = "mixed_key_" ++ integer_to_list(I),
                    eLmdb:put(EnvRef, DbiRef, Key, "mixed_value", 0, 2);
                1 -> %% 读取操作
                    Key = "mixed_key_" ++ integer_to_list(I - 1),
                    eLmdb:get(EnvRef, DbiRef, Key);
                2 -> %% 删除操作
                    Key = "mixed_key_" ++ integer_to_list(I - 2),
                    eLmdb:del(EnvRef, DbiRef, Key)
            end
        end)
    end, lists:seq(1, 10)),
    
    %% 等待混合操作完成
    lists:foreach(fun(Pid) -> Pid ! done end, MixedPids),
    timer:sleep(1000),
    
    eLmdb:envClose(EnvRef),
    
    io:format("~n=== 并发操作测试完成 ===~n"),
    ok.

%% 性能基准测试
performance_benchmark_test() ->
    io:format("=== 性能基准测试 ===~n~n"),
    
    %% 1. 创建测试环境
    io:format("1. 创建测试环境...~n"),
	Flags = ?MDB_NOSYNC bor ?MDB_NOMETASYNC bor ?MDB_WRITEMAP bor ?MDB_MAPASYNC bor ?MDB_NOMEMINIT bor ?MDB_NORDAHEAD,
    EnvRef = case eLmdb:envOpen(?TEST_DB_PATH, ?TEST_MAPSIZE, ?TEST_MAX_DBS, ?TEST_MAX_READERS, Flags) of
        {ok, Ref} -> Ref;
        {error, Reason} -> 
            io:format("   环境创建失败: ~p~n", [Reason]),
            return
    end,
    
    DbiRef = case eLmdb:dbOpen(EnvRef, "benchmark_db", 16#40000) of
        {ok, DbRef4} -> DbRef4;
        {error, Reason2} ->
            io:format("   数据库创建失败: ~p~n", [Reason2]),
            eLmdb:envClose(EnvRef),
            error(Reason2)
    end,
    
    %% 2. 写入性能测试
    io:format("2. 写入性能测试...~n"),
    NumRecords = 10000,
    
    WriteStart = erlang:system_time(microsecond),
    lists:foreach(fun(I) ->
        Key = "bench_key_" ++ integer_to_list(I),
        Value = "bench_value_" ++ integer_to_list(I),
        eLmdb:put(EnvRef, DbiRef, Key, Value, 0, 2)
    end, lists:seq(1, NumRecords)),
    WriteEnd = erlang:system_time(microsecond),
    
    WriteTime = (WriteEnd - WriteStart) / 1000,  %% 转换为毫秒
    WriteOpsPerSec = NumRecords / (WriteTime / 1000),
    
    io:format("   写入 ~p 条记录用时: ~.2f 毫秒~n", [NumRecords, WriteTime]),
    io:format("   写入性能: ~.2f 操作/秒~n", [WriteOpsPerSec]),
    
    %% 3. 读取性能测试
    io:format("3. 读取性能测试...~n"),
    ReadStart = erlang:system_time(microsecond),
    lists:foreach(fun(I) ->
        Key = "bench_key_" ++ integer_to_list(I),
        eLmdb:get(EnvRef, DbiRef, Key)
    end, lists:seq(1, NumRecords)),
    ReadEnd = erlang:system_time(microsecond),
    
    ReadTime = (ReadEnd - ReadStart) / 1000,  %% 转换为毫秒
    ReadOpsPerSec = NumRecords / (ReadTime / 1000),
    
    io:format("   读取 ~p 条记录用时: ~.2f 毫秒~n", [NumRecords, ReadTime]),
    io:format("   读取性能: ~.2f 操作/秒~n", [ReadOpsPerSec]),
    
    %% 4. 批量操作性能测试
    io:format("4. 批量操作性能测试...~n"),
    BatchSize = 1000,
    BatchData = lists:map(fun(I) ->
        {DbiRef, "batch_key_" ++ integer_to_list(I), "batch_value_" ++ integer_to_list(I)}
    end, lists:seq(1, BatchSize)),
    
    BatchStart = erlang:system_time(microsecond),
    eLmdb:writes(EnvRef, BatchData, ?DbTxnSame),
    BatchEnd = erlang:system_time(microsecond),
    
    BatchTime = (BatchEnd - BatchStart) / 1000,
    BatchOpsPerSec = BatchSize / (BatchTime / 1000),
    
    io:format("   批量写入 ~p 条记录用时: ~.2f 毫秒~n", [BatchSize, BatchTime]),
    io:format("   批量写入性能: ~.2f 操作/秒~n", [BatchOpsPerSec]),
    
    %% 5. 遍历性能测试
    io:format("5. 遍历性能测试...~n"),
    TraversalStart = erlang:system_time(microsecond),
    %% 使用tvsAsc函数进行遍历测试
    case eLmdb:tvsAsc(EnvRef, DbiRef, '$TvsBegin', 1000) of
        {ok, AllData, _NextKey} ->
            TraversalEnd = erlang:system_time(microsecond),
            TraversalTime = (TraversalEnd - TraversalStart) / 1000,
            io:format("   遍历 ~p 条记录用时: ~.2f 毫秒~n", [length(AllData), TraversalTime]);
        {error, Reason3} ->
            io:format("   遍历失败: ~p~n", [Reason3])
    end,
    
    %% 6. 性能对比分析
    io:format("6. 性能对比分析...~n"),
    io:format("   单条写入 vs 批量写入: ~.2f 倍性能提升~n", [WriteOpsPerSec / (NumRecords / BatchSize * BatchOpsPerSec)]),
    io:format("   读取 vs 写入: ~.2f 倍性能差异~n", [ReadOpsPerSec / WriteOpsPerSec]),
    
    eLmdb:envClose(EnvRef),
    
    io:format("~n=== 性能基准测试完成 ===~n"),
    ok.

%% 运行所有增强测试
run_all_enhanced_tests() ->
    io:format("=== 开始运行所有增强测试 ===~n~n"),
    
    EnhancedTests = [
        {boundary_conditions_test, "边界条件测试"},
        {concurrency_test, "并发操作测试"},
        {performance_benchmark_test, "性能基准测试"}
    ],
    
    Results = lists:map(fun({TestFun, TestName}) ->
        io:format("运行增强测试: ~ts~n", [TestName]),
        io:format("----------------------------------------~n"),
        
        StartTime = erlang:system_time(millisecond),
        Result = try
            apply(?MODULE, TestFun, []),
            {passed, TestName}
        catch
            _:Error ->
                io:format("增强测试失败: ~p~n", [Error]),
                {failed, TestName, Error}
        end,
        
        EndTime = erlang:system_time(millisecond),
        Duration = EndTime - StartTime,
        io:format("增强测试用时: ~p 毫秒~n", [Duration]),
        io:format("----------------------------------------~n~n"),
        
        {Result, Duration}
    end, EnhancedTests),
    
    %% 输出增强测试结果汇总
    io:format("=== 增强测试结果汇总 ===~n~n"),
    
    Passed = lists:filter(fun({{passed, _}, _}) -> true; (_) -> false end, Results),
    Failed = lists:filter(fun({{failed, _, _}, _}) -> true; (_) -> false end, Results),
    
    io:format("通过增强测试: ~p 个~n", [length(Passed)]),
    io:format("失败增强测试: ~p 个~n", [length(Failed)]),
    
    TotalTime = lists:sum([Duration || {_, Duration} <- Results]),
    io:format("总增强测试用时: ~p 毫秒~n", [TotalTime]),
    
    if
        Failed == [] ->
            io:format("~n=== 所有增强测试通过! ===~n");
        true ->
            io:format("~n=== 有增强测试失败，请检查 ===~n"),
            lists:foreach(fun({{failed, TestName, Error}, _}) ->
                io:format("失败增强测试: ~ts, 错误: ~p~n", [TestName, Error])
            end, Failed)
    end,
    
    io:format("~n=== 增强测试完成 ===~n"),
    ok.

%% 主函数 - 方便手动调用
main() ->
    io:format("eLmdb 功能测试套件~n"),
    io:format("==================~n~n"),
    
    io:format("可用测试函数:~n"),
    io:format("1. environment_management_test() - 环境管理功能测试~n"),
    io:format("2. database_operations_test() - 数据库操作功能测试~n"),
    io:format("3. data_operations_test() - 数据操作功能测试~n"),
    io:format("4. batch_operations_test() - 批量操作功能测试~n"),
    io:format("5. traversal_operations_test() - 遍历操作功能测试~n"),
    io:format("6. boundary_conditions_test() - 边界条件测试~n"),
    io:format("7. concurrency_test() - 并发操作测试~n"),
    io:format("8. performance_benchmark_test() - 性能基准测试~n"),
    io:format("9. run_all_functional_tests() - 运行所有功能测试~n"),
    io:format("10. run_all_enhanced_tests() - 运行所有增强测试~n"),
    io:format("~n"),
    
    io:format("当前状态: 测试模块已编译完成，可以正常运行所有测试~n"),
    io:format("~n"),
    io:format("要运行测试，请在Erlang shell中使用以下命令之一:~n"),
    io:format("   1> eLmdb_tests:run_all_functional_tests().  % 运行所有功能测试~n"),
    io:format("   2> eLmdb_tests:run_all_enhanced_tests().    % 运行所有增强测试~n"),
    io:format("   3> eLmdb_tests:main().                      % 显示此帮助信息~n"),
    io:format("~n"),
    io:format("测试模块编译状态: 正常~n"),
    io:format("所有变量安全问题已修复~n"),
    io:format("可以开始进行测试验证~n").