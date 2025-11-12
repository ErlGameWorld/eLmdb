-module(basic_usage).
-compile(export_all).

-include_lib("eLmdb/include/eLmdb.hrl").

%% eLmdb 基本使用示例

%% 示例配置
-define(DB_PATH, "./example_db").
-define(MAP_SIZE, 100 * 1024 * 1024).  %% 100MB
-define(MAX_DBS, 10).
-define(MAX_READERS, 126).

%% 基本使用示例
basic_example() ->
    io:format("=== eLmdb 基本使用示例 ===~n~n"),
    
    %% 1. 创建环境
    io:format("1. 创建 LMDB 环境...~n"),
    {ok, EnvRef} = eLmdb:env_open(?DB_PATH, ?MAP_SIZE, ?MAX_DBS, ?MAX_READERS, 0),
    io:format("   环境创建成功: ~p~n~n", [EnvRef]),
    
    %% 2. 打开数据库
    io:format("2. 打开数据库...~n"),
    {ok, DbiRef} = eLmdb:dbi_open(EnvRef, "example_db", 262144), %% MDB_CREATE
    io:format("   数据库打开成功: ~p~n~n", [DbiRef]),
    
    %% 3. 插入数据
    io:format("3. 插入测试数据...~n"),
    Data = [
        {"user:1", "Alice"},
        {"user:2", "Bob"},
        {"user:3", "Charlie"},
        {"config:language", "erlang"},
        {"config:version", "1.0"}
    ],
    
    lists:foreach(fun({Key, Value}) ->
        ok = eLmdb:put(EnvRef, DbiRef, Key, Value, 0, 2),
        io:format("   插入: ~s -> ~s~n", [Key, Value])
    end, Data),
    io:format("~n"),
    
    %% 4. 查询数据
    io:format("4. 查询数据...~n"),
    QueryKeys = ["user:1", "user:2", "config:language", "nonexistent"],
    
    lists:foreach(fun(Key) ->
        case eLmdb:get(EnvRef, DbiRef, Key) of
            {ok, Value} ->
                io:format("   查询成功: ~s -> ~s~n", [Key, Value]);
            {error, Reason} ->
                io:format("   查询失败: ~s -> ~p~n", [Key, Reason])
        end
    end, QueryKeys),
    io:format("~n"),
    
    %% 5. 更新数据
    io:format("5. 更新数据...~n"),
    ok = eLmdb:put(EnvRef, DbiRef, "user:1", "Alice Updated", 0, 2),
    {ok, UpdatedValue} = eLmdb:get(EnvRef, DbiRef, "user:1"),
    io:format("   更新成功: user:1 -> ~s~n~n", [UpdatedValue]),
    
    %% 6. 删除数据
    io:format("6. 删除数据...~n"),
    ok = eLmdb:del(EnvRef, DbiRef, "user:3", 0),
    case eLmdb:get(EnvRef, DbiRef, "user:3") of
        {ok, _} -> io:format("   删除失败~n");
        {error, _} -> io:format("   删除成功: user:3~n")
    end,
    io:format("~n"),
    
    %% 7. 获取环境信息
    io:format("7. 获取环境信息...~n"),
    {ok, EnvInfo} = eLmdb:env_info(EnvRef),
    io:format("   环境信息: ~p~n", [EnvInfo]),
    
    case eLmdb:env_stat(EnvRef) of
        {ok, #envStat{} = EnvStat} ->
            io:format("   环境统计: ~p~n~n", [EnvStat]);
        {error, Reason} ->
            io:format("   环境统计获取失败: ~p~n~n", [Reason])
    end,
    
    %% 8. 获取数据库信息
    io:format("8. 获取数据库信息...~n"),
    {ok, DbiStat} = eLmdb:dbi_stat(EnvRef, DbiRef),
    io:format("   数据库统计: ~p~n", [DbiStat]),
    
    {ok, DbiFlags} = eLmdb:dbi_flags(EnvRef, DbiRef),
    io:format("   数据库标志: ~p~n~n", [DbiFlags]),
    
    %% 9. 同步数据到磁盘
    io:format("9. 同步数据到磁盘...~n"),
    ok = eLmdb:env_sync(EnvRef),
    io:format("   同步完成~n~n"),
    
    %% 10. 清理资源
    io:format("10. 清理资源...~n"),
    ok = eLmdb:env_close(EnvRef),
    io:format("   环境关闭完成~n~n"),
    
    io:format("=== 示例完成 ===~n"),
    ok.

%% 批量操作示例
batch_operations_example() ->
    io:format("=== eLmdb 批量操作示例 ===~n~n"),
    
    %% 创建环境
    {ok, EnvRef} = eLmdb:env_open(?DB_PATH, ?MAP_SIZE, ?MAX_DBS, ?MAX_READERS, 0),
    {ok, DbiRef} = eLmdb:dbi_open(EnvRef, "batch_db", 262144),
    
    %% 批量插入数据
    io:format("1. 批量插入数据...~n"),
    BatchData = [
        {"batch:1", "value1"},
        {"batch:2", "value2"},
        {"batch:3", "value3"},
        {"batch:4", "value4"},
        {"batch:5", "value5"}
    ],
    
    Operations = [{DbiRef, Key, Value} || {Key, Value} <- BatchData],
    ok = eLmdb:writes(EnvRef, Operations, 2, 1), %% 同步模式，相同事务
    io:format("   批量插入完成: ~p 条数据~n~n", [length(BatchData)]),
    
    %% 批量查询数据
    io:format("2. 批量查询数据...~n"),
    QueryList = [{DbiRef, Key} || {Key, _} <- BatchData],
    {ok, Results} = eLmdb:get_multi(EnvRef, QueryList),
    
    lists:foreach(fun({Key, Status, ValueOrError}) ->
        case Status of
            ok ->
                io:format("   查询成功: ~s -> ~s~n", [Key, ValueOrError]);
            error ->
                io:format("   查询失败: ~s -> ~p~n", [Key, ValueOrError])
        end
    end, Results),
    io:format("~n"),
    
    %% 清理资源
    ok = eLmdb:env_close(EnvRef),
    io:format("=== 批量操作示例完成 ===~n"),
    ok.

%% 遍历操作示例
traversal_example() ->
    io:format("=== eLmdb 遍历操作示例 ===~n~n"),
    
    %% 创建环境并插入测试数据
    {ok, EnvRef} = eLmdb:env_open(?DB_PATH, ?MAP_SIZE, ?MAX_DBS, ?MAX_READERS, 0),
    {ok, DbiRef} = eLmdb:dbi_open(EnvRef, "traversal_db", 262144),
    
    TestData = [
        {"apple", "red"},
        {"banana", "yellow"},
        {"cherry", "red"},
        {"date", "brown"},
        {"elderberry", "purple"}
    ],
    
    lists:foreach(fun({Key, Value}) ->
        ok = eLmdb:put(EnvRef, DbiRef, Key, Value, 0, 2)
    end, TestData),
    
    %% 正序遍历
    io:format("1. 正序遍历...~n"),
    {ok, ForwardData, _} = eLmdb:traversal_forward(EnvRef, DbiRef, '$TvsBegin', 3),
    lists:foreach(fun({Key, Value}) ->
        io:format("   ~s -> ~s~n", [Key, Value])
    end, ForwardData),
    io:format("~n"),
    
    %% 反序遍历
    io:format("2. 反序遍历...~n"),
    {ok, ReverseData, _} = eLmdb:traversal_reverse(EnvRef, DbiRef, '$TvsBegin', 3),
    lists:foreach(fun({Key, Value}) ->
        io:format("   ~s -> ~s~n", [Key, Value])
    end, ReverseData),
    io:format("~n"),
    
    %% 仅键遍历
    io:format("3. 仅键遍历...~n"),
    {ok, Keys, _} = eLmdb:traversal_keys_forward(EnvRef, DbiRef, '$TvsBegin', 5),
    lists:foreach(fun(Key) ->
        io:format("   ~s~n", [Key])
    end, Keys),
    io:format("~n"),
    
    %% 完整遍历
    io:format("4. 完整数据库遍历...~n"),
    {ok, AllData} = eLmdb:traversal_all(EnvRef, DbiRef),
    io:format("   数据库中共有 ~p 条数据~n", [length(AllData)]),
    lists:foreach(fun({Key, Value}) ->
        io:format("   ~s -> ~s~n", [Key, Value])
    end, AllData),
    io:format("~n"),
    
    %% 清理资源
    ok = eLmdb:env_close(EnvRef),
    io:format("=== 遍历操作示例完成 ===~n"),
    ok.

%% 性能测试示例
performance_example() ->
    io:format("=== eLmdb 性能测试示例 ===~n~n"),
	Flags = ?MDB_NOSYNC bor ?MDB_NOMETASYNC bor ?MDB_WRITEMAP bor ?MDB_MAPASYNC bor ?MDB_NOMEMINIT bor ?MDB_NORDAHEAD,
    {ok, EnvRef} = eLmdb:env_open(?DB_PATH, ?MAP_SIZE, ?MAX_DBS, ?MAX_READERS, Flags),
    {ok, DbiRef} = eLmdb:dbi_open(EnvRef, "performance_db", 262144),
    
    %% 单条写入性能测试
    io:format("1. 单条写入性能测试...~n"),
    StartTime1 = erlang:system_time(microsecond),
    
    lists:foreach(fun(I) ->
        Key = "key_" ++ integer_to_list(I),
        Value = "value_" ++ integer_to_list(I),
        ok = eLmdb:put(EnvRef, DbiRef, Key, Value, 0, 2)
    end, lists:seq(1, 1000)),
    
    EndTime1 = erlang:system_time(microsecond),
    TimeTaken1 = EndTime1 - StartTime1,
    io:format("   单条写入 1000 条数据耗时: ~p 微秒~n", [TimeTaken1]),
    io:format("   平均耗时: ~p 微秒/条~n~n", [TimeTaken1 / 1000]),
    
    %% 批量写入性能测试
    io:format("2. 批量写入性能测试...~n"),
    StartTime2 = erlang:system_time(microsecond),
    
    Operations = lists:map(fun(I) ->
        Key = "batch_key_" ++ integer_to_list(I),
        Value = "batch_value_" ++ integer_to_list(I),
        {DbiRef, Key, Value}
    end, lists:seq(1, 1000)),
    
    ok = eLmdb:writes(EnvRef, Operations, 2, 1),
    
    EndTime2 = erlang:system_time(microsecond),
    TimeTaken2 = EndTime2 - StartTime2,
    io:format("   批量写入 1000 条数据耗时: ~p 微秒~n", [TimeTaken2]),
    io:format("   平均耗时: ~p 微秒/条~n~n", [TimeTaken2 / 1000]),
    
    io:format("   批量写入比单条写入快 ~.2f 倍~n~n", [TimeTaken1 / TimeTaken2]),
    
    %% 读取性能测试
    io:format("3. 读取性能测试...~n"),
    StartTime3 = erlang:system_time(microsecond),
    
    lists:foreach(fun(I) ->
        Key = "key_" ++ integer_to_list(I),
        {ok, _} = eLmdb:get(EnvRef, DbiRef, Key)
    end, lists:seq(1, 1000)),
    
    EndTime3 = erlang:system_time(microsecond),
    TimeTaken3 = EndTime3 - StartTime3,
    io:format("   读取 1000 条数据耗时: ~p 微秒~n", [TimeTaken3]),
    io:format("   平均耗时: ~p 微秒/条~n~n", [TimeTaken3 / 1000]),
    
    %% 清理资源
    ok = eLmdb:env_close(EnvRef),
    io:format("=== 性能测试示例完成 ===~n"),
    ok.

%% 主函数 - 运行所有示例
main() ->
    io:format("========================================~n"),
    io:format("        eLmdb 使用示例                 ~n"),
    io:format("========================================~n~n"),
    
    %% 添加代码路径
    code:add_pathz("_build/default/lib/eLmdb/ebin"),
    
    %% 运行示例
    basic_example(),
    io:format("~n"),
    
    batch_operations_example(),
    io:format("~n"),
    
    traversal_example(),
    io:format("~n"),
    
    performance_example(),
    
    io:format("========================================~n"),
    io:format("        所有示例运行完成               ~n"),
    io:format("========================================~n"),
    ok.