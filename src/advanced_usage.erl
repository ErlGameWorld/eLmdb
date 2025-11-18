-module(advanced_usage).
-compile(export_all).

-include_lib("eLmdb/include/eLmdb.hrl").

%% eLmdb 高级使用示例

%% 示例配置
-define(DB_PATH, "./advanced_db").
-define(MAP_SIZE, 500 * 1024 * 1024).  %% 500MB
-define(MAX_DBS, 20).
-define(MAX_READERS, 256).

%% 多数据库管理示例
multi_database_example() ->
    io:format("=== eLmdb 多数据库管理示例 ===~n~n"),
    
    {ok, EnvRef} = eLmdb:envOpen(?DB_PATH, ?MAP_SIZE, ?MAX_DBS, ?MAX_READERS, 0),
    
    %% 创建多个数据库
    Databases = [
        {"users_db", "用户数据库"},
        {"products_db", "产品数据库"},
        {"orders_db", "订单数据库"},
        {"logs_db", "日志数据库"},
        {"config_db", "配置数据库"}
    ],
    
    io:format("1. 创建多个数据库...~n"),
    DbiRefs = lists:map(fun({DbName, Description}) ->
        {ok, DbiRef} = eLmdb:dbOpen(EnvRef, DbName, 262144), %% MDB_CREATE
        io:format("   创建数据库: ~s (~ts) -> ~p~n", [DbName, Description, DbiRef]),
        {DbName, DbiRef}
    end, Databases),
    io:format("~n"),
    
    %% 为每个数据库插入不同类型的数据
    io:format("2. 为每个数据库插入数据...~n"),
    
    %% 用户数据库
    {_, UsersDbi} = lists:keyfind("users_db", 1, DbiRefs),
    UserData = [
        {"user:1001", "{\"name\":\"张三\",\"age\":25,\"email\":\"zhangsan@example.com\"}"},
        {"user:1002", "{\"name\":\"李四\",\"age\":30,\"email\":\"lisi@example.com\"}"},
        {"user:1003", "{\"name\":\"王五\",\"age\":28,\"email\":\"wangwu@example.com\"}"}
    ],
    lists:foreach(fun({Key, Value}) ->
        ok = eLmdb:put(EnvRef, UsersDbi, Key, Value, 0),
        ok = eLmdb:putAsync(EnvRef, UsersDbi, Key, Value, 0),
        ok = eLmdb:putSync(EnvRef, UsersDbi, Key, Value, 0),
        io:format("   插入: ~s -> ~ts~n", [Key, Value])
    end, UserData),
    io:format("   用户数据库: 插入 ~p 条记录~n", [length(UserData)]),
    
    %% 产品数据库
    {_, ProductsDbi} = lists:keyfind("products_db", 1, DbiRefs),
    ProductData = [
        {"product:2001", "{\"name\":\"笔记本电脑\",\"price\":5999,\"category\":\"电子产品\"}"},
        {"product:2002", "{\"name\":\"智能手机\",\"price\":2999,\"category\":\"电子产品\"}"},
        {"product:2003", "{\"name\":\"办公椅\",\"price\":899,\"category\":\"家具\"}"}
    ],
    lists:foreach(fun({Key, Value}) ->
        ok = eLmdb:put(EnvRef, ProductsDbi, Key, Value, 0)
    end, ProductData),
    io:format("   产品数据库: 插入 ~p 条记录~n", [length(ProductData)]),
    
    %% 跨数据库查询示例
    io:format("3. 跨数据库查询示例...~n"),
    
    %% 查询用户和产品信息
    UserQuery = [{UsersDbi, "user:1001"}, {UsersDbi, "user:1002"}],
    ProductQuery = [{ProductsDbi, "product:2001"}, {ProductsDbi, "product:2002"}],
    
    {ok, UserResults} = eLmdb:getMulti(EnvRef, UserQuery),
    {ok, ProductResults} = eLmdb:getMulti(EnvRef, ProductQuery),
    
    io:format("   用户查询结果: ~p~n", [UserResults]),
    io:format("   产品查询结果: ~p~n~n", [ProductResults]),
    
    %% 清理资源
    ok = eLmdb:envClose(EnvRef),
    io:format("=== 多数据库管理示例完成 ===~n"),
    ok.

%% 事务处理示例
transaction_example() ->
    io:format("=== eLmdb 事务处理示例 ===~n~n"),
    
    {ok, EnvRef} = eLmdb:envOpen(?DB_PATH, ?MAP_SIZE, ?MAX_DBS, ?MAX_READERS, 0),
    {ok, DbiRef} = eLmdb:dbOpen(EnvRef, "transaction_db", 262144),
    
    %% 事务1: 原子性操作 - 银行转账
    io:format("1. 原子性操作 - 银行转账示例...~n"),
    
    %% 初始化账户余额
    InitialData = [
        {"account:alice", "1000"},
        {"account:bob", "500"},
        {"account:charlie", "2000"}
    ],
    
    Operations = lists:map(fun({Key, Value}) ->
        {DbiRef, Key, Value}
    end, InitialData),
    
    ok = eLmdb:writes(EnvRef, Operations, ?DbTxnSame),
    io:format("   初始化账户余额完成~n"),
    
    %% 模拟转账操作
    TransferAmount = "200",
    FromAccount = "account:alice",
    ToAccount = "account:bob",
    
    %% 读取当前余额
    {ok, FromBalance} = eLmdb:get(EnvRef, DbiRef, FromAccount),
    {ok, ToBalance} = eLmdb:get(EnvRef, DbiRef, ToAccount),
    
    io:format("   转账前余额: ~s=~s, ~s=~s~n", 
              [FromAccount, FromBalance, ToAccount, ToBalance]),
    
    %% 执行转账（原子操作）
    TransferOperations = [
        {DbiRef, FromAccount, integer_to_list(list_to_integer(FromBalance) - list_to_integer(TransferAmount))},
        {DbiRef, ToAccount, integer_to_list(list_to_integer(ToBalance) + list_to_integer(TransferAmount))}
    ],
    
    ok = eLmdb:writes(EnvRef, TransferOperations, ?DbTxnSame),
    
    %% 验证转账结果
    {ok, NewFromBalance} = eLmdb:get(EnvRef, DbiRef, FromAccount),
    {ok, NewToBalance} = eLmdb:get(EnvRef, DbiRef, ToAccount),
    
    io:format("   转账后余额: ~s=~s, ~s=~s~n", 
              [FromAccount, NewFromBalance, ToAccount, NewToBalance]),
    
    TotalBefore = list_to_integer(FromBalance) + list_to_integer(ToBalance),
    TotalAfter = list_to_integer(NewFromBalance) + list_to_integer(NewToBalance),
    
    io:format("   总金额验证: 转账前=~p, 转账后=~p, 一致=~p~n~n", 
              [TotalBefore, TotalAfter, TotalBefore =:= TotalAfter]),
    
    %% 事务2: 批量数据导入
    io:format("2. 批量数据导入事务...~n"),
    
    BatchData = lists:map(fun(I) ->
        Key = "batch_item:" ++ integer_to_list(I),
        Value = "{\"id\":" ++ integer_to_list(I) ++ 
                ",\"timestamp\":" ++ integer_to_list(erlang:system_time(second)) ++ 
                ",\"data\":\"sample_data_" ++ integer_to_list(I) ++ "\"}",
        {DbiRef, Key, Value}
    end, lists:seq(1, 100)),
    
    ok = eLmdb:writes(EnvRef, BatchData, ?DbTxnSame),
	io:format("   批量导入 ~p 条数据完成~n", [length(BatchData)]),
	
	%% 验证批量数据
	{ok, AllData} = eLmdb:tvsFun(EnvRef, DbiRef, ?DbTvsAsc, ?DbTvsKv, fun(One, Acc) -> [One | Acc] end, []),
	io:format("   数据库中共有 ~p 条记录~n~n", [length(AllData)]),
    
    %% 清理资源
    ok = eLmdb:envClose(EnvRef),
    io:format("=== 事务处理示例完成 ===~n"),
    ok.

%% 错误处理和恢复示例
error_handling_example() ->
    io:format("=== eLmdb 错误处理和恢复示例 ===~n~n"),
    
    {ok, EnvRef} = eLmdb:envOpen(?DB_PATH, ?MAP_SIZE, ?MAX_DBS, ?MAX_READERS, 0),
    {ok, DbiRef} = eLmdb:dbOpen(EnvRef, "error_db", 262144),
    
    %% 1. 正常操作
    io:format("1. 正常数据操作...~n"),
    ok = eLmdb:put(EnvRef, DbiRef, "normal_key", "normal_value", 0),
    {ok, NormalValue} = eLmdb:get(EnvRef, DbiRef, "normal_key"),
    io:format("   正常操作成功: normal_key -> ~s~n~n", [NormalValue]),
    
    %% 2. 错误操作示例
    io:format("2. 错误操作处理...~n"),
    
    %% 查询不存在的键
    case eLmdb:get(EnvRef, DbiRef, "nonexistent_key") of
        {ok, Value} ->
            io:format("   意外成功: nonexistent_key -> ~s~n", [Value]);
        {error, Reason} ->
            io:format("   预期错误: nonexistent_key -> ~p~n", [Reason]);
        notFound ->
            io:format("   预期结果: nonexistent_key -> 键不存在~n")
    end,
    
    %% 删除不存在的键
    case eLmdb:del(EnvRef, DbiRef, "nonexistent_key") of
        ok ->
            io:format("   意外成功: 删除不存在的键~n");
        {error, Reason2} ->
            io:format("   预期错误: 删除不存在的键 -> ~p~n", [Reason2])
    end,
    
    %% 3. 环境状态检查
    io:format("3. 环境状态检查...~n"),
    
    case eLmdb:envInfo(EnvRef) of
        {ok, EnvInfo} ->
            io:format("   环境信息正常: ~p~n", [EnvInfo]);
        {error, Reason3} ->
            io:format("   环境信息获取失败: ~p~n", [Reason3])
    end,
    
    case eLmdb:envStat(EnvRef) of
        {ok, #envStat{} = EnvStat} ->
            io:format("   环境统计正常: ~p~n", [EnvStat]);
        {error, Reason4} ->
            io:format("   环境统计获取失败: ~p~n", [Reason4])
    end,
    
    %% 4. 数据库状态检查
    io:format("4. 数据库状态检查...~n"),
    
    case eLmdb:dbStat(EnvRef, DbiRef) of
        {ok, DbiStat} ->
            io:format("   数据库统计正常: ~p~n", [DbiStat]);
        {error, Reason5} ->
            io:format("   数据库统计获取失败: ~p~n", [Reason5])
    end,
    
    case eLmdb:dbFlags(EnvRef, DbiRef) of
        {ok, DbiFlags} ->
            io:format("   数据库标志正常: ~p~n", [DbiFlags]);
        {error, Reason6} ->
            io:format("   数据库标志获取失败: ~p~n", [Reason6])
    end,
    
    %% 5. 恢复操作
    io:format("5. 数据恢复操作...~n"),
    
    %% 备份数据
	{ok, BackupData} = eLmdb:tvsFun(EnvRef, DbiRef, ?DbTvsAsc, ?DbTvsKv, fun(One, Acc) -> [One | Acc] end, []),
	io:format("   备份数据: ~p 条记录~n", [length(BackupData)]),
    
    %% 模拟数据损坏恢复
    io:format("   模拟数据恢复过程...~n"),
    
    %% 清理所有数据
    lists:foreach(fun({Key, _}) ->
        eLmdb:del(EnvRef, DbiRef, Key)
    end, BackupData),
    
    %% 从备份恢复数据
    RecoveryOperations = lists:map(fun({Key, Value}) ->
        {DbiRef, Key, Value}
    end, BackupData),
    
    ok = eLmdb:writes(EnvRef, RecoveryOperations, ?DbTxnSame),
	io:format("   数据恢复完成: ~p 条记录~n", [length(RecoveryOperations)]),
	
	%% 验证恢复结果
	{ok, RestoredData} = eLmdb:tvsFun(EnvRef, DbiRef, ?DbTvsAsc, ?DbTvsKv, fun(One, Acc) -> [One | Acc] end, []),
	io:format("   恢复验证: 原始 ~p 条, 恢复后 ~p 条, 一致=~p~n~n", 
			  [length(BackupData), length(RestoredData), 
			   length(BackupData) =:= length(RestoredData)]),
    
    %% 清理资源
    ok = eLmdb:envClose(EnvRef),
    io:format("=== 错误处理和恢复示例完成 ===~n"),
    ok.

%% 性能优化示例
performance_optimization_example() ->
    io:format("=== eLmdb 性能优化示例 ===~n~n"),
    
    %% 使用不同的环境配置进行性能对比
    Configs = [
        {"def", ?MAP_SIZE, ?MAX_DBS, ?MAX_READERS, 0},
        {"big", 1000 * 1024 * 1024, ?MAX_DBS, ?MAX_READERS, 0},
        {"mdb", ?MAP_SIZE, 50, ?MAX_READERS, 0},
        {"pdb", ?MAP_SIZE, ?MAX_DBS, 512, 0}
    ],
    
    TestDataSize = 1000,
    
    lists:foreach(fun({ConfigName, MapSize, MaxDbs, MaxReaders, Flags1}) ->
        io:format("配置: ~ts~n", [ConfigName]),
        
        %% 创建测试环境
        EnvPath = ?DB_PATH ++ "_" ++ ConfigName,
        {ok, EnvRef} = eLmdb:envOpen(EnvPath, MapSize, MaxDbs, MaxReaders, Flags1),
        {ok, DbiRef} = eLmdb:dbOpen(EnvRef, "perf_db", 262144),
        
        %% 写入性能测试
        StartTime = erlang:system_time(microsecond),
        
        Operations = lists:map(fun(I) ->
            Key = "key_" ++ integer_to_list(I),
            Value = "value_" ++ integer_to_list(I) ++ string:copies("x", 100), %% 100字节数据
            {DbiRef, Key, Value}
        end, lists:seq(1, TestDataSize)),
        
        ok = eLmdb:writes(EnvRef, Operations, ?DbTxnSame),
        
        EndTime = erlang:system_time(microsecond),
        WriteTime = EndTime - StartTime,
        
        %% 读取性能测试
        StartTime2 = erlang:system_time(microsecond),
        
        QueryList = lists:map(fun(I) ->
            Key = "key_" ++ integer_to_list(I),
            {DbiRef, Key}
        end, lists:seq(1, TestDataSize)),
        
        {ok, _} = eLmdb:getMulti(EnvRef, QueryList),
        
        EndTime2 = erlang:system_time(microsecond),
        ReadTime = EndTime2 - StartTime2,
        
        %% 统计信息
        io:format("   写入 ~p 条数据耗时: ~p 微秒 (~.2f 条/秒)~n", 
                  [TestDataSize, WriteTime, TestDataSize / (WriteTime / 1000000)]),
        io:format("   读取 ~p 条数据耗时: ~p 微秒 (~.2f 条/秒)~n", 
                  [TestDataSize, ReadTime, TestDataSize / (ReadTime / 1000000)]),
        
        %% 环境信息
        {ok, EnvInfo} = eLmdb:envInfo(EnvRef),
        case eLmdb:envStat(EnvRef) of
            {ok, #envStat{} = EnvStat} ->
                io:format("   环境信息: ~p~n", [EnvInfo]),
                io:format("   环境统计: ~p~n", [EnvStat]);
            {error, Reason} ->
                io:format("   环境统计获取失败: ~p~n", [Reason])
        end,
        
        %% 清理
        ok = eLmdb:envClose(EnvRef),
        io:format("~n")
    end, Configs),
    
    io:format("=== 性能优化示例完成 ===~n"),
    ok.

%% 主函数 - 运行所有高级示例
main() ->
    io:format("========================================~n"),
    io:format("        eLmdb 高级使用示例             ~n"),
    io:format("========================================~n~n"),
    
    %% 添加代码路径
    code:add_pathz("_build/default/lib/eLmdb/ebin"),
    
    %% 运行高级示例
    multi_database_example(),
    io:format("~n"),
    
    transaction_example(),
    io:format("~n"),
    
    error_handling_example(),
    io:format("~n"),
    
    performance_optimization_example(),
    
    io:format("========================================~n"),
    io:format("        所有高级示例运行完成           ~n"),
    io:format("========================================~n"),
    ok.