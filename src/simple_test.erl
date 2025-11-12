-module(simple_test).
-compile([export_all]).

%% 测试NIF模块是否能正确加载
load_test() ->
    io:format("测试NIF模块加载...~n"),
    try
        %% 尝试加载eNifLmdb模块
        case code:load_file(eNifLmdb) of
            {module, eNifLmdb} ->
                io:format("✓ eNifLmdb模块加载成功~n");
            LoadError ->
                io:format("✗ eNifLmdb模块加载失败: ~p~n", [LoadError])
        end,
        
        %% 检查模块信息
        ModuleInfo = eNifLmdb:module_info(),
        io:format("模块信息: ~p~n", [ModuleInfo])
    catch
        LoadErr:LoadReason ->
            io:format("✗ 加载过程中出错: ~p:~p~n", [LoadErr, LoadReason])
    end.

%% 测试环境创建
create_env_test() ->
    io:format("测试环境创建...~n"),
    try
        %% 创建测试目录
        ok = filelib:ensure_dir("./simple_test_db/"),
        
        %% 尝试创建环境
        case eLmdb:env_open("./simple_test_db", 104857600, 10, 126, 0) of
            {ok, Ref} ->
                io:format("✓ 环境创建成功~n"),
                eLmdb:env_close(Ref),
                io:format("✓ 环境关闭成功~n");
            EnvError ->
                io:format("✗ 环境创建失败: ~p~n", [EnvError])
        end
    catch
        EnvErr:EnvReason ->
            io:format("✗ 环境创建过程中出错: ~p:~p~n", [EnvErr, EnvReason])
    end.

main() ->
    io:format("========================================~n"),
    io:format("        LMDB 简单测试套件            ~n"),
    io:format("========================================~n~n"),
    
    %% 添加代码路径
    code:add_pathz("_build/default/lib/eLmdb/ebin"),
    
    load_test(),
    io:format("~n"),
    create_env_test(),
    
    io:format("~n========================================~n"),
    io:format("        LMDB 简单测试完成            ~n"),
    io:format("========================================~n").