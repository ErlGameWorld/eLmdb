# eLmdb - Erlang LMDB 绑定

一个高性能的 Erlang LMDB（Lightning Memory-Mapped Database）绑定库，提供完备的 LMDB 功能接口。

## 特性

- **完整功能支持**：支持 LMDB 的所有核心功能，包括环境管理、数据库操作、数据读写、批量操作等
- **高性能**：基于 NIF（Native Implemented Functions）实现，提供接近原生性能
- **类型安全**：提供 Erlang 友好的 API 接口
- **并发安全**：支持多线程并发访问，内置事务管理 支持同步异步读写 以及每个环境自己独有的写线程
- **跨平台**：支持 Windows、Linux、macOS 等主流操作系统
- **基于LMDB 0.9.33 Release (2024/05/21)**：最新发布的版本 
- **详细的性能测试**：tcLmdb.erl 模块 

## 说明
	由于LMDB文档申明事务和游标不能跨线程使用(http://www.lmdb.tech/doc/index.html)
	A thread can only use one transaction at a time, plus any child transactions. Each transaction belongs to one thread. See below. The MDB_NOTLS flag changes this for read-only transactions.
	所以事务和游标不对nif外提供接口，因为多次nif调用可能跨线程

## 快速开始

### 安装依赖

确保系统已安装 Erlang/OTP 和 rebar3：

```bash
# 安装 Erlang/OTP
# 从 https://www.erlang.org/downloads 下载安装

# 安装 rebar3
# 从 https://rebar3.org/ 下载或使用包管理器安装
```

### 编译项目

```bash
# 清理项目
rebar3 clean

# 编译项目
rebar3 compile
```

## API 文档

### 环境管理

```erlang
%% 创建 LMDB 环境
{ok, EnvRef} = eLmdb:env_open(Path, MapSize, MaxDbs, MaxReaders, Flags).

%% 关闭环境
ok = eLmdb:env_close(EnvRef).

%% 获取环境信息
{ok, Info} = eLmdb:env_info(EnvRef).

%% 获取环境统计
{ok, Stat} = eLmdb:env_stat(EnvRef).

%% 同步数据到磁盘
ok = eLmdb:env_sync(EnvRef).
```

### 数据库操作

```erlang
%% 打开数据库
{ok, DbiRef} = eLmdb:dbi_open(EnvRef, DbName, Flags).

%% 关闭数据库
ok = eLmdb:dbi_close(EnvRef, DbiRef).

%% 获取数据库统计
{ok, Stat} = eLmdb:dbi_stat(EnvRef, DbiRef).

%% 获取数据库标志
{ok, Flags} = eLmdb:dbi_flags(EnvRef, DbiRef).

%% 清空数据库
ok = eLmdb:drop(EnvRef, DbiRef).
```

### 数据操作

```erlang
%% 插入/更新数据
ok = eLmdb:put(EnvRef, DbiRef, Key, Value, Flags, TxnFlags).

%% 查询数据
{ok, Value} = eLmdb:get(EnvRef, DbiRef, Key).
{error, Reason} = eLmdb:get(EnvRef, DbiRef, Key).

%% 删除数据
ok = eLmdb:del(EnvRef, DbiRef, Key, TxnFlags).
```

### 批量操作

```erlang
%% 批量写入数据
Operations = [{DbiRef, Key1, Value1}, {DbiRef, Key2, Value2}, ...],
ok = eLmdb:writes(EnvRef, Operations, TxnFlags, SyncMode).

%% 批量查询数据
QueryList = [{DbiRef, Key1}, {DbiRef, Key2}, ...],
{ok, Results} = eLmdb:get_multi(EnvRef, QueryList).
```

### 遍历操作

```erlang
%% 正序遍历
{ok, Data, Cursor} = eLmdb:traversal_forward(EnvRef, DbiRef, StartKey, Limit).

%% 反序遍历
{ok, Data, Cursor} = eLmdb:traversal_reverse(EnvRef, DbiRef, StartKey, Limit).

%% 仅键遍历
{ok, Keys, Cursor} = eLmdb:traversal_keys_forward(EnvRef, DbiRef, StartKey, Limit).

%% 完整数据库遍历
{ok, AllData} = eLmdb:traversal_all(EnvRef, DbiRef).
```

## 配置选项

### 环境标志

- `0` - 默认标志
- `262144` - MDB_CREATE（创建数据库）
- `524288` - MDB_DUPSORT（允许重复键）
- `1048576` - MDB_INTEGERKEY（整数键）
- `2097152` - MDB_DUPFIXED（固定大小重复值）
- `4194304` - MDB_INTEGERDUP（整数重复值）
- `8388608` - MDB_REVERSEDUP（反向重复值）

### 事务标志

- `0` - 默认事务
- `2` - 同步事务
- `4` - 只读事务

## 性能优化

### 内存映射大小

根据数据量设置合适的映射大小：

```erlang
%% 100MB 映射大小
MapSize = 100 * 1024 * 1024.

%% 1GB 映射大小
MapSize = 1024 * 1024 * 1024.
```

### 批量操作

使用批量操作可以显著提高性能：

```erlang
%% 单条操作（慢）
lists:foreach(fun({Key, Value}) ->
    eLmdb:put(EnvRef, DbiRef, Key, Value, 0, 2)
end, DataList).

%% 批量操作（快）
Operations = [{DbiRef, Key, Value} || {Key, Value} <- DataList],
ok = eLmdb:writes(EnvRef, Operations, 2, 1).
```

## 错误处理

所有函数都返回标准 Erlang 错误格式：

```erlang
%% 成功情况
{ok, Result}

%% 错误情况
{error, Reason}
```

常见错误原因：

- `not_found` - 键不存在
- `invalid_handle` - 无效句柄
- `permission_denied` - 权限不足
- `disk_full` - 磁盘空间不足

## 开发指南

### 添加新功能

1. 在 `eNifLmdb.erl` 中添加 Erlang 函数声明
2. 在 `eNifLmdb.cc` 中添加 C++ 实现
3. 在 `eLmdb.erl` 中添加高级 API 封装
4. 在测试文件中添加相应的测试用例

### 调试技巧

```erlang
%% 启用详细日志
application:set_env(eLmdb, debug, true).

%% 检查 NIF 加载状态
eNifLmdb:module_info().

%% 运行模拟测试以验证逻辑
run_tests_mock:main().
```

## 故障排除

### NIF 加载失败

如果遇到 NIF 加载失败，请检查：

1. 系统是否安装了必要的编译工具
2. 是否已正确编译 NIF 库
3. 操作系统架构是否匹配

### 编译错误

Windows 系统可能需要安装 Visual Studio Build Tools 或 Windows SDK。

### 性能问题

- 确保使用合适的映射大小
- 优先使用批量操作
- 合理设置事务标志
- 避免频繁的环境创建和销毁

## 贡献指南

欢迎提交 Issue 和 Pull Request！

### 开发环境设置

1. 克隆项目
2. 安装依赖
3. 运行测试确保一切正常
4. 开始开发

### 代码规范

- 遵循 Erlang 编码规范
- 添加适当的注释和文档
- 为新功能添加测试用例
- 确保向后兼容性

## 许可证

详见 LICENSE 文件。

## 联系方式

如有问题或建议，请通过以下方式联系：

- 提交 GitHub Issue
- 发送邮件到项目维护者

---

**注意**：本项目仍在开发中，API 可能会有变动。生产环境使用前请充分测试。

