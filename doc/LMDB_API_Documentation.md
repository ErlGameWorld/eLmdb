# LMDB API 接口文档

## 概述
LMDB (Lightning Memory-Mapped Database) 是一个高性能的嵌入式键值存储数据库，采用内存映射文件技术实现零拷贝数据访问。

## 1. 环境管理 (Environment Management)

### mdb_env_create()
**功能**: 创建LMDB环境
**参数**: 无
**返回值**: 成功返回环境句柄，失败返回错误码
**说明**: 创建新的LMDB环境，必须在调用其他环境函数之前调用

### mdb_env_open()
**功能**: 打开LMDB环境
**参数**: 
- `env`: 环境句柄
- `path`: 数据库文件路径
- `flags`: 打开标志
- `mode`: 文件权限模式
**返回值**: 成功返回0，失败返回错误码

### mdb_env_close()
**功能**: 关闭LMDB环境
**参数**: `env`: 环境句柄
**返回值**: 无
**说明**: 关闭环境并释放所有相关资源

### mdb_env_set_mapsize()
**功能**: 设置内存映射大小
**参数**: 
- `env`: 环境句柄
- `size`: 映射大小（字节）
**返回值**: 成功返回0，失败返回错误码
**说明**: 必须在环境打开前调用，大小应为操作系统页大小的倍数

### mdb_env_set_maxreaders()
**功能**: 设置最大读者数
**参数**: 
- `env`: 环境句柄
- `readers`: 最大读者数
**返回值**: 成功返回0，失败返回错误码
**说明**: 默认126，必须在环境打开前调用

### mdb_env_set_maxdbs()
**功能**: 设置最大数据库数
**参数**: 
- `env`: 环境句柄
- `dbs`: 最大数据库数
**返回值**: 成功返回0，失败返回错误码
**说明**: 支持多个命名数据库时必须调用

### mdb_env_get_maxreaders()
**功能**: 获取最大读者数
**参数**: 
- `env`: 环境句柄
- `readers`: 输出参数，存储读者数
**返回值**: 成功返回0，失败返回错误码

### mdb_env_get_maxkeysize()
**功能**: 获取最大键大小
**参数**: `env`: 环境句柄
**返回值**: 最大键大小（字节）

### mdb_env_sync()
**功能**: 同步数据到磁盘
**参数**: `env`: 环境句柄
**返回值**: 成功返回0，失败返回错误码

### mdb_env_copy()
**功能**: 复制环境到文件
**参数**: 
- `env`: 环境句柄
- `path`: 目标文件路径
**返回值**: 成功返回0，失败返回错误码

### mdb_env_copyfd()
**功能**: 复制环境到文件描述符
**参数**: 
- `env`: 环境句柄
- `fd`: 文件描述符
**返回值**: 成功返回0，失败返回错误码

### mdb_env_get_fd()
**功能**: 获取环境文件描述符
**参数**: 
- `env`: 环境句柄
- `fd`: 输出参数，存储文件描述符
**返回值**: 成功返回0，失败返回错误码

### mdb_env_set_userctx() / mdb_env_get_userctx()
**功能**: 设置/获取用户上下文
**参数**: 
- `env`: 环境句柄
- `ctx`: 用户上下文指针
**返回值**: 成功返回0，失败返回错误码

## 2. 事务管理 (Transaction Management)

### mdb_txn_begin()
**功能**: 开始事务
**参数**: 
- `env`: 环境句柄
- `parent`: 父事务句柄（嵌套事务）
- `flags`: 事务标志（MDB_RDONLY等）
- `txn`: 输出参数，存储事务句柄
**返回值**: 成功返回0，失败返回错误码

### mdb_txn_commit()
**功能**: 提交事务
**参数**: `txn`: 事务句柄
**返回值**: 成功返回0，失败返回错误码
**说明**: 提交所有操作，释放事务资源

### mdb_txn_abort()
**功能**: 中止事务
**参数**: `txn`: 事务句柄
**返回值**: 无
**说明**: 放弃所有操作，释放事务资源

### mdb_txn_reset()
**功能**: 重置只读事务
**参数**: `txn`: 事务句柄
**返回值**: 无
**说明**: 中止事务但保留句柄，可用于mdb_txn_renew()

### mdb_txn_renew()
**功能**: 续用只读事务
**参数**: `txn`: 事务句柄
**返回值**: 成功返回0，失败返回错误码
**说明**: 为重置的事务重新获取读者锁

### mdb_txn_env()
**功能**: 获取事务所属环境
**参数**: `txn`: 事务句柄
**返回值**: 环境句柄

### mdb_txn_id()
**功能**: 获取事务ID
**参数**: `txn`: 事务句柄
**返回值**: 事务ID

## 3. 数据库操作 (Database Operations)

### mdb_dbi_open()
**功能**: 打开数据库
**参数**: 
- `txn`: 事务句柄
- `name`: 数据库名称（NULL表示默认数据库）
- `flags`: 数据库标志
- `dbi`: 输出参数，存储数据库句柄
**返回值**: 成功返回0，失败返回错误码

### mdb_dbi_close()
**功能**: 关闭数据库
**参数**: 
- `env`: 环境句柄
- `dbi`: 数据库句柄
**返回值**: 无
**说明**: 通常不需要手动关闭

### mdb_dbi_flags()
**功能**: 获取数据库标志
**参数**: 
- `txn`: 事务句柄
- `dbi`: 数据库句柄
- `flags`: 输出参数，存储标志
**返回值**: 成功返回0，失败返回错误码

### mdb_drop()
**功能**: 清空或删除数据库
**参数**: 
- `txn`: 事务句柄
- `dbi`: 数据库句柄
- `del`: 0=清空，1=删除
**返回值**: 成功返回0，失败返回错误码

### mdb_stat()
**功能**: 获取数据库统计信息
**参数**: 
- `txn`: 事务句柄
- `dbi`: 数据库句柄
- `stat`: 输出参数，存储统计信息
**返回值**: 成功返回0，失败返回错误码

## 4. 数据操作 (Data Operations)

### mdb_get()
**功能**: 获取数据
**参数**: 
- `txn`: 事务句柄
- `dbi`: 数据库句柄
- `key`: 键
- `data`: 输出参数，存储数据
**返回值**: 成功返回0，失败返回错误码

### mdb_put()
**功能**: 存储数据
**参数**: 
- `txn`: 事务句柄
- `dbi`: 数据库句柄
- `key`: 键
- `data`: 数据
- `flags`: 操作标志
**返回值**: 成功返回0，失败返回错误码

### mdb_del()
**功能**: 删除数据
**参数**: 
- `txn`: 事务句柄
- `dbi`: 数据库句柄
- `key`: 键
- `data`: 数据（对于重复键数据库）
**返回值**: 成功返回0，失败返回错误码

## 5. 游标操作 (Cursor Operations)

### mdb_cursor_open()
**功能**: 打开游标
**参数**: 
- `txn`: 事务句柄
- `dbi`: 数据库句柄
- `cursor`: 输出参数，存储游标句柄
**返回值**: 成功返回0，失败返回错误码

### mdb_cursor_close()
**功能**: 关闭游标
**参数**: `cursor`: 游标句柄
**返回值**: 无

### mdb_cursor_renew()
**功能**: 续用游标
**参数**: 
- `txn`: 事务句柄
- `cursor`: 游标句柄
**返回值**: 成功返回0，失败返回错误码

### mdb_cursor_get()
**功能**: 通过游标获取数据
**参数**: 
- `cursor`: 游标句柄
- `key`: 键
- `data`: 数据
- `op`: 游标操作类型
**返回值**: 成功返回0，失败返回错误码

### mdb_cursor_put()
**功能**: 通过游标存储数据
**参数**: 
- `cursor`: 游标句柄
- `key`: 键
- `data`: 数据
- `flags`: 操作标志
**返回值**: 成功返回0，失败返回错误码

### mdb_cursor_del()
**功能**: 通过游标删除数据
**参数**: 
- `cursor`: 游标句柄
- `flags`: 操作标志
**返回值**: 成功返回0，失败返回错误码

### mdb_cursor_count()
**功能**: 获取重复键计数
**参数**: 
- `cursor`: 游标句柄
- `countp`: 输出参数，存储计数
**返回值**: 成功返回0，失败返回错误码

### mdb_cursor_txn()
**功能**: 获取游标所属事务
**参数**: `cursor`: 游标句柄
**返回值**: 事务句柄

### mdb_cursor_dbi()
**功能**: 获取游标所属数据库
**参数**: `cursor`: 游标句柄
**返回值**: 数据库句柄

## 6. 比较函数设置 (Comparison Functions)

### mdb_set_compare()
**功能**: 设置键比较函数
**参数**: 
- `txn`: 事务句柄
- `dbi`: 数据库句柄
- `cmp`: 比较函数
**返回值**: 成功返回0，失败返回错误码

### mdb_set_dupsort()
**功能**: 设置重复数据比较函数
**参数**: 
- `txn`: 事务句柄
- `dbi`: 数据库句柄
- `cmp`: 比较函数
**返回值**: 成功返回0，失败返回错误码

### mdb_set_relfunc()
**功能**: 设置重定位函数
**参数**: 
- `txn`: 事务句柄
- `dbi`: 数据库句柄
- `rel`: 重定位函数
**返回值**: 成功返回0，失败返回错误码

### mdb_set_relctx()
**功能**: 设置重定位上下文
**参数**: 
- `txn`: 事务句柄
- `dbi`: 数据库句柄
- `ctx`: 上下文指针
**返回值**: 成功返回0，失败返回错误码

### mdb_cmp()
**功能**: 比较键
**参数**: 
- `txn`: 事务句柄
- `dbi`: 数据库句柄
- `a`: 第一个键
- `b`: 第二个键
**返回值**: <0(a<b), 0(a==b), >0(a>b)

### mdb_dcmp()
**功能**: 比较数据
**参数**: 
- `txn`: 事务句柄
- `dbi`: 数据库句柄
- `a`: 第一个数据
- `b`: 第二个数据
**返回值**: <0(a<b), 0(a==b), >0(a>b)

## 7. 读者管理 (Reader Management)

### mdb_reader_list()
**功能**: 列出读者锁表
**参数**: 
- `env`: 环境句柄
- `func`: 消息回调函数
- `ctx`: 回调上下文
**返回值**: 成功返回0，失败返回错误码

### mdb_reader_check()
**功能**: 检查过期的读者条目
**参数**: 
- `env`: 环境句柄
- `dead`: 输出参数，存储清理的条目数
**返回值**: 成功返回0，失败返回错误码

## 8. 断言回调 (Assert Callback)

### mdb_env_set_assert()
**功能**: 设置断言回调函数
**参数**: 
- `env`: 环境句柄
- `func`: 断言回调函数
**返回值**: 成功返回0，失败返回错误码

## 常用标志 (Common Flags)

### 环境打开标志
- `MDB_FIXEDMAP`: 使用固定映射
- `MDB_NOSUBDIR`: 数据库文件在当前目录
- `MDB_RDONLY`: 只读模式
- `MDB_WRITEMAP`: 使用写时映射
- `MDB_NOMETASYNC`: 不等待元数据同步
- `MDB_NOSYNC`: 不等待数据同步
- `MDB_MAPASYNC`: 异步映射
- `MDB_NOTLS`: 无线程局部存储
- `MDB_NOLOCK`: 无锁模式
- `MDB_NORDAHEAD`: 无预读
- `MDB_NOMEMINIT`: 不初始化内存

### 数据库标志
- `MDB_REVERSEKEY`: 反向键比较
- `MDB_DUPSORT`: 支持重复键
- `MDB_INTEGERKEY`: 整数键
- `MDB_DUPFIXED`: 固定大小重复数据
- `MDB_INTEGERDUP`: 整数重复数据
- `MDB_REVERSEDUP`: 反向重复数据比较
- `MDB_CREATE`: 不存在时创建

### 数据操作标志
- `MDB_NOOVERWRITE`: 不覆盖现有键
- `MDB_NODUPDATA`: 不添加重复数据
- `MDB_CURRENT`: 替换当前位置数据
- `MDB_RESERVE`: 预留数据空间
- `MDB_APPEND`: 追加数据
- `MDB_APPENDDUP`: 追加重复数据

## 错误码 (Error Codes)
- `MDB_SUCCESS`: 操作成功
- `MDB_KEYEXIST`: 键已存在
- `MDB_NOTFOUND`: 键不存在
- `MDB_PAGE_NOTFOUND`: 页面不存在
- `MDB_CORRUPTED`: 数据库损坏
- `MDB_PANIC`: 致命错误
- `MDB_VERSION_MISMATCH`: 版本不匹配
- `MDB_INVALID`: 无效参数
- `MDB_MAP_FULL`: 映射已满
- `MDB_DBS_FULL`: 数据库已满
- `MDB_READERS_FULL`: 读者已满
- `MDB_TXN_FULL`: 事务已满
- `MDB_CURSOR_FULL`: 游标已满
- `MDB_PAGE_FULL`: 页面已满
- `MDB_MAP_RESIZED`: 映射大小已调整
- `MDB_INCOMPATIBLE`: 不兼容操作
- `MDB_BAD_RSLOT`: 错误的读者槽
- `MDB_BAD_TXN`: 错误的事务
- `MDB_BAD_VALSIZE`: 错误的值大小
- `MDB_BAD_DBI`: 错误的数据库句柄

## 游标操作类型 (Cursor Operations)
- `MDB_FIRST`: 第一个键/值对
- `MDB_FIRST_DUP`: 第一个重复数据
- `MDB_GET_BOTH`: 获取特定键值对
- `MDB_GET_BOTH_RANGE`: 获取键值范围
- `MDB_GET_CURRENT`: 获取当前位置
- `MDB_GET_MULTIPLE`: 获取多个数据
- `MDB_LAST`: 最后一个键/值对
- `MDB_LAST_DUP`: 最后一个重复数据
- `MDB_NEXT`: 下一个键/值对
- `MDB_NEXT_DUP`: 下一个重复数据
- `MDB_NEXT_MULTIPLE`: 下一个多个数据
- `MDB_NEXT_NODUP`: 下一个非重复键
- `MDB_PREV`: 前一个键/值对
- `MDB_PREV_DUP`: 前一个重复数据
- `MDB_PREV_NODUP`: 前一个非重复键
- `MDB_SET`: 设置当前位置
- `MDB_SET_KEY`: 设置当前键
- `MDB_SET_RANGE`: 设置键范围

## 9. eLmdb NIF接口参数控制机制

### 概述
eLmdb NIF接口扩展了标准LMDB API，增加了参数控制机制，允许更精细地控制数据操作的行为模式。

### 参数说明

#### Flags参数
- **类型**: 无符号整数 (unsigned int)
- **功能**: 控制数据操作的具体行为
- **取值**: 标准LMDB操作标志（如MDB_NOOVERWRITE、MDB_APPEND等）

#### Mode参数
- **类型**: 整数 (int)
- **功能**: 控制操作的执行模式
- **取值**:
  - `0`: 异步无结果模式 - 操作异步执行，不返回结果
  - `1`: 异步有结果模式 - 操作异步执行，返回结果
  - `2`: 同步执行模式 - 操作同步执行，返回结果

### 支持的函数

#### 支持Flags和Mode参数的函数
- `nif_put/6` - 存储数据
- `nif_del/5` - 删除数据
- `nif_put_multi/6` - 批量存储数据
- `nif_del_multi/5` - 批量删除数据

#### 仅支持同步模式的函数（Mode参数固定为2）
- `nif_get/3` - 获取数据（GET操作只支持同步模式）
- `nif_get_multi/5` - 批量获取数据（GET操作只支持同步模式）
- `nif_drop/2` - 清空或删除数据库（DROP操作只支持同步模式）

### 使用示例

#### 同步存储数据（默认模式）
```erlang
% 同步存储数据，等待操作完成
nif_put(TxnHandle, Dbi, Key, Value, 0, 2).
```

#### 异步存储数据（无结果）
```erlang
% 异步存储数据，不等待结果
nif_put(TxnHandle, Dbi, Key, Value, 0, 0).
```

#### 带标志的存储操作
```erlang
% 使用MDB_NOOVERWRITE标志，不覆盖现有键
nif_put(TxnHandle, Dbi, Key, Value, MDB_NOOVERWRITE, 2).
```

### 注意事项

1. **GET操作限制**: GET操作（包括单值和批量）只支持同步模式，异步模式会自动降级为同步模式并输出警告。

2. **DROP操作限制**: DROP操作只支持同步模式，确保数据库操作的安全性。

3. **事务一致性**: 异步操作在事务提交时保证一致性，但可能延迟返回结果。

4. **错误处理**: 所有操作都包含完整的错误处理机制，返回标准化的错误格式。

### 性能建议

- **读密集型应用**: 使用同步模式确保数据一致性
- **写密集型应用**: 考虑使用异步模式提高吞吐量
- **混合负载**: 根据操作类型选择合适的模式

---

*文档生成时间: 2024年*  
*基于LMDB 0.9.18+版本*  
*eLmdb NIF接口扩展版本*