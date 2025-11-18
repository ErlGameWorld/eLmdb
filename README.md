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