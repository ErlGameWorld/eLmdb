-ifndef(eLmdb_h_).
-define(eLmdb_h_, true).

%%%===================================================================
%%% env_open flags 环境标记位
%%%===================================================================
-define(MDB_FIXEDMAP, 16#01).                 %% 使用固定内存地址映射(实验性) | 可移植性差 (experimental)
-define(MDB_NOSUBDIR, 16#4000).               %% 路径直接用作数据库主数据文件 默认情况下，LMDB在其路径下创建数据文件和锁文件
-define(MDB_NOSYNC, 16#10000).                %% 不强制刷新到磁盘(提交不做 fsync；掉电可能丢最近多次提交（风险最大，但速度最快）)  **写性能 +1000%**
-define(MDB_RDONLY, 16#20000).                %% 只读模式打开环境 多进程只读 (禁止写事务)
-define(MDB_NOMETASYNC, 16#40000).            %% 不同步刷新元数据 提交不每次同步 meta 页；掉电可能丢“最后一次提交”，但库保持一致性。  写性能 +200%
-define(MDB_WRITEMAP, 16#80000).              %% 写时映射，减少一次拷贝，常能带来 1.2x~2x 写入提升
-define(MDB_MAPASYNC, 16#100000).             %% 异步刷新内存映射  配合 WRITEMAP 让 OS 更懒地刷数据；提交更快，掉电可能丢最近数据。
-define(MDB_NOTLS, 16#200000).                %% 不使用线程本地存储  读事务不绑定到线程局部存储（便于把只读 txn 传到其他线程），通常不是性能关键。
-define(MDB_NOLOCK, 16#400000).           	  %% 不使用文件锁。仅限单进程/你自己保证互斥，生产不建议。
-define(MDB_NORDAHEAD, 16#800000).            %% 禁用 readahead(Windows不生效)。随机读工作负载在 SSD 上可减少页缓存污染。
-define(MDB_NOMEMINIT, 16#1000000).           %% 不把再利用的页清零(在写入数据文件之前，不要初始化通过 malloc 分配的内存)有小幅写入增益  写性能 +10%

%%%===================================================================
%%% dbi_open flags 数据库标志位
%%%===================================================================
-define(MDB_REVERSEKEY, 16#02).       		  %% key 按逆序比较 | 特殊排序
-define(MDB_DUPSORT, 16#04).       			  %% 允许重复 key | 一对多关系 |  允许同一个 key 下存放多个 value（重复键）
-define(MDB_INTEGERKEY, 16#08).       		  %% key 是整数 | ⚡ 性能优化 |  The keys must all be of the same size.
-define(MDB_DUPFIXED, 16#10).       		  %% 重复数据大小固定 | 配合 DUPSORT
-define(MDB_INTEGERDUP, 16#20).       		  %% 重复数据是整数 | 配合 DUPSORT |
-define(MDB_REVERSEDUP, 16#40).       		  %% 重复数据逆序 | 配合 DUPSORT |
-define(MDB_CREATE, 16#40000).       		  %% 不存在则创建数据库

%%%===================================================================
%%% mdb_put flags
%%%===================================================================
-define(MDB_NOOVERWRITE, 16#10).          	  %%键已存在则不写，返回 MDB_KEYEXIST (0x10)
-define(MDB_NODUPDATA, 16#20).            	  %% 不插入重复数据对 | DUPSORT 用 (0x20)
-define(MDB_CURRENT, 16#40).                  %% 更新当前 cursor 位置 | Cursor 专用 (0x40)
-define(MDB_RESERVE, 16#10000).               %% 只预留空间，不复制数据；成功后 data->mv_data 指向可写内存，你再把字节填进去 (0x10000)
-define(MDB_APPEND, 16#20000).                %%追加模式(要求Key 递增) | ⚡ **性能 +300%** | 键按比较器严格递增时使用，能显著减少分裂、提高插入速度。顺序不对会失败(MDB_KEYEXIST) (0x20000)
-define(MDB_APPENDDUP, 16#40000).             %% 仅 DUPSORT；往某键的重复集合尾部按顺序追加 (0x40000)
-define(MDB_MULTIPLE, 16#80000).           	  %% 批量写入多个值  仅仅是 MDB_DUPFIXED dbi flags时 (0x80000)
%% 说明：MDB_CURRENT 是 mdb_cursor_put 的 flag。

%% 写/删操作同步参数
-define(DbModeAsync, 0).							%% 异步无结果 - 发送到写进程，不等待结果
-define(DbModeAsyncWait, 1).					%% 异步等待结果 - 发送到写进程，等待结果返回
-define(DbModeSyncExe, 2).						%% 同步执行 - 在当前NIF函数内直接执行
-define(DbModeSyncReturn, 3).					%% 同步执行 - 在当前NIF函数内直接执行

-define(DbTimeout, 5000).

%% nif_env_sync  Force 参数
%% - 0 ：非强制同步
%% - 1 ：强制同步
%% ## 具体行为差异：
%% ### 当 force = 0（非强制同步）：
%% - 只有在环境没有设置 MDB_NOSYNC 标志时才会执行同步操作
%% - 如果设置了 MDB_MAPASYNC 标志，会使用 MS_ASYNC 进行异步同步
%% - 这是性能优化的选项，允许系统在合适的时候进行同步
%% ### 当 force = 1（强制同步）：
%% - 无论环境是否设置了 MDB_NOSYNC 标志，都会执行同步操作
%% - 总是使用 MS_SYNC 进行同步同步
%% - 确保所有数据都立即写入磁盘，提供最强的数据持久性保证
-define(DbSyncNoeForce, 0).
-define(DbSyncForce, 1).

%%nif_env_copy Flags 参数
%% - 0: 普通复制
%% - ?MDB_CP_COMPACT (1): 压缩复制 - 省略空闲空间，顺序重新编号所有页面
-define(DbCopyNormal, 0).  %% 0: 普通复制
-define(DbCopyCompact, 1). %% 压缩复制

%% nif_drop  Force 参数
%% Mode 操作模式
%%  - 0: 清空数据库 - 删除所有数据但保留数据库结构
%%  - 1: 删除数据库 - 从环境中删除数据库并关闭句柄
-define(DbDropClear, 0).  %% 0: 清空数据库 - 删除所有数据但保留数据库结构
-define(DbDropDel, 1).    %% 1: 删除数据库 - 从环境中删除数据库并关闭句柄

%% nif_writes  SameTransaction 参数
%% SameTransaction 事务模式
%%  - 0: 不同事务 - 每个操作使用独立事务
%%  - 1: 相同事务 - 所有操作使用同一个事务
-define(DbTxnDiff, 0).  %% 不同事务 - 每个操作使用独立事务
-define(DbTxnSame, 1).  %% 相同事务 - 所有操作使用同一个事务

%% @param StartKey 起始键（可选，可以是'$TvsBegin'表示从头开始）
%% @param BatchSize 批量大小（可选，默认100）
%% @param Direction 遍历方向（0-正序，1-反序）
%% @param ReturnType 返回类型（0-键值对，1-仅键）
%% @returns {ok, list(), key() | '$TvsOver'} | {error, term()}
-define(TvsBegin, '$TvsBegin').
-define(TvsOver, '$TvsOver').

-define(DbTvsAsc, 0).    %% 0-正序
-define(DbTvsDesc, 1).	%% 1-反序

-define(DbTvsKv, 0).		%% 0-键值对
-define(DbTvsKey, 1).		%% 1-仅键
%% 默认遍历批量大小
-define(DefTvsSize, 100).

%%%===================================================================
%%% LMDB 错误码 (Error Codes)
%%%===================================================================
-define(MDB_SUCCESS, 0).                     %% 成功
-define(MDB_KEYEXIST, -30799).               %% 键值对已存在 使用 MDB_NOOVERWRITE 标志插入已存在的键 在 DUPSORT 数据库中插入重复的键值对
-define(MDB_NOTFOUND, -30798).               %% 未找到键 get 查询不存在的键  cursor 移动超出范围 del 删除不存在的键
-define(MDB_PAGE_NOTFOUND, -30797).       	 %% 页面未找到 （通常表示数据损坏） 数据库文件物理损坏 文件被截断或部分覆盖 硬盘故障 不当关闭导致的数据损坏解决方案(1. 从备份恢复 2. 如果没有备份，尝试salvage(需要工具) mdb_dump 可以导出未损坏的数据)
-define(MDB_CORRUPTED, -30796).              %% 已损坏
-define(MDB_PANIC, -30795).                  %% 发生恐慌
-define(MDB_VERSION_MISMATCH, -30794).    	 %% 版本不匹配
-define(MDB_INVALID, -30793).                %% 无效参数
-define(MDB_MAP_FULL, -30792).               %% 映射文件已满
-define(MDB_DBS_FULL, -30791).               %% 数据库已满 打开的数据库数量超上限
-define(MDB_READERS_FULL, -30790).           %% 读者已满
-define(MDB_TXN_FULL, -30789).               %% 事务已满
-define(MDB_CURSOR_FULL, -30788).            %% 游标栈已满
-define(MDB_PAGE_FULL, -30787).              %% 页面已满
-define(MDB_MAP_RESIZED, -30786).            %% 映射已调整大小
-define(MDB_INCOMPATIBLE, -30785).           %% 文件操作失败
-define(MDB_BAD_RSLOT, -30784).              %% 文件名无效
-define(MDB_BAD_TXN, -30783).                %% 设备上没有空间
-define(MDB_BAD_VALSIZE, -30781).            %% 文件太大
-define(MDB_BAD_DBI, -30780).                %% 文件不是有效的数据库
-define(MDB_PROBLEM, -30779).                %% 问题已修复

%%%===================================================================
%%% LMDB 游标操作标志位 (Cursor Operation Flags)
%%%===================================================================
-define(MDB_FIRST, 0).                        %% 游标位置：第一个数据项
-define(MDB_FIRST_DUP, 1).                    %% 游标位置：第一个重复数据项
-define(MDB_GET_BOTH, 2).                     %% 游标位置：获取当前键/数据项
-define(MDB_GET_BOTH_RANGE, 3).               %% 游标位置：获取当前键和更大的数据项
-define(MDB_GET_CURRENT, 4).                  %% 游标位置：当前键/数据项
-define(MDB_LAST, 8).                         %% 游标位置：最后一个数据项
-define(MDB_LAST_DUP, 9).                     %% 游标位置：最后一个重复数据项
-define(MDB_NEXT, 10).                        %% 游标位置：下一个数据项
-define(MDB_NEXT_DUP, 11).                    %% 游标位置：下一个重复数据项
-define(MDB_NEXT_NODUP, 12).                  %% 游标位置：下一个非重复数据项
-define(MDB_PREV, 13).                        %% 游标位置：上一个数据项
-define(MDB_PREV_DUP, 14).                    %% 游标位置：上一个重复数据项
-define(MDB_PREV_NODUP, 15).                  %% 游标位置：上一个非重复数据项
-define(MDB_SET, 16).                         %% 游标位置：设置范围
-define(MDB_SET_KEY, 17).                     %% 游标位置：设置键范围
-define(MDB_SET_RANGE, 18).                   %% 游标位置：设置范围并返回第一个大于等于的数据项

%%%===================================================================
%%% 数据结构定义
%%%===================================================================

-record(envInfo, {
	me_mapaddr :: integer()                 %%映射地址（如果固定） 数据库文件在进程虚拟内存中映射的起始地址 用于内存映射文件（mmap）技术
	, me_mapsize :: integer()               %%数据内存映射的大小
	, me_last_pgno :: integer()             %%最后使用的页面ID  用于内存映射文件（mmap）技术  实际使用空间 = (me_last_pgno + 1) × ms_psize 用于判断数据库实际占用空间
	, me_last_txnid :: integer()            %%最后提交的事务ID  最后一次提交事务的 ID。每个写事务提交都会自增。可用来判断"自某个时间点后环境是否发生过写入变化  单调递增，用于事务版本控制和MVCC
	, me_maxreaders :: integer()            %%环境中的最大读者槽数 
	, me_numreaders :: integer()            %%环境中使用的最大读者槽数  当前使用的读者数 （可能包含"僵尸读者"，比如进程异常退出遗留）。可用 mdb_reader_list / mdb_reader_check 查看与清理。
	, me_queue_size :: integer()            %%写队列当前积累数量  当前写队列中等待处理的操作数量，用于监控队列状态和背压控制
}).

-record(envStat, {
	ms_psize :: integer()                   %%数据库页面大小(通常 4096 字节)。当前所有数据库相同。
	, ms_depth :: integer()                 %%B树的深度（高度） 根到叶子的距离  深度越大，查询需要的IO次数越多 depth=1：只有根节点（数据很少）  depth=2：根节点 + 叶子节点  depth=3：根节点 + 中间节点 + 叶子节点
	, ms_branch_pages :: integer()          %%内部（非叶子）页面数 B树的内部节点（非叶子节点）数量  只包含索引信息，不存储实际数据 用于导航到叶子节点
	, ms_leaf_pages :: integer()            %%叶子页面数 B树的叶子节点数量 存储实际的键值对数据
	, ms_overflow_pages :: integer()        %%溢出页面数 存储超大键值对的额外页面 当单个键值对大于页面大小时需要 数量多说明有很多大数据项
	, ms_entries :: integer()               %%数据项数量(即记录总数)。注意对 DUPSORT 库（允许重复值）的 DB，这个是“键-值对”的总数，而不是“不同键”的数量
}).

%% 实际使用空间
%  UsedSpace = (envInfo#envInfo.me_last_pgno + 1) * envStat#envStat.ms_psize
%%%% 空间利用率
%  UsageRate = UsedSpace / envInfo#envInfo.me_mapsize * 100
%%%% 平均每条记录大小
%  AvgRecordSize = UsedSpace / envStat#envStat.ms_entries
%% 最大可用页数 ≈ me_mapsize / ms_psize。
%% 查询复杂度（IO次数）大约等于 ms_depth:
%  QueryComplexity = envStat#envStat.ms_depth
%% 索引效率（分支页与总页比例）:
%  IndexRatio = ms_branch_pages / (ms_branch_pages + ms_leaf_pages)
%% 是否有过多大对象:
%  HasManyLargeObjects = ms_overflow_pages > (ms_leaf_pages * 0.1)

-record(dbiStat, {
	ms_psize :: integer()                   %%数据库页面大小。当前所有数据库相同。
	, ms_depth :: integer()                 %%B树的深度（高度）
	, ms_branch_pages :: integer()          %%内部（非叶子）页面数
	, ms_leaf_pages :: integer()            %%叶子页面数
	, ms_overflow_pages :: integer()        %%溢出页面数
	, ms_entries :: integer()               %%数据项数
}).

-endif.
