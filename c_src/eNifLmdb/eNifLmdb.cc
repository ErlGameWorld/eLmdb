#include <erl_nif.h>
#include <atomic>
#include <stdint.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>

#include "lmdb.h"

/* 配置常量 */
#define MAX_BATCH_SIZE 1000
#define MAX_BATCH_BYTES (512 * 1024) /* 512KB */
#define MAX_BATCH_TIME_NS 1000000    /* 1ms */
#define WRITE_QUEUE_SIZE 10000
#define TVS_MAX_CNT 1000

/* Resource types for LMDB handles */
static ErlNifResourceType *env_resource_type = nullptr;

ERL_NIF_TERM atomOk;
ERL_NIF_TERM atomTrue;
ERL_NIF_TERM atomFalse;
ERL_NIF_TERM atomError;
ERL_NIF_TERM atomUndefined;
ERL_NIF_TERM atomEnvInfo;
ERL_NIF_TERM atomEnvStat;
ERL_NIF_TERM atomDbiStat;
ERL_NIF_TERM atomWaitWrite;
ERL_NIF_TERM atomWriteReturn;
ERL_NIF_TERM atomNotFound;
ERL_NIF_TERM atomTvsBegin;
ERL_NIF_TERM atomTvsOver;

/* 队列命令类型 */
typedef enum {
    CMD_PUT = 1,
    CMD_DEL = 2,
    CMD_WRITES = 3
} cmd_type_t;

/* 队列命令结构 */
typedef struct write_cmd {
    // 优化内存布局，统一使用ErlNifBinary存储数据
    ErlNifBinary opData; // 序列化数据（40字节）- 根据type决定内容： 非批量操作：序列化{key, value} 批量操作：序列化操作列表
    ErlNifPid caller; // 进程ID结构（8字节）
    uint64_t requestId; // 请求ID（8字节）
    cmd_type_t type; // 枚举类型（4字节）
    MDB_dbi dbi; // 数据库句柄（4字节）
    bool isSameTxn; // 批量操作是否使用相同事务（1字节）
    bool isCall; // 是否需要回调（1字节）
} write_cmd_t;

typedef struct lfq_node {
    void *data;
    std::atomic<struct lfq_node *> next;

    // 显式删除拷贝和移动操作，因为std::atomic成员不支持这些操作
    lfq_node() = default;

    lfq_node(const lfq_node &) = delete;

    lfq_node(lfq_node &&) = delete;

    lfq_node &operator=(const lfq_node &) = delete;

    lfq_node &operator=(lfq_node &&) = delete;
} lfq_node_t;

typedef struct {
    std::atomic<lfq_node_t *> head;
    std::atomic<lfq_node_t *> tail;
    lfq_node_t *stub;
    std::atomic<size_t> count; // 队列元素计数
} lfq_t;

typedef struct {
    MDB_env *mdbEnv; /* 8 bytes (64-bit pointer) */
    ErlNifCond *writeCond; /* 8 bytes (64-bit pointer) */
    ErlNifMutex *queueMutex; /* 8 bytes (64-bit pointer) */
    ErlNifEnv *msgEnv; /* 8 bytes (64-bit pointer) */
    lfq_t *writeQueue; /* 8 bytes (64-bit pointer) */
    ErlNifTid writeTid; /* 8 bytes (pthread_t on 64-bit systems) */
    std::atomic<bool> writerStarted; /* 1 byte - 是否已创建写线程 */
    std::atomic<bool> isRunning; /* 1 byte (atomic bool) */
    /* Total: 51 bytes + padding */
} mdb_env_res_t;

/* NIF function declarations */
static ERL_NIF_TERM nif_env_open(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]);

static ERL_NIF_TERM nif_env_close(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]);

static ERL_NIF_TERM nif_env_set_mapsize(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]);

static ERL_NIF_TERM nif_env_info(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]);

static ERL_NIF_TERM nif_env_stat(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]);

static ERL_NIF_TERM nif_env_sync(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]);

static ERL_NIF_TERM nif_env_copy(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]);

static ERL_NIF_TERM nif_dbi_open(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]);

static ERL_NIF_TERM nif_dbi_stat(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]);

static ERL_NIF_TERM nif_dbi_flags(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]);

static ERL_NIF_TERM nif_put(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]);

static ERL_NIF_TERM nif_get(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]);

static ERL_NIF_TERM nif_del(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]);

static ERL_NIF_TERM nif_drop(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]);

static ERL_NIF_TERM nif_get_multi(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]);

static ERL_NIF_TERM nif_writes(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]);

/* Traversal functions */
static ERL_NIF_TERM nif_traversal(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]);

/////////////////////////////////////////////////// 无锁队列 start //////////////////////////////////////////////////////
// ==================== 初始化 ====================
int lfq_init(lfq_t *q) {
    if (!q)
        return -1;

    q->stub = (lfq_node_t *) enif_alloc(sizeof(lfq_node_t));
    if (!q->stub)
        return -1;

    q->stub->data = nullptr;
    q->stub->next = nullptr;

    q->head = q->stub;
    q->tail = q->stub;
    q->count = 0; // 初始化队列计数

    return 0;
}

// ==================== 入队 (MPMC) ====================
// 返回是否从空 -> 非空（prev == 0）
int lfq_enqueue(lfq_t *q, void *data, bool *became_nonempty) {
    if (!q || !data) return -1;

    lfq_node_t *node = (lfq_node_t *) enif_alloc(sizeof(lfq_node_t));
    if (!node) return -1;
    node->data = data;
    node->next = nullptr;

    lfq_node_t *tail, *next;
    for (;;) {
        tail = q->tail.load(std::memory_order_acquire);
        next = tail->next.load(std::memory_order_acquire);
        // 验证 tail 仍然有效
        if (tail != q->tail.load(std::memory_order_acquire)) continue;

        if (next == nullptr) {
            // 尝试链接新节点
            if (tail->next.compare_exchange_weak(next, node,
                                                 std::memory_order_release,
                                                 std::memory_order_relaxed)) {
                break;
            }
        } else {
            // 帮助推进 tail
            q->tail.compare_exchange_weak(tail, next,
                                          std::memory_order_release,
                                          std::memory_order_relaxed);
        }
    }
    // 尝试更新 tail
    q->tail.compare_exchange_strong(tail, node,
                                    std::memory_order_release,
                                    std::memory_order_relaxed);

    // 增加计数，检查是否从 0 -> 1
    size_t prev = q->count.fetch_add(1, std::memory_order_relaxed);
    if (became_nonempty) *became_nonempty = (prev == 0);
    return 0;
}

// ==================== 出队 (MPMC) ====================

void *lfq_dequeue(lfq_t *q) {
    if (!q)
        return nullptr;

    lfq_node_t *head, *tail, *next;
    void *data = nullptr;

    while (1) {
        head = q->head.load(std::memory_order_acquire);
        tail = q->tail.load(std::memory_order_acquire);
        next = head->next.load(std::memory_order_acquire);

        // 验证 head
        if (head != q->head.load(std::memory_order_acquire)) {
            continue;
        }

        if (head == tail) {
            if (next == nullptr) {
                // 队列为空
                return nullptr;
            }
            // tail 落后
            q->tail.compare_exchange_weak(tail, next, std::memory_order_release, std::memory_order_relaxed);
        } else {
            if (next == nullptr) {
                continue;
            }

            data = next->data;

            if (q->head.compare_exchange_strong(head, next, std::memory_order_acq_rel, std::memory_order_relaxed)) {
                break;
            }
        }
    }

    // 释放旧 head
    if (head != q->stub) {
        enif_free(head);
    }

    // 减少队列计数（使用acquire语义，确保看到最新的计数状态）
    if (data != nullptr) {
        q->count.fetch_sub(1, std::memory_order_relaxed);
    }

    return data;
}

// ==================== 获取队列大小 ====================
size_t lfq_size(lfq_t *q) {
    if (!q)
        return 0;
    return q->count.load(std::memory_order_relaxed);
}

// ==================== 销毁 (关键！) ====================
void lfq_destroy(lfq_t *q) {
    if (!q) return;

    // 1) 先把队列中的数据全部出队并释放
    write_cmd_t *cmd;
    while ((cmd = (write_cmd_t *) lfq_dequeue(q)) != nullptr) {
        if (cmd->opData.data) {
            enif_release_binary(&cmd->opData);
        }
        enif_free(cmd);
    }

    // 2) 释放剩余节点（最后一个 head 节点 + 初始 stub）
    lfq_node_t *head = q->head.load(std::memory_order_relaxed);
    lfq_node_t *stub = q->stub;

    if (head && head != stub) {
        enif_free(head);
    }
    if (stub) {
        enif_free(stub);
    }

    q->head.store(nullptr, std::memory_order_relaxed);
    q->tail.store(nullptr, std::memory_order_relaxed);
    q->stub = nullptr;
    q->count.store(0, std::memory_order_relaxed);
}

/////////////////////////////////////////////////// 无锁队列 end //////////////////////////////////////////////////////
/////////////////////////////////////////////////// 辅助构建 start ////////////////////////////////////////////////////
/* Helper function to create error tuple */
static inline ERL_NIF_TERM make_error(ErlNifEnv *env, const char *error_msg) {
    return enif_make_tuple2(env, atomError, enif_make_string(env, error_msg, ERL_NIF_LATIN1));
}

/* Helper function to create error tuple with LMDB error code */
static inline ERL_NIF_TERM make_lmdb_error(ErlNifEnv *env, int error_code) {
    // 直接返回错误码，避免Windows下GBK编码问题
    // const char *error_msg = mdb_strerror(error_code);
    // return enif_make_tuple2(env, atomError, enif_make_string(env, error_msg, ERL_NIF_LATIN1));

    ERL_NIF_TERM error_term = enif_make_int(env, error_code);
    return enif_make_tuple2(env, atomError, error_term);
}

/* Helper function to create ok tuple with result */
static inline ERL_NIF_TERM make_ok_result(ErlNifEnv *env, ERL_NIF_TERM result) {
    return enif_make_tuple2(env, atomOk, result);
}

/////////////////////////////////////////////////// 辅助构建 end //////////////////////////////////////////////////////


// 通用的入队函数：PUT/DELETE 用同一个函数，减少重复代码
static int enqueue_write_cmd(mdb_env_res_t *env_res, cmd_type_t type, MDB_dbi dbi, ErlNifBinary *opData, bool isSameTxn,
                             ErlNifPid *caller, uint64_t requestId) {
    // 检查队列大小，如果超过阈值则拒绝入队（背压机制）
    size_t current_size = lfq_size(env_res->writeQueue);
    if (current_size >= WRITE_QUEUE_SIZE) {
        return -2; // 队列已满，返回特定错误码
    }

    if (!env_res->isRunning.load(std::memory_order_acquire) || env_res->writeQueue == nullptr) {
        return -3; // 已关闭
    }

    write_cmd_t *cmd = (write_cmd_t *) enif_alloc(sizeof(write_cmd_t));
    if (!cmd) {
        return -1;
    }

    cmd->type = type;
    cmd->dbi = dbi;
    cmd->isSameTxn = isSameTxn;
    cmd->opData = *opData;

    if (caller) {
        cmd->caller = *caller;
        cmd->requestId = requestId;
        cmd->isCall = true;
    } else {
        cmd->requestId = 0;
        cmd->isCall = false;
    }
    bool wake = false;
    if (lfq_enqueue(env_res->writeQueue, cmd, &wake) != 0) {
        enif_free(cmd);
        return -1;
    }

    // 只在"从空变非空"时发信号
    if (wake) {
        enif_mutex_lock(env_res->queueMutex);
        enif_cond_signal(env_res->writeCond);
        enif_mutex_unlock(env_res->queueMutex);
    }

    return 0;
}

// put 函数
static ERL_NIF_TERM deal_put(ErlNifEnv *env, MDB_env *mdb_env, const MDB_dbi dbi, const ERL_NIF_TERM *key_term,
                             const ERL_NIF_TERM *value_term, const unsigned int flags) {
    MDB_val key, value;
    ErlNifBinary KeyBin;
    if (!enif_term_to_binary(env, *key_term, &KeyBin))
        return make_error(env, "error convert key to binary");

    ErlNifBinary ValueBin;
    if (!enif_term_to_binary(env, *value_term, &ValueBin)) {
        enif_release_binary(&KeyBin);
        return make_error(env, "error convert value to binary");
    }

    key.mv_size = KeyBin.size;
    key.mv_data = KeyBin.data;
    value.mv_size = ValueBin.size;
    value.mv_data = ValueBin.data;
    // 对于同步执行，需要创建事务
    MDB_txn *txn;
    int rc = mdb_txn_begin(mdb_env, nullptr, 0, &txn);
    if (rc != MDB_SUCCESS) {
        enif_release_binary(&KeyBin);
        enif_release_binary(&ValueBin);
        return make_lmdb_error(env, rc);
    }

    rc = mdb_put(txn, dbi, &key, &value, flags);
    if (rc != MDB_SUCCESS) {
        mdb_txn_abort(txn);
        enif_release_binary(&KeyBin);
        enif_release_binary(&ValueBin);
        return make_lmdb_error(env, rc);
    }

    rc = mdb_txn_commit(txn);
    if (rc != MDB_SUCCESS) {
        /* 事务提交失败，事务已被内部abort，只需清理本地资源 */
        enif_release_binary(&KeyBin);
        enif_release_binary(&ValueBin);
        return make_lmdb_error(env, rc);
    }
    enif_release_binary(&KeyBin);
    enif_release_binary(&ValueBin);
    return atomOk;
}

// del 函数
static ERL_NIF_TERM deal_del(ErlNifEnv *env, MDB_env *mdb_env, const MDB_dbi dbi, const ERL_NIF_TERM *key_term) {
    // 使用单个写事务来确保原子性
    MDB_val key;
    int rc;
    ErlNifBinary KeyBin;
    if (!enif_term_to_binary(env, *key_term, &KeyBin))
        return make_error(env, "error convert key to binary");

    key.mv_size = KeyBin.size;
    key.mv_data = KeyBin.data;
    // 对于同步执行，需要创建事务
    MDB_txn *txn;
    rc = mdb_txn_begin(mdb_env, nullptr, 0, &txn);
    if (rc != MDB_SUCCESS) {
        enif_release_binary(&KeyBin);
        return make_lmdb_error(env, rc);
    }

    rc = mdb_del(txn, dbi, &key, nullptr);
    if (rc == MDB_SUCCESS) {
        enif_release_binary(&KeyBin);
        rc = mdb_txn_commit(txn);
        if (rc == MDB_SUCCESS) {
            return atomOk;
        } else {
            /* 事务提交失败，事务已被内部abort */
            return make_lmdb_error(env, rc);
        }
    } else if (rc == MDB_NOTFOUND) {
        mdb_txn_abort(txn);
        enif_release_binary(&KeyBin);
        return enif_make_tuple2(env, atomError, atomNotFound);
    } else {
        mdb_txn_abort(txn);
        enif_release_binary(&KeyBin);
        return make_lmdb_error(env, rc);
    }
}

// del return 函数
static ERL_NIF_TERM deal_del_return(ErlNifEnv *env, MDB_env *mdb_env, const MDB_dbi dbi, const ERL_NIF_TERM *key_term) {
    // 使用单个写事务来确保原子性
    MDB_txn *txn;
    MDB_val key, value;
    ErlNifBinary KeyBin;
    ERL_NIF_TERM ValeTerm;
    int rc;
    if (!enif_term_to_binary(env, *key_term, &KeyBin))
        return make_error(env, "error convert key to binary");

    key.mv_size = KeyBin.size;
    key.mv_data = KeyBin.data;

    rc = mdb_txn_begin(mdb_env, nullptr, 0, &txn);
    if (rc != MDB_SUCCESS) {
        enif_release_binary(&KeyBin);
        return make_lmdb_error(env, rc);
    }

    // 在同一个事务中先获取值
    rc = mdb_get(txn, dbi, &key, &value);
    if (rc == MDB_SUCCESS) {
        // 先复制数据并转换为Erlang term
        if (!enif_binary_to_term(env, (const unsigned char *) value.mv_data, value.mv_size, &ValeTerm, 0)) {
            mdb_txn_abort(txn);
            enif_release_binary(&KeyBin);
            return make_error(env, "error convert value to term");
        }

        // 成功获取到值，现在删除
        rc = mdb_del(txn, dbi, &key, nullptr);
        if (rc != MDB_SUCCESS) {
            mdb_txn_abort(txn);
            enif_release_binary(&KeyBin);
            return make_lmdb_error(env, rc);
        }

        // 提交事务
        rc = mdb_txn_commit(txn);
        if (rc != MDB_SUCCESS) {
            /* 事务提交失败，事务已被内部abort */
            enif_release_binary(&KeyBin);
            return make_lmdb_error(env, rc);
        }
        enif_release_binary(&KeyBin);
        return enif_make_tuple2(env, atomOk, ValeTerm);
    } else if (rc == MDB_NOTFOUND) {
        // 键不存在，回滚事务
        mdb_txn_abort(txn);
        enif_release_binary(&KeyBin);
        return enif_make_tuple2(env, atomError, atomNotFound);
    } else {
        // 其他错误，回滚事务
        mdb_txn_abort(txn);
        enif_release_binary(&KeyBin);
        return make_lmdb_error(env, rc);
    }
}

// write 函数
static ERL_NIF_TERM deal_writes_same_txn(ErlNifEnv *env, MDB_env *mdb_env, const ERL_NIF_TERM *Writes) {
    ERL_NIF_TERM head, tail = *Writes;
    const ERL_NIF_TERM *tuple;
    /* 相同事务模式 - 所有操作在同一个事务中执行 */
    MDB_txn *txn = nullptr;
    MDB_val key, value;
    int tuple_size;
    unsigned int flags = 0;
    MDB_dbi dbi;
    int rc;
    /* 创建写事务 */
    rc = mdb_txn_begin(mdb_env, nullptr, 0, &txn);
    if (rc != MDB_SUCCESS) {
        return make_lmdb_error(env, rc);
    }
    /* 处理所有操作项 */
    while (enif_get_list_cell(env, tail, &head, &tail) != 0) {
        /* 检查操作项格式 */
        if (!enif_get_tuple(env, head, &tuple_size, &tuple)) {
            mdb_txn_abort(txn);
            return make_error(env, "invalid operation item format, expected {DbiRes, Key, Value}");
        }
        if (tuple_size == 3 || tuple_size == 4) {
            /* 获取数据库索引 */

            if (!enif_get_uint(env, tuple[0], (unsigned int *) &dbi)) {
                mdb_txn_abort(txn);
                return make_error(env, "invalid database index");
            }

            if (tuple_size == 4) {
                if (!enif_get_uint(env, tuple[3], &flags)) {
                    return make_error(env, "invalid flags");
                }
            } else {
                flags = 0;
            }

            /* 获取键并转换为二进制 */
            ErlNifBinary KeyBin;
            if (!enif_term_to_binary(env, tuple[1], &KeyBin)) {
                mdb_txn_abort(txn);
                return make_error(env, "error convert key to binary");
            }

            /* 获取值并转换为二进制 */
            ErlNifBinary ValueBin;
            if (!enif_term_to_binary(env, tuple[2], &ValueBin)) {
                mdb_txn_abort(txn);
                enif_release_binary(&KeyBin);
                return make_error(env, "error convert value to binary");
            }

            key.mv_size = KeyBin.size;
            key.mv_data = KeyBin.data;
            value.mv_size = ValueBin.size;
            value.mv_data = ValueBin.data;

            /* 执行插入操作 */
            rc = mdb_put(txn, dbi, &key, &value, flags);
            if (rc != MDB_SUCCESS) {
                mdb_txn_abort(txn);
                enif_release_binary(&KeyBin);
                enif_release_binary(&ValueBin);
                return make_lmdb_error(env, rc);
            }

            enif_release_binary(&KeyBin);
            enif_release_binary(&ValueBin);
        } else if (tuple_size == 2) {
            //删除操作 {Dbi, Key}
            if (!enif_get_uint(env, tuple[0], (unsigned int *) &dbi)) {
                mdb_txn_abort(txn);
                return make_error(env, "invalid database index");
            }

            /* 获取键并转换为二进制 */
            ErlNifBinary KeyBin;
            if (!enif_term_to_binary(env, tuple[1], &KeyBin)) {
                mdb_txn_abort(txn);
                return make_error(env, "error convert key to binary");
            }
            key.mv_size = KeyBin.size;
            key.mv_data = KeyBin.data;

            /* 执行插入操作 */
            rc = mdb_del(txn, dbi, &key, nullptr);
            if (rc != MDB_SUCCESS && rc != MDB_NOTFOUND) {
                mdb_txn_abort(txn);
                enif_release_binary(&KeyBin);
                return make_lmdb_error(env, rc);
            }
            enif_release_binary(&KeyBin);
        } else {
            mdb_txn_abort(txn);
            return make_error(env, "invalid operation item format");
        }
    }

    /* 所有操作完成后提交事务 */
    rc = mdb_txn_commit(txn);
    if (rc != MDB_SUCCESS) {
        /* 事务提交失败，事务已被内部abort */
        return make_lmdb_error(env, rc);
    }

    return atomOk;
}

// write 函数
static ERL_NIF_TERM deal_writes_diff_txn(ErlNifEnv *env, MDB_env *mdb_env, const ERL_NIF_TERM *Writes) {
    /* 不同事务模式 - 每个操作在自己的事务中执行 失败了互不影响 */
    MDB_txn *txn = nullptr;
    MDB_val key, value;
    int tuple_size;
    unsigned int flags = 0;
    const ERL_NIF_TERM *tuple;
    MDB_dbi dbi;
    int rc;
    ERL_NIF_TERM head, tail = *Writes;

    /* 处理所有操作项 */
    while (enif_get_list_cell(env, tail, &head, &tail) != 0) {
        /* 检查操作项格式 */
        if (!enif_get_tuple(env, head, &tuple_size, &tuple)) {
            return make_error(env, "invalid operation item format, expected {DbiRes, Key, Value}");
        }
        if (tuple_size == 3 || tuple_size == 4) {
            /* 获取数据库索引 */
            if (!enif_get_uint(env, tuple[0], (unsigned int *) &dbi)) {
                return make_error(env, "invalid database index");
            }

            if (tuple_size == 4) {
                if (!enif_get_uint(env, tuple[3], &flags)) {
                    return make_error(env, "invalid flags");
                }
            } else {
                flags = 0;
            }

            /* 获取键并转换为二进制 */
            ErlNifBinary KeyBin;
            if (!enif_term_to_binary(env, tuple[1], &KeyBin)) {
                return make_error(env, "error convert key to binary");
            }

            /* 获取值并转换为二进制 */
            ErlNifBinary ValueBin;
            if (!enif_term_to_binary(env, tuple[2], &ValueBin)) {
                enif_release_binary(&KeyBin);
                return make_error(env, "error convert value to binary");
            }

            key.mv_size = KeyBin.size;
            key.mv_data = KeyBin.data;
            value.mv_size = ValueBin.size;
            value.mv_data = ValueBin.data;
            /* 创建写事务 */
            rc = mdb_txn_begin(mdb_env, nullptr, 0, &txn);
            if (rc != MDB_SUCCESS) {
                enif_release_binary(&KeyBin);
                enif_release_binary(&ValueBin);
                return make_lmdb_error(env, rc);
            }
            /* 执行插入操作 */
            rc = mdb_put(txn, dbi, &key, &value, flags);
            if (rc != MDB_SUCCESS) {
                mdb_txn_abort(txn);
                enif_release_binary(&KeyBin);
                enif_release_binary(&ValueBin);
                return make_lmdb_error(env, rc);
            }

            enif_release_binary(&KeyBin);
            enif_release_binary(&ValueBin);
            /* 提交事务 */
            rc = mdb_txn_commit(txn);
            if (rc != MDB_SUCCESS) {
                /* 事务提交失败，事务已被内部abort */
                return make_lmdb_error(env, rc);
            }
        } else if (tuple_size == 2) {
            if (!enif_get_uint(env, tuple[0], (unsigned int *) &dbi)) {
                return make_error(env, "invalid database index");
            }

            /* 获取键并转换为二进制 */
            ErlNifBinary KeyBin;
            if (!enif_term_to_binary(env, tuple[1], &KeyBin)) {
                return make_error(env, "error convert key to binary");
            }

            key.mv_size = KeyBin.size;
            key.mv_data = KeyBin.data;
            rc = mdb_txn_begin(mdb_env, nullptr, 0, &txn);
            if (rc != MDB_SUCCESS) {
                enif_release_binary(&KeyBin);
                return make_lmdb_error(env, rc);
            }
            /* 执行插入操作 */
            rc = mdb_del(txn, dbi, &key, nullptr);
            if (rc != MDB_SUCCESS && rc != MDB_NOTFOUND) {
                mdb_txn_abort(txn);
                enif_release_binary(&KeyBin);
                return make_lmdb_error(env, rc);
            }
            enif_release_binary(&KeyBin);
            /* 提交事务 */
            rc = mdb_txn_commit(txn);
            if (rc != MDB_SUCCESS) {
                /* 事务提交失败，事务已被内部abort */
                return make_lmdb_error(env, rc);
            }
        } else {
            return make_error(env, "invalid operation item format");
        }
    }

    return atomOk;
}


/* 写线程函数 */
static void *writer_thread_func(void *arg) {
    mdb_env_res_t *env_res = (mdb_env_res_t *) arg;
    ERL_NIF_TERM op_term;
    ERL_NIF_TERM result_term;
    ERL_NIF_TERM send_term;
    const ERL_NIF_TERM *tuple;
    unsigned int flags = 0;
    int tuple_size;
    MDB_dbi dbi;

    for (;;) {
        // ✅ 改为无限循环
        write_cmd_t *cmd = nullptr;

        enif_mutex_lock(env_res->queueMutex);

        // 尝试出队
        cmd = (write_cmd_t *) lfq_dequeue(env_res->writeQueue);

        // 谓词循环：队列空 且 仍在运行 → 等待
        while (cmd == nullptr && env_res->isRunning.load(std::memory_order_acquire)) {
            enif_cond_wait(env_res->writeCond, env_res->queueMutex);
            cmd = (write_cmd_t *) lfq_dequeue(env_res->writeQueue);
        }

        // 退出条件：队列空 且 已关闭
        if (cmd == nullptr && !env_res->isRunning.load(std::memory_order_acquire)) {
            enif_mutex_unlock(env_res->queueMutex);
            break; // ✅ 优雅退出
        }

        enif_mutex_unlock(env_res->queueMutex);

        // 到这里，cmd 保证不为 nullptr
        switch (cmd->type) {
            case CMD_PUT: {
                dbi = cmd->dbi;

                if (!enif_binary_to_term(env_res->msgEnv, cmd->opData.data, cmd->opData.size, &op_term, 0)) {
                    if (cmd->isCall) {
                        ERL_NIF_TERM error = make_error(env_res->msgEnv, "binary_to_term_failed");
                        send_term = enif_make_tuple3(env_res->msgEnv, atomWriteReturn,
                                                     enif_make_uint64(env_res->msgEnv, cmd->requestId), error);
                        enif_send(nullptr, &cmd->caller, env_res->msgEnv, send_term);
                    }
                    goto cleanup;
                }

                if (!enif_get_tuple(env_res->msgEnv, op_term, &tuple_size, &tuple)) {
                    if (cmd->isCall) {
                        ERL_NIF_TERM error = make_error(env_res->msgEnv, "get_tuple_failed");
                        send_term = enif_make_tuple3(env_res->msgEnv, atomWriteReturn,
                                                     enif_make_uint64(env_res->msgEnv, cmd->requestId), error);
                        enif_send(nullptr, &cmd->caller, env_res->msgEnv, send_term);
                    }
                    goto cleanup;
                }

                enif_get_uint(env_res->msgEnv, tuple[2], &flags);
                result_term = deal_put(env_res->msgEnv, env_res->mdbEnv, dbi, &tuple[0], &tuple[1], flags);

                if (cmd->isCall) {
                    send_term = enif_make_tuple3(env_res->msgEnv, atomWriteReturn,
                                                 enif_make_uint64(env_res->msgEnv, cmd->requestId), result_term);
                    enif_send(nullptr, &cmd->caller, env_res->msgEnv, send_term);
                }
                break;
            }

            case CMD_DEL: {
                dbi = cmd->dbi;

                if (!enif_binary_to_term(env_res->msgEnv, cmd->opData.data, cmd->opData.size, &op_term, 0)) {
                    if (cmd->isCall) {
                        ERL_NIF_TERM error = make_error(env_res->msgEnv, "binary_to_term_failed");
                        send_term = enif_make_tuple3(env_res->msgEnv, atomWriteReturn,
                                                     enif_make_uint64(env_res->msgEnv, cmd->requestId), error);
                        enif_send(nullptr, &cmd->caller, env_res->msgEnv, send_term);
                    }
                    goto cleanup;
                }

                result_term = deal_del(env_res->msgEnv, env_res->mdbEnv, dbi, &op_term);

                if (cmd->isCall) {
                    send_term = enif_make_tuple3(env_res->msgEnv, atomWriteReturn,
                                                 enif_make_uint64(env_res->msgEnv, cmd->requestId), result_term);
                    enif_send(nullptr, &cmd->caller, env_res->msgEnv, send_term);
                }
                break;
            }

            case CMD_WRITES: {
                if (!enif_binary_to_term(env_res->msgEnv, cmd->opData.data, cmd->opData.size, &op_term, 0)) {
                    if (cmd->isCall) {
                        ERL_NIF_TERM error = make_error(env_res->msgEnv, "binary_to_term_failed");
                        send_term = enif_make_tuple3(env_res->msgEnv, atomWriteReturn,
                                                     enif_make_uint64(env_res->msgEnv, cmd->requestId), error);
                        enif_send(nullptr, &cmd->caller, env_res->msgEnv, send_term);
                    }
                    goto cleanup;
                }

                if (cmd->isSameTxn) {
                    result_term = deal_writes_same_txn(env_res->msgEnv, env_res->mdbEnv, &op_term);
                } else {
                    result_term = deal_writes_diff_txn(env_res->msgEnv, env_res->mdbEnv, &op_term);
                }

                if (cmd->isCall) {
                    send_term = enif_make_tuple3(env_res->msgEnv, atomWriteReturn,
                                                 enif_make_uint64(env_res->msgEnv, cmd->requestId), result_term);
                    enif_send(nullptr, &cmd->caller, env_res->msgEnv, send_term);
                }
                break;
            }
        }

    cleanup:
        enif_clear_env(env_res->msgEnv);
        enif_release_binary(&cmd->opData);
        enif_free(cmd);
    }

#ifdef DEBUG_LMDB
    fprintf(stderr, "[LMDB Writer] Thread stopped for env %p\n", env_res->mdbEnv);
#endif

    return nullptr;
}

/* NIF: env_open */
static ERL_NIF_TERM nif_env_open(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    /* Check argument count */
    if (argc != 5) {
        return make_error(env, "invalid_argument_count");
    }

    char path[1024];
    uint64_t maps_size;
    unsigned int max_dbs, max_readers;
    unsigned int flags = 0;

    /* Get path string */
    if (!enif_get_string(env, argv[0], path, sizeof(path), ERL_NIF_UTF8)) {
        return make_error(env, "invalid_path");
    }

    /* Get maps_size */
    if (!enif_get_uint64(env, argv[1], &maps_size)) {
        return make_error(env, "invalid_maps_size");
    }

    /* Get max_dbs */
    if (!enif_get_uint(env, argv[2], &max_dbs)) {
        return make_error(env, "invalid_max_dbs");
    }

    /* Get max_readers */
    if (!enif_get_uint(env, argv[3], &max_readers)) {
        return make_error(env, "invalid_max_readers");
    }

    /* Get max_readers */
    if (!enif_get_uint(env, argv[4], &flags)) {
        return make_error(env, "invalid_flags");
    }

    MDB_env *mdb_env;
    int rc;
    rc = mdb_env_create(&mdb_env);
    if (rc != MDB_SUCCESS) {
        return make_lmdb_error(env, rc);
    }

    /* Set env options */
    rc = mdb_env_set_mapsize(mdb_env, (size_t) maps_size);
    if (rc != MDB_SUCCESS) {
        mdb_env_close(mdb_env);
        return make_lmdb_error(env, rc);
    }

    /* Set env options */
    rc = mdb_env_set_maxdbs(mdb_env, max_dbs);
    if (rc != MDB_SUCCESS) {
        mdb_env_close(mdb_env);
        return make_lmdb_error(env, rc);
    }

    rc = mdb_env_set_maxreaders(mdb_env, max_readers);
    if (rc != MDB_SUCCESS) {
        mdb_env_close(mdb_env);
        return make_lmdb_error(env, rc);
    }

    rc = mdb_env_open(mdb_env, path, flags, 0664);
    if (rc != MDB_SUCCESS) {
        mdb_env_close(mdb_env);
        return make_lmdb_error(env, rc);
    }

    // 创建环境资源
    mdb_env_res_t *env_res = (mdb_env_res_t *) enif_alloc_resource(env_resource_type, sizeof(mdb_env_res_t));
    if (env_res == nullptr) {
        mdb_env_close(mdb_env);
        return make_error(env, "alloc_resource");
    }

    // 预初始化资源字段
    env_res -> mdbEnv = nullptr;
    env_res ->writeCond = nullptr;
    env_res ->queueMutex = nullptr;
    env_res ->msgEnv = nullptr;
    env_res ->writeQueue = nullptr;
    env_res -> writeTid = nullptr;
    env_res->writerStarted.store(false, std::memory_order_relaxed);
    env_res->isRunning.store(false, std::memory_order_release);

    // 初始化环境资源
    env_res->mdbEnv = mdb_env;

    // 初始化无锁队列
    env_res->writeQueue = (lfq_t *) enif_alloc(sizeof(lfq_t));
    if (env_res->writeQueue == nullptr) {
        enif_release_resource(env_res);
        return make_error(env, "queue_alloc");
    }

    if (lfq_init(env_res->writeQueue) != 0) {
        enif_free(env_res->writeQueue);
        env_res->writeQueue = nullptr; // ✅ 防止 destructor 重复释放
        enif_release_resource(env_res);
        return make_error(env, "queue_init");
    }

    // 初始化Erlang NIF同步原语
    env_res->writeCond = enif_cond_create((char *) "lmdb_write_cond");
    if (env_res->writeCond == nullptr) {
        enif_release_resource(env_res);
        return make_error(env, "cond_create");
    }

    env_res->queueMutex = enif_mutex_create((char *) "lmdb_write_mutex");
    if (env_res->queueMutex == nullptr) {
        enif_release_resource(env_res);
        return make_error(env, "mutex_create");
    }

    env_res->msgEnv = enif_alloc_env();
    if (env_res->msgEnv == nullptr) {
        enif_release_resource(env_res);
        return make_error(env, "alloc_env");
    }
    // 初始化线程状态标志
    env_res->writerStarted.store(false, std::memory_order_relaxed);
    env_res->isRunning.store(true, std::memory_order_release);

    // 创建写线程
    rc = enif_thread_create((char *) "lmdb_writer", &(env_res->writeTid), writer_thread_func, env_res, nullptr);
    if (rc != 0) {
        env_res->isRunning.store(false, std::memory_order_release);
        enif_release_resource(env_res);
        return make_error(env, "thread_create");
    }
    // 线程创建成功，设置标志
    env_res->writerStarted.store(true, std::memory_order_release);

    const ERL_NIF_TERM RefTerm = enif_make_resource(env, env_res);
    enif_release_resource(env_res); // 让Erlang管理内存
    return enif_make_tuple2(env, atomOk, RefTerm);
}

/* NIF: env_close */
static ERL_NIF_TERM nif_env_close(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    /* Check argument count */
    if (argc != 1) {
        return make_error(env, "invalid_argument_count");
    }

    mdb_env_res_t *env_res;

    /* Get env resource */
    if (!enif_get_resource(env, argv[0], env_resource_type, (void **) &env_res)) {
        return make_error(env, "invalid env handle");
    }

    // 标记环境为关闭状态
    env_res->isRunning.store(false, std::memory_order_release);

    // 唤醒可能正在等待的写线程
    if (env_res->queueMutex && env_res->writeCond) {
        enif_mutex_lock(env_res->queueMutex);
        enif_cond_broadcast(env_res->writeCond);
        enif_mutex_unlock(env_res->queueMutex);
    }

    // 等待写线程退出
    if (env_res->writerStarted.exchange(false, std::memory_order_acq_rel)) {
        void *thread_result;
        enif_thread_join(env_res->writeTid, &thread_result);
    }

    // 写线程已join后，立即清理所有资源
    if (env_res->writeQueue) {
        // 清理队列中剩余的命令
        write_cmd_t *cmd;
        while ((cmd = (write_cmd_t *) lfq_dequeue(env_res->writeQueue)) != nullptr) {
            // 释放opData二进制数据
            if (cmd->opData.data) {
                enif_release_binary(&cmd->opData);
            }
            // 释放命令结构本身
            enif_free(cmd);
        }
        // 销毁队列并释放队列结构体
        lfq_destroy(env_res->writeQueue);
        enif_free(env_res->writeQueue);
        env_res->writeQueue = nullptr;
    }

    if (env_res->msgEnv) {
        enif_free_env(env_res->msgEnv);
        env_res->msgEnv = nullptr;
    }

    if (env_res->queueMutex) {
        enif_mutex_destroy(env_res->queueMutex);
        env_res->queueMutex = nullptr;
    }

    if (env_res->writeCond) {
        enif_cond_destroy(env_res->writeCond);
        env_res->writeCond = nullptr;
    }

    if (env_res->mdbEnv) {
        mdb_env_close(env_res->mdbEnv);
        env_res->mdbEnv = nullptr;
    }

    return atomOk;
}

/* NIF: env_set_mapsize */
static ERL_NIF_TERM nif_env_set_mapsize(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    /* Check argument count */
    if (argc != 2) {
        return make_error(env, "invalid_argument_count");
    }

    mdb_env_res_t *env_res;
    int rc;

    /* Get env resource */
    if (!enif_get_resource(env, argv[0], env_resource_type, (void **) &env_res)) {
        return make_error(env, "invalid env handle");
    }

    /* Get size */
    uint64_t maps_size;
    if (!enif_get_uint64(env, argv[1], &maps_size)) {
        return make_error(env, "invalid size");
    }

    rc = mdb_env_set_mapsize(env_res->mdbEnv, (size_t) maps_size);
    if (rc != MDB_SUCCESS) {
        return make_lmdb_error(env, rc);
    }

    return atomOk;
}

/* NIF: env_info */
static ERL_NIF_TERM nif_env_info(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    /* Check argument count */
    if (argc != 1) {
        return make_error(env, "invalid_argument_count");
    }

    mdb_env_res_t *env_res;

    /* Get env resource */
    if (!enif_get_resource(env, argv[0], env_resource_type, (void **) &env_res)) {
        return make_error(env, "invalid env handle");
    }

    /* Check if env is closed */
    if (env_res->mdbEnv == nullptr) {
        return make_error(env, "env is closed");
    }

    MDB_envinfo info;
    int rc;
    rc = mdb_env_info(env_res->mdbEnv, &info);
    if (rc != MDB_SUCCESS) {
        return make_lmdb_error(env, rc);
    }

    /* Create result tuple with env info */
    ERL_NIF_TERM map_addr = enif_make_uint64(env, (uint64_t) (uintptr_t) info.me_mapaddr);
    ERL_NIF_TERM map_size = enif_make_uint64(env, (uint64_t) info.me_mapsize);
    ERL_NIF_TERM last_pgno = enif_make_uint64(env, (uint64_t) info.me_last_pgno);
    ERL_NIF_TERM last_txnid = enif_make_uint64(env, (uint64_t) info.me_last_txnid);
    ERL_NIF_TERM max_readers = enif_make_uint64(env, (uint64_t) info.me_maxreaders);
    ERL_NIF_TERM num_readers = enif_make_uint64(env, (uint64_t) info.me_numreaders);

    /* Get queue size */
    size_t queue_size = 0;
    if (env_res->writeQueue != nullptr) {
        queue_size = lfq_size(env_res->writeQueue);
    }
    ERL_NIF_TERM queue_size_term = enif_make_uint64(env, (uint64_t) queue_size);

    const ERL_NIF_TERM result = enif_make_tuple8(env, atomEnvInfo, map_addr, map_size, last_pgno, last_txnid, max_readers,
                                                 num_readers, queue_size_term);
    return make_ok_result(env, result);
}

/* NIF: env_stat */
static ERL_NIF_TERM nif_env_stat(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    /* Check argument count */
    if (argc != 1) {
        return make_error(env, "invalid_argument_count");
    }

    mdb_env_res_t *env_res;
    /* Get env resource */
    if (!enif_get_resource(env, argv[0], env_resource_type, (void **) &env_res)) {
        return make_error(env, "invalid env handle");
    }

    /* Check if env is closed */
    if (env_res->mdbEnv == nullptr) {
        return make_error(env, "env is closed");
    }

    MDB_stat stat;
    int rc;
    rc = mdb_env_stat(env_res->mdbEnv, &stat);
    if (rc != MDB_SUCCESS) {
        return make_lmdb_error(env, rc);
    }

    /* Create result tuple with env statistics */
    ERL_NIF_TERM psize = enif_make_uint64(env, (uint64_t) stat.ms_psize);
    ERL_NIF_TERM depth = enif_make_uint64(env, (uint64_t) stat.ms_depth);
    ERL_NIF_TERM branch_pages = enif_make_uint64(env, (uint64_t) stat.ms_branch_pages);
    ERL_NIF_TERM leaf_pages = enif_make_uint64(env, (uint64_t) stat.ms_leaf_pages);
    ERL_NIF_TERM overflow_pages = enif_make_uint64(env, (uint64_t) stat.ms_overflow_pages);
    ERL_NIF_TERM entries = enif_make_uint64(env, (uint64_t) stat.ms_entries);

    const ERL_NIF_TERM result = enif_make_tuple7(env, atomEnvStat, psize, depth, branch_pages, leaf_pages, overflow_pages,
                                                 entries);
    return make_ok_result(env, result);
}

/* NIF: env_sync */
static ERL_NIF_TERM nif_env_sync(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    /* Check argument count */
    if (argc != 2) {
        return make_error(env, "invalid_argument_count");
    }

    mdb_env_res_t *env_res;
    int force = 0;
    int rc;

    /* Get env resource */
    if (!enif_get_resource(env, argv[0], env_resource_type, (void **) &env_res)) {
        return make_error(env, "invalid env handle");
    }

    /* Check if env is closed */
    if (env_res->mdbEnv == nullptr) {
        return make_error(env, "env is closed");
    }

    /* Get force flag */
    if (!enif_get_int(env, argv[1], &force)) {
        return make_error(env, "invalid force flag");
    }

    rc = mdb_env_sync(env_res->mdbEnv, force);
    if (rc != MDB_SUCCESS) {
        return make_lmdb_error(env, rc);
    }

    return atomOk;
}

/* NIF: env_copy - 统一使用 mdb_env_copy2 增强版本 */
static ERL_NIF_TERM nif_env_copy(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    /* Check argument count */
    if (argc != 3) {
        return make_error(env, "invalid_argument_count");
    }

    mdb_env_res_t *env_res;
    char path[1024];
    unsigned int flags = 0;
    int rc;

    /* Get env resource */
    if (!enif_get_resource(env, argv[0], env_resource_type, (void **) &env_res)) {
        return make_error(env, "invalid env handle");
    }

    /* Check if env is closed */
    if (env_res->mdbEnv == nullptr) {
        return make_error(env, "env is closed");
    }

    /* Get path string */
    if (!enif_get_string(env, argv[1], path, sizeof(path), ERL_NIF_UTF8)) {
        return make_error(env, "invalid path");
    }

    /* Get flags */
    if (!enif_get_uint(env, argv[2], &flags)) {
        return make_error(env, "invalid flags");
    }

    /* 验证 flags */
    if (flags != 0 && flags != MDB_CP_COMPACT) {
        return make_error(env, "invalid flags, must be 0 or MDB_CP_COMPACT");
    }

    rc = mdb_env_copy2(env_res->mdbEnv, path, flags);
    if (rc != MDB_SUCCESS) {
        return make_lmdb_error(env, rc);
    }

    return atomOk;
}

/* NIF: dbi_open */
static ERL_NIF_TERM nif_dbi_open(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    /* Check argument count */
    if (argc != 3) {
        return make_error(env, "invalid_argument_count");
    }

    mdb_env_res_t *env_res;
    char name[256];
    unsigned int flags = 0;
    MDB_dbi dbi;
    MDB_txn *txn = nullptr;
    int rc;

    /* Get env resource */
    if (!enif_get_resource(env, argv[0], env_resource_type, (void **) &env_res)) {
        return make_error(env, "invalid env handle");
    }

    /* Check if env is closed */
    if (env_res->mdbEnv == nullptr) {
        return make_error(env, "env is closed");
    }

    /* Get database name */
    if (!enif_get_string(env, argv[1], name, sizeof(name), ERL_NIF_UTF8)) {
        return make_error(env, "invalid database name");
    }

    /* Get flags */
    if (!enif_get_uint(env, argv[2], &flags)) {
        return make_error(env, "invalid flags");
    }

    /* Create a write transaction for opening the database */
    rc = mdb_txn_begin(env_res->mdbEnv, nullptr, 0, &txn);
    if (rc != MDB_SUCCESS) {
        return make_lmdb_error(env, rc);
    }

    /* Open the database within the transaction */
    /* 修复主数据库处理：如果name是空字符串，传递NULL给mdb_dbi_open */
    if (name[0] == '\0') {
        rc = mdb_dbi_open(txn, NULL, flags, &dbi);
    } else {
        rc = mdb_dbi_open(txn, name, flags, &dbi);
    }
    if (rc != MDB_SUCCESS) {
        mdb_txn_abort(txn);
        return make_lmdb_error(env, rc);
    }

    /* Commit the transaction */
    rc = mdb_txn_commit(txn);
    if (rc != MDB_SUCCESS) {
        /* 事务提交失败，事务已被内部abort，无需再调用mdb_txn_abort */
        return make_lmdb_error(env, rc);
    }

    ERL_NIF_TERM result = enif_make_uint(env, dbi);
    return make_ok_result(env, result);
}

/* NIF: dbi_stat */
static ERL_NIF_TERM nif_dbi_stat(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    mdb_env_res_t *env_res;
    MDB_dbi dbi;
    MDB_txn *txn;
    MDB_stat stat;
    int rc;

    if (argc != 2) {
        return make_error(env, "invalid_argument_count");
    }

    /* Get env handle resource */
    if (!enif_get_resource(env, argv[0], env_resource_type, (void **) &env_res)) {
        return make_error(env, "invalid env handle");
    }

    /* Check if env is closed */
    if (env_res->mdbEnv == nullptr) {
        return make_error(env, "env is closed");
    }

    /* Get database index */
    if (!enif_get_uint(env, argv[1], (unsigned int *) &dbi)) {
        return make_error(env, "invalid database index");
    }

    /* Begin read-only transaction */
    rc = mdb_txn_begin(env_res->mdbEnv, nullptr, MDB_RDONLY, &txn);
    if (rc != MDB_SUCCESS) {
        return make_lmdb_error(env, rc);
    }

    rc = mdb_stat(txn, dbi, &stat);
    if (rc != MDB_SUCCESS) {
        mdb_txn_abort(txn);
        return make_lmdb_error(env, rc);
    }

    mdb_txn_abort(txn);

    /* Create result tuple with database statistics */
    ERL_NIF_TERM psize = enif_make_uint64(env, (uint64_t) stat.ms_psize);
    ERL_NIF_TERM depth = enif_make_uint64(env, (uint64_t) stat.ms_depth);
    ERL_NIF_TERM branch_pages = enif_make_uint64(env, (uint64_t) stat.ms_branch_pages);
    ERL_NIF_TERM leaf_pages = enif_make_uint64(env, (uint64_t) stat.ms_leaf_pages);
    ERL_NIF_TERM overflow_pages = enif_make_uint64(env, (uint64_t) stat.ms_overflow_pages);
    ERL_NIF_TERM entries = enif_make_uint64(env, (uint64_t) stat.ms_entries);

    const ERL_NIF_TERM result = enif_make_tuple7(env, atomDbiStat, psize, depth, branch_pages, leaf_pages, overflow_pages,
                                                 entries);
    return make_ok_result(env, result);
}

/* NIF: dbi_flags */
static ERL_NIF_TERM nif_dbi_flags(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    mdb_env_res_t *env_res;
    MDB_dbi dbi;
    MDB_txn *txn;
    unsigned int flags;
    int rc;

    if (argc != 2) {
        return make_error(env, "invalid_argument_count");
    }

    /* Get env handle resource */
    if (!enif_get_resource(env, argv[0], env_resource_type, (void **) &env_res)) {
        return make_error(env, "invalid env handle");
    }

    /* Check if env is closed */
    if (env_res->mdbEnv == nullptr) {
        return make_error(env, "env is closed");
    }

    /* Get database index */
    if (!enif_get_uint(env, argv[1], (unsigned int *) &dbi)) {
        return make_error(env, "invalid database index");
    }

    /* Begin read-only transaction */
    rc = mdb_txn_begin(env_res->mdbEnv, nullptr, MDB_RDONLY, &txn);
    if (rc != MDB_SUCCESS) {
        return make_lmdb_error(env, rc);
    }

    rc = mdb_dbi_flags(txn, dbi, &flags);
    if (rc != MDB_SUCCESS) {
        mdb_txn_abort(txn);
        return make_lmdb_error(env, rc);
    }

    mdb_txn_abort(txn);

    ERL_NIF_TERM result = enif_make_uint(env, flags);
    return make_ok_result(env, result);
}

/* NIF: put (同步版本，保持原有逻辑) */
static ERL_NIF_TERM nif_put(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    /* Check argument count */
    if (argc != 6) {
        return make_error(env, "invalid_argument_count");
    }

    mdb_env_res_t *env_res;
    MDB_dbi dbi;
    unsigned int flags = 0;
    int mode = 0; // 模型选择：0-异步无结果，1-异步等待结果，2-同步执行

    /* Get env handle resource */
    if (!enif_get_resource(env, argv[0], env_resource_type, (void **) &env_res)) {
        return make_error(env, "invalid env handle");
    }

    /* Check if env is closed */
    if (env_res->mdbEnv == nullptr) {
        return make_error(env, "env is closed");
    }

    /* Get database index */
    if (!enif_get_uint(env, argv[1], (unsigned int *) &dbi)) {
        return make_error(env, "invalid database index");
    }

    /* Get flags */
    if (!enif_get_uint(env, argv[4], &flags)) {
        return make_error(env, "invalid flags");
    }

    /* Get mode */
    if (!enif_get_int(env, argv[5], &mode)) {
        return make_error(env, "invalid mode");
    }

    /* 根据模式选择执行方式 */
    switch (mode) {
        case 0: // 异步无结果 - 发送到写进程，不等待结果
        {
            // 序列化{key, value, Flags}元组到binary
            const ERL_NIF_TERM deal_term = enif_make_tuple3(env, argv[2], argv[3], argv[4]);
            ErlNifBinary op_data;

            if (!enif_term_to_binary(env, deal_term, &op_data)) {
                return make_error(env, "failed to serialize key-value pair");
            }
            int enqueue_result = enqueue_write_cmd(env_res, CMD_PUT, dbi, &op_data, true, nullptr, 0);
            if (enqueue_result == -2) {
                enif_release_binary(&op_data);
                return make_error(env, "write queue is full, try again later");
            } else if (enqueue_result == -3) {
                enif_release_binary(&op_data);
                return make_error(env, "env is closed");
            } else if (enqueue_result != 0) {
                enif_release_binary(&op_data);
                return make_error(env, "failed to enqueue put command");
            }
            return atomOk;
        }
        case 1: // 异步有结果 - 发送到写进程，等待结果返回
        {
            // 序列化{key, value, Flags}元组到binary
            const ERL_NIF_TERM deal_term = enif_make_tuple3(env, argv[2], argv[3], argv[4]);
            ErlNifBinary op_data;

            if (!enif_term_to_binary(env, deal_term, &op_data)) {
                return make_error(env, "failed to serialize key-value pair");
            }

            ErlNifPid caller;
            enif_self(env, &caller);
            uint64_t RquestId;
            ERL_NIF_TERM RequestIdTerm = enif_make_unique_integer(
                env, (ErlNifUniqueInteger) (ERL_NIF_UNIQUE_MONOTONIC | ERL_NIF_UNIQUE_POSITIVE));
            /* Get RquestId */
            if (!enif_get_uint64(env, RequestIdTerm, &RquestId)) {
                RquestId = enif_hash(ERL_NIF_INTERNAL_HASH, RequestIdTerm, 0);
            }

            int enqueue_result = enqueue_write_cmd(env_res, CMD_PUT, dbi, &op_data, true, &caller, RquestId);
            if (enqueue_result == -2) {
                enif_release_binary(&op_data);
                return make_error(env, "write queue is full, try again later");
            } else if (enqueue_result == -3) {
                enif_release_binary(&op_data);
                return make_error(env, "env is closed");
            } else if (enqueue_result != 0) {
                enif_release_binary(&op_data);
                return make_error(env, "failed to enqueue put command");
            }
            const ERL_NIF_TERM result = enif_make_tuple2(env, atomWaitWrite, enif_make_uint64(env, RquestId));
            return result;
        }
        default: // 同步执行 - 在当前NIF函数内直接执行
        {
            // 创建非const临时变量来传递参数，避免const限定符丢失
            return deal_put(env, env_res->mdbEnv, dbi, &argv[2], &argv[3], flags);
        }
    }
}

/* NIF: get (write transaction) */
static ERL_NIF_TERM nif_get(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    /* Check argument count */
    if (argc != 3) {
        return make_error(env, "invalid_argument_count");
    }

    mdb_env_res_t *env_res;
    MDB_dbi dbi;
    MDB_val key, value;
    MDB_txn *txn = nullptr;
    int rc;

    /* Get env handle resource */
    if (!enif_get_resource(env, argv[0], env_resource_type, (void **) &env_res)) {
        return make_error(env, "invalid env handle");
    }

    /* Check if env is closed */
    if (env_res->mdbEnv == nullptr) {
        return make_error(env, "env is closed");
    }

    /* Get database index */
    if (!enif_get_uint(env, argv[1], (unsigned int *) &dbi)) {
        return make_error(env, "invalid database index");
    }

    /* Get key - convert Erlang term to binary */
    ErlNifBinary KeyBin;
    if (!enif_term_to_binary(env, argv[2], &KeyBin))
        return make_error(env, "error convert key to binary");

    key.mv_size = KeyBin.size;
    key.mv_data = KeyBin.data;

    /* 创建只读事务 */
    rc = mdb_txn_begin(env_res->mdbEnv, nullptr, MDB_RDONLY, &txn);
    if (rc != MDB_SUCCESS) {
        enif_release_binary(&KeyBin);
        return make_lmdb_error(env, rc);
    }

    rc = mdb_get(txn, dbi, &key, &value);
    if (rc != MDB_SUCCESS) {
        mdb_txn_abort(txn);
        enif_release_binary(&KeyBin);
        if (rc == MDB_NOTFOUND) {
            return enif_make_tuple2(env, atomError, atomNotFound);
        }
        return make_lmdb_error(env, rc);
    }

    /* 在事务内部将获取的值转换为Erlang term */
    ERL_NIF_TERM ValeTerm;
    if (!enif_binary_to_term(env, (const unsigned char *) value.mv_data, value.mv_size, &ValeTerm, 0)) {
        mdb_txn_abort(txn);
        enif_release_binary(&KeyBin);
        return make_error(env, "error convert value to term");
    }

    /* 中止事务（读事务使用abort） */
    mdb_txn_abort(txn);
    enif_release_binary(&KeyBin);

    return enif_make_tuple2(env, atomOk, ValeTerm);
}

/* NIF: del */
static ERL_NIF_TERM nif_del(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    /* Check argument count */
    if (argc != 4) {
        return make_error(env, "invalid_argument_count");
    }

    mdb_env_res_t *env_res;
    MDB_dbi dbi;
    unsigned int mode = 0; // 模型选择：0-异步无结果，1-异步等待结果，2-同步执行，3-同步执行并返回值

    /* Get env handle resource */
    if (!enif_get_resource(env, argv[0], env_resource_type, (void **) &env_res)) {
        return make_error(env, "invalid env handle");
    }

    /* Check if env is closed */
    if (env_res->mdbEnv == nullptr) {
        return make_error(env, "env is closed");
    }

    /* Get database index */
    if (!enif_get_uint(env, argv[1], (unsigned int *) &dbi)) {
        return make_error(env, "invalid database index");
    }

    /* Get mode */
    if (!enif_get_uint(env, argv[3], &mode)) {
        return make_error(env, "invalid mode");
    }

    /* 根据模式选择执行方式 */
    switch (mode) {
        case 0: // 异步无结果 - 发送到写进程，不等待结果
        {
            // 序列化{key}元组到binary
            ErlNifBinary op_data;
            if (!enif_term_to_binary(env, argv[2], &op_data)) {
                return make_error(env, "failed to serialize key");
            }

            int enqueue_result = enqueue_write_cmd(env_res, CMD_DEL, dbi, &op_data, true, nullptr, 0);
            if (enqueue_result == -2) {
                enif_release_binary(&op_data);
                return make_error(env, "write queue is full, try again later");
            } else if (enqueue_result == -3) {
                enif_release_binary(&op_data);
                return make_error(env, "env is closed");
            } else if (enqueue_result != 0) {
                enif_release_binary(&op_data);
                return make_error(env, "failed to enqueue delete command");
            }
            return atomOk;
        }

        case 1: // 异步等待结果 - 发送到写进程，等待结果返回
        {
            // 序列化{key}元组到binary
            ErlNifBinary op_data;
            if (!enif_term_to_binary(env, argv[2], &op_data)) {
                return make_error(env, "failed to serialize key");
            }

            ErlNifPid caller;
            enif_self(env, &caller);
            uint64_t RquestId;
            /* 生成唯一的RequestId，避免参数越界 */
            ERL_NIF_TERM RequestIdTerm = enif_make_unique_integer(
                env, (ErlNifUniqueInteger) (ERL_NIF_UNIQUE_MONOTONIC | ERL_NIF_UNIQUE_POSITIVE));
            /* Get RquestId */
            if (!enif_get_uint64(env, RequestIdTerm, &RquestId)) {
                RquestId = enif_hash(ERL_NIF_INTERNAL_HASH, RequestIdTerm, 0);
            }

            int enqueue_result = enqueue_write_cmd(env_res, CMD_DEL, dbi, &op_data, true, &caller, RquestId);
            if (enqueue_result == -2) {
                enif_release_binary(&op_data);
                return make_error(env, "write queue is full, try again later");
            } else if (enqueue_result == -3) {
                enif_release_binary(&op_data);
                return make_error(env, "env is closed");
            } else if (enqueue_result != 0) {
                enif_release_binary(&op_data);
                return make_error(env, "failed to enqueue delete command");
            }

            const ERL_NIF_TERM result = enif_make_tuple2(env, atomWaitWrite, enif_make_uint64(env, RquestId));
            return result;
        }
        case 2: // 同步执行 - 在当前NIF函数内直接执行
        {
            return deal_del(env, env_res->mdbEnv, dbi, &argv[2]);
        }
        default: // 同步执行并返回值 - 在单个事务中获取值并删除，确保原子性
        {
            return deal_del_return(env, env_res->mdbEnv, dbi, &argv[2]);
        }
    }
}

/* NIF: drop */
static ERL_NIF_TERM nif_drop(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    /* Check argument count */
    if (argc != 3) {
        return make_error(env, "invalid_argument_count");
    }

    mdb_env_res_t *env_res;
    MDB_dbi dbi;
    MDB_txn *txn;
    unsigned int mode = 1; /* 模式选择：1-删除数据库，0-清空数据库 */
    int del;
    int rc;

    /* Get env handle resource */
    if (!enif_get_resource(env, argv[0], env_resource_type, (void **) &env_res)) {
        return make_error(env, "invalid env handle");
    }

    /* Check if env is closed */
    if (env_res->mdbEnv == nullptr) {
        return make_error(env, "env is closed");
    }

    /* Get database index */
    if (!enif_get_uint(env, argv[1], (unsigned int *) &dbi)) {
        return make_error(env, "invalid database index");
    }

    /* Get mode parameter */
    if (!enif_get_uint(env, argv[2], &mode)) {
        return make_error(env, "invalid mode parameter");
    }

    /* Set del parameter based on mode */
    switch (mode) {
        case 0: /* 清空数据库：删除所有数据但保留数据库结构 */
            del = 0;
            break;
        default: /* 删除数据库：从环境中删除数据库并关闭句柄 */
            del = 1;
            break;
    }

    /* Begin a write transaction */
    rc = mdb_txn_begin(env_res->mdbEnv, nullptr, 0, &txn);
    if (rc != MDB_SUCCESS) {
        return make_lmdb_error(env, rc);
    }

    /* Call mdb_drop within the transaction */
    rc = mdb_drop(txn, dbi, del);
    if (rc != MDB_SUCCESS) {
        mdb_txn_abort(txn);
        return make_lmdb_error(env, rc);
    }

    /* Commit the transaction */
    rc = mdb_txn_commit(txn);
    if (rc != MDB_SUCCESS) {
        return make_lmdb_error(env, rc);
    }

    return atomOk;
}

/* NIF: get_multi */
static ERL_NIF_TERM nif_get_multi(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    /* Check argument count */
    if (argc != 2) {
        return make_error(env, "invalid_argument_count");
    }

    mdb_env_res_t *env_res = nullptr;
    MDB_txn *txn = nullptr;

    /* 获取环境句柄 */
    if (!enif_get_resource(env, argv[0], env_resource_type, (void **) &env_res)) {
        return make_error(env, "invalid env handle");
    }

    /* Check if env is closed */
    if (env_res->mdbEnv == nullptr) {
        return make_error(env, "env is closed");
    }

    /* 创建只读事务 */
    int rc = mdb_txn_begin(env_res->mdbEnv, nullptr, MDB_RDONLY, &txn);
    if (rc != MDB_SUCCESS) {
        return make_lmdb_error(env, rc);
    }

    /* 先计算查询项数量 */
    unsigned int count = 0;
    /* 获取列表长度 */
    if (!enif_get_list_length(env, argv[1], &count)) {
        return make_error(env, "invalid query list");
    }

    /* 空查询特判：直接返回空列表 */
    if (count == 0) {
        mdb_txn_abort(txn);
        return enif_make_tuple2(env, atomOk, enif_make_list(env, 0));
    }

    /* 分配结果数组 */
    ERL_NIF_TERM *results = nullptr;
    results = (ERL_NIF_TERM *) enif_alloc(count * sizeof(ERL_NIF_TERM));
    if (!results) {
        mdb_txn_abort(txn);
        return make_error(env, "memory allocation failed");
    }

    /* 处理所有查询项 */
    unsigned int index = 0;
    ERL_NIF_TERM head, tail = argv[1];
    int tuple_size;
    while (enif_get_list_cell(env, tail, &head, &tail)) {
        const ERL_NIF_TERM *tuple;
        /* 检查查询项格式 */
        if (!enif_get_tuple(env, head, &tuple_size, &tuple) || tuple_size != 2) {
            mdb_txn_abort(txn);
            enif_free(results);
            return make_error(env, "invalid query item format, expected {Dbi, Key}");
        }

        /* 获取数据库索引 */
        MDB_dbi dbi;
        if (!enif_get_uint(env, tuple[0], (unsigned int *) &dbi)) {
            mdb_txn_abort(txn);
            enif_free(results);
            return make_error(env, "invalid database index");
        }

        /* 获取键并转换为二进制 */
        ErlNifBinary KeyBin;
        if (!enif_term_to_binary(env, tuple[1], &KeyBin)) {
            mdb_txn_abort(txn);
            enif_free(results);
            return make_error(env, "error convert key to binary");
        }

        MDB_val key, value;
        key.mv_size = KeyBin.size;
        key.mv_data = KeyBin.data;

        /* 执行查询 */
        rc = mdb_get(txn, dbi, &key, &value);
        if (rc == MDB_SUCCESS) {
            /* 在事务内部将获取的值转换为Erlang term */
            ERL_NIF_TERM value_term;
            if (!enif_binary_to_term(env, (const unsigned char *) value.mv_data, value.mv_size, &value_term, 0)) {
                mdb_txn_abort(txn);
                enif_release_binary(&KeyBin);
                enif_free(results);
                return make_error(env, "error convert value to term");
            }

            /* 添加结果到数组 */
            results[index] = enif_make_tuple3(env, tuple[1], atomOk, value_term);
        } else if (rc == MDB_NOTFOUND) {
            /* 键不存在，返回错误元组 */
            results[index] = enif_make_tuple3(env, tuple[1], atomError, atomNotFound);
        } else {
            /* 其他错误 */
            mdb_txn_abort(txn);
            enif_release_binary(&KeyBin);
            if (results)
                enif_free(results);
            return make_lmdb_error(env, rc);
        }
        enif_release_binary(&KeyBin);
        index++;
    }

    /* 提交事务 */
    mdb_txn_abort(txn);

    /* 一次性构建结果列表 */
    const ERL_NIF_TERM result_list = enif_make_list_from_array(env, results, count);
    enif_free(results);
    return enif_make_tuple2(env, atomOk, result_list);
}

static ERL_NIF_TERM nif_writes(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    /* Check argument count */
    if (argc != 4) {
        return make_error(env, "invalid_argument_count");
    }

    mdb_env_res_t *env_res = nullptr;
    unsigned int mode; // 执行模式：0-异步无结果，1-异步等待结果，2-同步执行
    unsigned int isSameTxn; // 事务模式：0-不同事务，1-相同事务

    /* 获取环境句柄 */
    if (!enif_get_resource(env, argv[0], env_resource_type, (void **) &env_res)) {
        return make_error(env, "invalid env handle");
    }

    /* Check if env is closed */
    if (env_res->mdbEnv == nullptr) {
        return make_error(env, "env is closed");
    }

    /* 获取执行模式 */
    if (!enif_get_uint(env, argv[2], &mode)) {
        return make_error(env, "invalid mode");
    }

    /* 获取事务模式 */
    if (!enif_get_uint(env, argv[3], &isSameTxn)) {
        return make_error(env, "invalid transaction mode");
    }

    /* 根据模式选择执行方式 */
    switch (mode) {
        case 0: // 异步无结果 - 发送到写进程，不等待结果
        {
            // 检查writeQueue是否可用
            if (env_res->writeQueue == nullptr) {
                return make_error(env, "env is closed");
            }

            // 序列化操作列表binary
            ErlNifBinary op_data;

            if (!enif_term_to_binary(env, argv[1], &op_data)) {
                return make_error(env, "failed to serialize operation list");
            }
            int enqueue_result = enqueue_write_cmd(env_res, CMD_WRITES, 0, &op_data, (isSameTxn != 0), nullptr, 0);
            if (enqueue_result == -2) {
                enif_release_binary(&op_data);
                return make_error(env, "write queue is full, try again later");
            } else if (enqueue_result == -3) {
                enif_release_binary(&op_data);
                return make_error(env, "env is closed");
            } else if (enqueue_result != 0) {
                enif_release_binary(&op_data);
                return make_error(env, "failed to enqueue writes command");
            }
            return atomOk;
        }
        case 1: // 异步有结果 - 发送到写进程，等待结果返回
        {
            // 检查writeQueue是否可用
            if (env_res->writeQueue == nullptr) {
                return make_error(env, "env is closed");
            }

            // 序列化操作列表到binary
            ErlNifBinary op_data;
            if (!enif_term_to_binary(env, argv[1], &op_data)) {
                return make_error(env, "failed to serialize operation list");
            }

            ErlNifPid caller;
            enif_self(env, &caller);
            uint64_t RquestId;
            /* Get RquestId */
            ERL_NIF_TERM RequestIdTerm = enif_make_unique_integer(
                env, (ErlNifUniqueInteger) (ERL_NIF_UNIQUE_MONOTONIC | ERL_NIF_UNIQUE_POSITIVE));
            if (!enif_get_uint64(env, RequestIdTerm, &RquestId)) {
                RquestId = enif_hash(ERL_NIF_INTERNAL_HASH, RequestIdTerm, 0);
            }

            int enqueue_result = enqueue_write_cmd(env_res, CMD_WRITES, 0, &op_data, (isSameTxn != 0), &caller,
                                                   RquestId);
            if (enqueue_result == -2) {
                enif_release_binary(&op_data);
                return make_error(env, "write queue is full, try again later");
            } else if (enqueue_result == -3) {
                enif_release_binary(&op_data);
                return make_error(env, "env is closed");
            } else if (enqueue_result != 0) {
                enif_release_binary(&op_data);
                return make_error(env, "failed to enqueue writes command");
            }

            const ERL_NIF_TERM result = enif_make_tuple2(env, atomWaitWrite, enif_make_uint64(env, RquestId));
            return result;
        }
        default: // 同步执行 - 在当前NIF函数内直接执行
        {
            if (isSameTxn) {
                return deal_writes_same_txn(env, env_res->mdbEnv, &argv[1]);
            } else {
                return deal_writes_diff_txn(env, env_res->mdbEnv, &argv[1]);
            }
        }
    }
}

/* 遍历方向枚举 */
typedef enum {
    TRAVERSE_FORWARD = 0, // 正序遍历
    TRAVERSE_BACKWARD = 1 // 反序遍历
} traverse_direction_t;

/* 返回类型枚举 */
typedef enum {
    RETURN_KEY_VALUE = 0, // 返回Key-Value对
    RETURN_KEY_ONLY = 1 // 只返回Key
} return_type_t;

/* NIF: nif_traversal - 分批次遍历数据库（支持正序/反序，单Key或Key-Value） */
static ERL_NIF_TERM nif_traversal(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    /* Check argument count - accepts 2 to 6 arguments */
    if (argc != 6) {
        return make_error(env, "invalid_argument_count");
    }

    mdb_env_res_t *env_res;
    MDB_dbi dbi;
    int batch_size = 100;
    bool has_start_key = false;
    int direction = TRAVERSE_FORWARD; // 默认正序遍历
    int return_type = RETURN_KEY_VALUE; // 默认返回Key-Value对

    /* 参数检查: EnvRes, Dbi, StartKey(可选), BatchSize(可选), Direction(可选), ReturnType(可选) */

    /* Get env handle resource */
    if (!enif_get_resource(env, argv[0], env_resource_type, (void **) &env_res)) {
        return make_error(env, "invalid env handle");
    }

    /* Check if env is closed */
    if (env_res->mdbEnv == nullptr) {
        return make_error(env, "env is closed");
    }

    /* Get database index */
    if (!enif_get_uint(env, argv[1], (unsigned int *) &dbi)) {
        return make_error(env, "invalid database index");
    }

    /* Get start key - argv[2] */
    has_start_key = !enif_is_identical(argv[2], atomTvsBegin);

    /* Get batch size - argv[3] */
    if (!enif_get_int(env, argv[3], &batch_size)) {
        return make_error(env, "invalid batch size");
    }
    if (batch_size <= 0 || batch_size > TVS_MAX_CNT) {
        return make_error(env, "batch_size out of range");
    }

    /* Get direction - argv[4] */
    if (!enif_get_int(env, argv[4], &direction)) {
        return make_error(env, "invalid direction");
    }
    if (direction != TRAVERSE_FORWARD && direction != TRAVERSE_BACKWARD) {
        return make_error(env, "direction must be 0 (forward) or 1 (backward)");
    }

    /* Get return type - argv[5] */
    if (!enif_get_int(env, argv[5], &return_type)) {
        return make_error(env, "invalid return type");
    }
    if (return_type != RETURN_KEY_VALUE && return_type != RETURN_KEY_ONLY) {
        return make_error(env, "return_type must be 0 (key-value) or 1 (key-only)");
    }

    /* ==================== 开启事务和游标 ==================== */

    MDB_txn *txn = nullptr;
    MDB_cursor *cursor = nullptr;
    int rc;

    // 将Erlang term转换为二进制以保持一致性
    ErlNifBinary start_key_bin = {0};
    if (has_start_key) {
        if (!enif_term_to_binary(env, argv[2], &start_key_bin)) {
            return make_error(env, "invalid start key");
        }
    }

    rc = mdb_txn_begin(env_res->mdbEnv, nullptr, MDB_RDONLY, &txn);
    if (rc != MDB_SUCCESS) {
        if (has_start_key) { enif_release_binary(&start_key_bin); }
        return make_lmdb_error(env, rc);
    }

    rc = mdb_cursor_open(txn, dbi, &cursor);
    if (rc != MDB_SUCCESS) {
        mdb_txn_abort(txn);
        if (has_start_key) { enif_release_binary(&start_key_bin); }
        return make_lmdb_error(env, rc);
    }
    /* ==================== 游标定位（核心优化） ==================== */
    MDB_val key, data;
    if (direction == TRAVERSE_FORWARD) {
        // 正序遍历
        if (has_start_key) {
            key.mv_data = start_key_bin.data;
            key.mv_size = start_key_bin.size;

            /* 使用 SET_RANGE 找到 >= start_key 的位置 */
            rc = mdb_cursor_get(cursor, &key, &data, MDB_SET_RANGE);
            // 不要求精确匹配：从第一个 >= start_key 的键开始
            // rc == MDB_SUCCESS: 有数据，从当前位置开始
            // rc == MDB_NOTFOUND: start_key 大于所有键，结果为空
        } else {
            rc = mdb_cursor_get(cursor, &key, &data, MDB_FIRST);
        }
    } else {
        // 反序遍历
        if (has_start_key) {
            key.mv_data = start_key_bin.data;
            key.mv_size = start_key_bin.size;

            /* 使用 SET_RANGE 找到 >= start_key 的位置 */
            rc = mdb_cursor_get(cursor, &key, &data, MDB_SET_RANGE);

            if (rc == MDB_SUCCESS) {
                /*
                 * SET_RANGE 找到了 >= start_key 的位置
                 * 检查是否精确匹配：
                 * - 精确匹配：从这个位置开始反向遍历
                 * - 不匹配：找到的是 > start_key 的第一个键，需要回退到 <= start_key 的最大键
                 */
                if (key.mv_size != start_key_bin.size ||
                    memcmp(key.mv_data, start_key_bin.data, key.mv_size) != 0) {
                    /* 不是精确匹配，回退到前一个键（<= start_key 的最大键） */
                    rc = mdb_cursor_get(cursor, &key, &data, MDB_PREV);

                    if (rc == MDB_NOTFOUND) {
                        /* 回退失败，说明 start_key 小于所有键，遍历结果为空 */
                        // rc 保持 MDB_NOTFOUND
                    }
                }
                /* 精确匹配时不需要额外操作，直接从当前位置开始反向遍历 */
            } else if (rc == MDB_NOTFOUND) {
                /* start_key 大于所有键，从最后一个键开始 */
                rc = mdb_cursor_get(cursor, &key, &data, MDB_LAST);
            }
        } else {
            /* 无起始 key，从最后开始 */
            rc = mdb_cursor_get(cursor, &key, &data, MDB_LAST);
        }
    }

    /* 释放 start_key binary */
    if (has_start_key) { enif_release_binary(&start_key_bin); }

    /* 如果定位失败且不是 NOTFOUND，返回错误 */
    if (rc != MDB_SUCCESS && rc != MDB_NOTFOUND) {
        mdb_cursor_close(cursor);
        mdb_txn_abort(txn);
        return make_lmdb_error(env, rc);
    }

    /* ==================== 收集数据（优化：使用数组） ==================== */
    ERL_NIF_TERM *items = nullptr;
    if (batch_size > 0) {
        items = (ERL_NIF_TERM *) enif_alloc(batch_size * sizeof(ERL_NIF_TERM));
        if (!items) {
            mdb_cursor_close(cursor);
            mdb_txn_abort(txn);
            return make_error(env, "memory allocation failed");
        }
    }

    int count = 0;
    ERL_NIF_TERM next_key_term = atomTvsOver;
    const MDB_cursor_op cursor_op = direction == TRAVERSE_FORWARD ? MDB_NEXT : MDB_PREV;

    /* 数据收集循环 */
    while (rc == MDB_SUCCESS && count < batch_size) {
        ERL_NIF_TERM item;

        if (return_type == RETURN_KEY_ONLY) {
            /* ========== 仅返回 Key ========== */
            ERL_NIF_TERM key_term;
            if (!enif_binary_to_term(env, (const unsigned char *) key.mv_data, key.mv_size, &key_term, 0)) {
                // 转换失败，跳过此记录
                rc = mdb_cursor_get(cursor, &key, &data, cursor_op);
                continue;
            }
            item = key_term;
        } else {
            /* ========== 返回 Key-Value 对 ========== */
            ERL_NIF_TERM key_term, value_term;

            if (!enif_binary_to_term(env, (const unsigned char *) key.mv_data, key.mv_size, &key_term, 0)) {
                rc = mdb_cursor_get(cursor, &key, &data, cursor_op);
                continue;
            }
            if (!enif_binary_to_term(env, (const unsigned char *) data.mv_data, data.mv_size, &value_term, 0)) {
                rc = mdb_cursor_get(cursor, &key, &data, cursor_op);
                continue;
            }

            item = enif_make_tuple2(env, key_term, value_term);
        }

        items[count++] = item;

        /* 移动到下一条记录 */
        rc = mdb_cursor_get(cursor, &key, &data, cursor_op);
    }

    /* ==================== 记录下一个 Key ==================== */

    if (rc == MDB_SUCCESS) {
        /* 还有更多数据 - 记录当前 key 作为下次的起点 */
        ERL_NIF_TERM next_key_term_val;
        if (enif_binary_to_term(env, (const unsigned char *) key.mv_data, key.mv_size, &next_key_term_val, 0)) {
            next_key_term = next_key_term_val;
        }
        // 转换失败则保持 atomTvsOver
    }
    // rc == MDB_NOTFOUND 表示遍历结束，next_key_term 保持为 atomTvsOver

    /* ==================== 清理资源 ==================== */

    mdb_cursor_close(cursor);
    mdb_txn_abort(txn);

    /* ==================== 构建返回结果 ==================== */

    const ERL_NIF_TERM result_list = enif_make_list_from_array(env, items, count);
    if (items) {
        enif_free(items);
    }

    return enif_make_tuple3(env, atomOk, result_list, next_key_term);
}

/* Resource destructor for env */
static void env_resource_destructor(ErlNifEnv *env, void *arg) {
    mdb_env_res_t *env_res = (mdb_env_res_t *) arg;

    // 标记环境为关闭状态
    env_res->isRunning.store(false, std::memory_order_release);

    // 正常情况下，env_close 应该已经清理了所有资源
    // 这里只处理异常情况：如果用户没有调用 env_close，资源仍然存在

    // 通知线程退出，避免死锁
    if (env_res->queueMutex && env_res->writeCond) {
        enif_mutex_lock(env_res->queueMutex);
        enif_cond_broadcast(env_res->writeCond);
        enif_mutex_unlock(env_res->queueMutex);
    }

    // 等待写线程退出
    if (env_res->writerStarted.exchange(false, std::memory_order_acq_rel)) {
        void *thread_result;
        enif_thread_join(env_res->writeTid, &thread_result);
    }

    // 清理队列剩余命令并销毁队列
    if (env_res->writeQueue) {
        // 清理队列中剩余的命令
        write_cmd_t *cmd;
        while ((cmd = (write_cmd_t *) lfq_dequeue(env_res->writeQueue)) != nullptr) {
            // 释放opData二进制数据
            if (cmd->opData.data) {
                enif_release_binary(&cmd->opData);
            }
            // 释放命令结构本身
            enif_free(cmd);
        }
        lfq_destroy(env_res->writeQueue);
        enif_free(env_res->writeQueue);
        env_res->writeQueue = nullptr;
    }

    // 释放所有分配的资源
    if (env_res->msgEnv) {
        enif_free_env(env_res->msgEnv);
        env_res->msgEnv = nullptr;
    }
    if (env_res->queueMutex) {
        enif_mutex_destroy(env_res->queueMutex);
        env_res->queueMutex = nullptr;
    }
    if (env_res->writeCond) {
        enif_cond_destroy(env_res->writeCond);
        env_res->writeCond = nullptr;
    }
    if (env_res->mdbEnv) {
        mdb_env_close(env_res->mdbEnv);
        env_res->mdbEnv = nullptr;
    }
}

/* NIF load callback */
static int nifLoad(ErlNifEnv *env, void **priv_data, ERL_NIF_TERM load_info) {
    /* Create resource types */
    env_resource_type = enif_open_resource_type(env, nullptr, "env_resource", env_resource_destructor,
                                                ERL_NIF_RT_CREATE,
                                                nullptr);
    if (!env_resource_type)
        return -1;

    /* Create atoms */
    atomOk = enif_make_atom(env, "ok");
    atomTrue = enif_make_atom(env, "true");
    atomFalse = enif_make_atom(env, "false");
    atomError = enif_make_atom(env, "error");
    atomUndefined = enif_make_atom(env, "undefined");
    atomEnvInfo = enif_make_atom(env, "envInfo");
    atomEnvStat = enif_make_atom(env, "envStat");
    atomDbiStat = enif_make_atom(env, "dbiStat");
    atomWaitWrite = enif_make_atom(env, "waitWrite");
    atomWriteReturn = enif_make_atom(env, "writeReturn");
    atomNotFound = enif_make_atom(env, "notFound");
    atomTvsBegin = enif_make_atom(env, "$TvsBegin");
    atomTvsOver = enif_make_atom(env, "$TvsOver");
    return 0;
}

/* NIF upgrade callback */
static int upgrade(ErlNifEnv *env, void **priv_data, void **old_priv_data, ERL_NIF_TERM load_info) {
    return 0;
}

/* NIF unload callback */
static void unload(ErlNifEnv *env, void *priv_data) {
    /* Resource types are automatically closed by Erlang */
}

static ERL_NIF_TERM nif_test(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    /* Check argument count - accepts 0, 1, or 2 arguments */
    if (argc < 0 || argc > 2) {
        return make_error(env, "invalid_argument_count");
    }

    // fprintf(stderr, "IMY*****************  %d %d %d \n", argc, enif_thread_type(), enif_thread_self());
    // 1 nomal 2 cpu 3 io
    return atomOk;
}

static ERL_NIF_TERM nif_test1(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    /* Check argument count - accepts 0 arguments */
    if (argc != 0) {
        return make_error(env, "invalid_argument_count");
    }

    // fprintf(stderr, "IMY*****************  %d %d %d \n", argc, enif_thread_type(), enif_thread_self());
    // 1 nomal 2 cpu 3 io
    return atomOk;
}

static ErlNifFunc nifFuncs[] = {
    {"nif_env_open", 5, nif_env_open, ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"nif_env_close", 1, nif_env_close, ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"nif_env_set_mapsize", 2, nif_env_set_mapsize, ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"nif_env_info", 1, nif_env_info},
    {"nif_env_stat", 1, nif_env_stat},
    {"nif_env_sync", 2, nif_env_sync, ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"nif_env_copy", 3, nif_env_copy, ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"nif_dbi_open", 3, nif_dbi_open, ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"nif_dbi_stat", 2, nif_dbi_stat},
    {"nif_dbi_flags", 2, nif_dbi_flags},
    {"nif_put", 6, nif_put},
    {"nif_put_sync", 6, nif_put, ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"nif_get", 3, nif_get},
    {"nif_del", 4, nif_del},
    {"nif_del_sync", 4, nif_del, ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"nif_drop", 3, nif_drop, ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"nif_get_multi", 2, nif_get_multi, ERL_NIF_DIRTY_JOB_CPU_BOUND},
    {"nif_writes", 4, nif_writes},
    {"nif_writes_sync", 4, nif_writes, ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"nif_traversal", 6, nif_traversal, ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"test1", 0, nif_test1},
    {"test", 0, nif_test},
    {"test", 1, nif_test, ERL_NIF_DIRTY_JOB_CPU_BOUND},
    {"test", 2, nif_test, ERL_NIF_DIRTY_JOB_IO_BOUND}
};
ERL_NIF_INIT(eNifLmdb, nifFuncs, &nifLoad, nullptr, upgrade, unload);
