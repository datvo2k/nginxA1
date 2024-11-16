/*
 * Copyright (C) 2007 Grzegorz Nosek
 * Work sponsored by Ezra Zygmuntowicz & EngineYard.com
 *
 * Based on nginx source (C) Igor Sysoev
 */

#include <ngx_config.h>
#include <ngx_core.h>
#include <ngx_http.h>

typedef struct {
    ngx_uint_t                          nreq;
    ngx_uint_t                          total_req;
    ngx_uint_t                          last_req_id;
    ngx_uint_t                          fails;
    ngx_uint_t                          current_weight;
} ngx_http_upstream_fair_shared_t;

typedef struct ngx_http_upstream_fair_peers_s ngx_http_upstream_fair_peers_t;

typedef struct {
    ngx_rbtree_node_t                   node;
    ngx_uint_t                          generation;
    uintptr_t                           peers;      /* forms a unique cookie together with generation */
    ngx_uint_t                          total_nreq;
    ngx_uint_t                          total_requests;
    ngx_atomic_t                        lock;
    ngx_http_upstream_fair_shared_t     stats[1];
} ngx_http_upstream_fair_shm_block_t;

/* ngx_spinlock is defined without a matching unlock primitive */
#define ngx_spinlock_unlock(lock)       (void) ngx_atomic_cmp_set(lock, ngx_pid, 0)

typedef struct {
    ngx_http_upstream_fair_shared_t    *shared;
    struct sockaddr                    *sockaddr;
    socklen_t                           socklen;
    ngx_str_t                           name;

    ngx_uint_t                          weight;
    ngx_uint_t                          max_fails;
    time_t                              fail_timeout;

    time_t                              accessed;
    ngx_uint_t                          down:1;

#if (NGX_HTTP_SSL)
    ngx_ssl_session_t                  *ssl_session;    /* local to a process */
#endif
} ngx_http_upstream_fair_peer_t;

#define NGX_HTTP_UPSTREAM_FAIR_NO_RR            (1<<26)
#define NGX_HTTP_UPSTREAM_FAIR_WEIGHT_MODE_IDLE (1<<27)
#define NGX_HTTP_UPSTREAM_FAIR_WEIGHT_MODE_PEAK (1<<28)
#define NGX_HTTP_UPSTREAM_FAIR_WEIGHT_MODE_MASK ((1<<27) | (1<<28))

enum { WM_DEFAULT = 0, WM_IDLE, WM_PEAK };


struct ngx_http_upstream_fair_peers_s {
    ngx_http_upstream_fair_shm_block_t *shared;
    ngx_uint_t                          current;
    ngx_uint_t                          size_err:1;
    ngx_uint_t                          no_rr:1;
    ngx_uint_t                          weight_mode:2;
    ngx_uint_t                          number;
    ngx_str_t                          *name;
    ngx_http_upstream_fair_peers_t     *next;           /* for backup peers support, not really used yet */
    ngx_http_upstream_fair_peer_t       peer[1];
};


#define NGX_PEER_INVALID (~0UL)

typedef struct {
    ngx_http_upstream_fair_peers_t     *peers;
    ngx_uint_t                          current;
    uintptr_t                          *tried;
    uintptr_t                          *done;
    uintptr_t                           data;
    uintptr_t                           data2;
} ngx_http_upstream_fair_peer_data_t;