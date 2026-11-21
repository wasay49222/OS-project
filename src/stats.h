#ifndef STATS_H
#define STATS_H

#include <pthread.h>
#include <stdint.h>

#define SWITCH_STATS_NAME "/virtual_switch_stats"
#define SWITCH_ADMIN_PATH "/tmp/virtual_switch_admin.sock"
#define SWITCH_MAX_PORTS 256U

typedef struct {
    uint64_t in;
    uint64_t out;
    uint64_t dropped;
    uint64_t mirrored;
} PortStats;

typedef struct {
    pthread_mutex_t lock;
    unsigned port_count;
    PortStats ports[SWITCH_MAX_PORTS];
} SharedStats;

#endif
