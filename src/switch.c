#define _POSIX_C_SOURCE 200809L

#include "frame.h"
#include "io.h"
#include "stats.h"

#include <arpa/inet.h>
#include <errno.h>
#include <netinet/in.h>
#include <pthread.h>
#include <semaphore.h>
#include <signal.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <fcntl.h>
#include <stdarg.h>
#include <time.h>
#include <unistd.h>

#define QUEUE_CAPACITY 64U
#define MAC_BUCKETS 64U
#ifndef MAC_LIMIT
#define MAC_LIMIT 1024U
#endif
#ifndef MACS_PER_PORT
#define MACS_PER_PORT 16U
#endif
#ifndef MAC_AGE_SECONDS
#define MAC_AGE_SECONDS 300
#endif
#define PORT_WAIT_SECONDS 5
#define PRIORITY_CLASSES 3U
#define LOW_PRIORITY_AGE_MS 200
#define LOG_QUEUE_CAPACITY 128U

typedef struct Packet {
    Frame frame;
    pthread_mutex_t refs_lock;
    unsigned refs;
} Packet;

typedef struct {
    Packet *items[PRIORITY_CLASSES][QUEUE_CAPACITY];
    struct timespec queued_at[PRIORITY_CLASSES][QUEUE_CAPACITY];
    size_t head[PRIORITY_CLASSES];
    size_t tail[PRIORITY_CLASSES];
    size_t count[PRIORITY_CLASSES];
    size_t total;
    unsigned schedule_cursor;
    bool closing;
    pthread_mutex_t lock;
    pthread_cond_t ready;
    atomic_ulong dropped;
} OutputQueue;

typedef enum {
    PORT_FREE,
    PORT_RESERVED,
    PORT_ACTIVE
} PortState;

typedef struct {
    int fd;
    PortState state;
    bool has_receiver;
    pthread_t receiver;
    pthread_t sender;
    OutputQueue output;
    atomic_uint mac_count;
    pthread_mutex_t rate_lock;
    double rate_limit;
    double tokens;
    uint16_t vlan;
    bool trunk;
    bool admin_down;
    int mirror_to;
} Port;

typedef struct MacEntry {
    uint8_t address[6];
    uint16_t vlan;
    unsigned port;
    struct timespec seen;
    struct MacEntry *next;
} MacEntry;

typedef struct {
    MacEntry *head;
    pthread_mutex_t lock;
} MacBucket;

typedef struct {
    Port *ports;
    unsigned port_count;
    sem_t free_ports;
    pthread_mutex_t ports_lock;
    MacBucket mac[MAC_BUCKETS];
    pthread_mutex_t mac_eviction_lock;
    atomic_uint mac_count;
    pthread_mutex_t timer_lock;
    pthread_cond_t timer_changed;
    atomic_bool stopping;
    int listener;
    int admin_listener;
    int stats_fd;
    bool stats_created;
    SharedStats *stats;
    const char *config_path;
    pthread_mutex_t log_lock;
    pthread_cond_t log_ready;
    char log_queue[LOG_QUEUE_CAPACITY][256];
    size_t log_head;
    size_t log_count;
    bool log_closing;
    FILE *log_file;
} Switch;

typedef struct {
    Switch *sw;
    unsigned port;
} PortTask;

typedef struct {
    uint16_t vlan;
    bool trunk;
    double rate;
    int mirror_to;
} PortConfig;

static void pthread_check(int result)
{
    if (result != 0) {
        fprintf(stderr, "pthread call failed: %s\n", strerror(result));
        abort();
    }
}

static void sem_post_check(sem_t *semaphore)
{
    if (sem_post(semaphore) < 0) {
        perror("sem_post");
        abort();
    }
}

#define PTHREAD_CHECK(call) pthread_check((call))
#define SEM_POST_CHECK(semaphore) sem_post_check((semaphore))

static void log_enqueue(Switch *sw, const char *format, ...)
{
    if (pthread_mutex_trylock(&sw->log_lock) != 0) {
        return;
    }
    if (sw->log_count < LOG_QUEUE_CAPACITY && !sw->log_closing) {
        size_t slot = (sw->log_head + sw->log_count) % LOG_QUEUE_CAPACITY;
        va_list arguments;
        va_start(arguments, format);
        (void)vsnprintf(sw->log_queue[slot], sizeof(sw->log_queue[slot]),
                        format, arguments);
        va_end(arguments);
        sw->log_count++;
        PTHREAD_CHECK(pthread_cond_signal(&sw->log_ready));
    }
    PTHREAD_CHECK(pthread_mutex_unlock(&sw->log_lock));
}

static void *logger_thread(void *argument)
{
    Switch *sw = argument;
    for (;;) {
        char message[256];
        PTHREAD_CHECK(pthread_mutex_lock(&sw->log_lock));
        while (sw->log_count == 0 && !sw->log_closing) {
            PTHREAD_CHECK(pthread_cond_wait(&sw->log_ready, &sw->log_lock));
        }
        if (sw->log_count == 0 && sw->log_closing) {
            PTHREAD_CHECK(pthread_mutex_unlock(&sw->log_lock));
            break;
        }
        memcpy(message, sw->log_queue[sw->log_head], sizeof(message));
        sw->log_head = (sw->log_head + 1U) % LOG_QUEUE_CAPACITY;
        sw->log_count--;
        PTHREAD_CHECK(pthread_mutex_unlock(&sw->log_lock));

        time_t now = time(NULL);
        struct tm local_time;
        char stamp[32];
        if (localtime_r(&now, &local_time) != NULL &&
            strftime(stamp, sizeof(stamp), "%Y-%m-%d %H:%M:%S", &local_time) > 0) {
            fprintf(sw->log_file, "%s %s\n", stamp, message);
            fflush(sw->log_file);
        }
    }
    return NULL;
}

static void logger_stop(Switch *sw)
{
    PTHREAD_CHECK(pthread_mutex_lock(&sw->log_lock));
    sw->log_closing = true;
    PTHREAD_CHECK(pthread_cond_broadcast(&sw->log_ready));
    PTHREAD_CHECK(pthread_mutex_unlock(&sw->log_lock));
}

static void close_fd(int fd);

static int stats_init(Switch *sw)
{
    if (shm_unlink(SWITCH_STATS_NAME) < 0 && errno != ENOENT) {
        perror("shm_unlink");
        return -1;
    }
    sw->stats_fd = shm_open(SWITCH_STATS_NAME, O_CREAT | O_EXCL | O_RDWR, 0600);
    if (sw->stats_fd < 0) {
        perror("shm_open");
        return -1;
    }
    if (ftruncate(sw->stats_fd, (off_t)sizeof(SharedStats)) < 0) {
        perror("ftruncate shared stats");
        close_fd(sw->stats_fd);
        sw->stats_fd = -1;
        shm_unlink(SWITCH_STATS_NAME);
        return -1;
    }
    sw->stats = mmap(NULL, sizeof(SharedStats), PROT_READ | PROT_WRITE,
                     MAP_SHARED, sw->stats_fd, 0);
    if (sw->stats == MAP_FAILED) {
        sw->stats = NULL;
        perror("mmap shared stats");
        close_fd(sw->stats_fd);
        sw->stats_fd = -1;
        shm_unlink(SWITCH_STATS_NAME);
        return -1;
    }
    memset(sw->stats, 0, sizeof(*sw->stats));
    pthread_mutexattr_t attributes;
    int result = pthread_mutexattr_init(&attributes);
    bool attributes_ready = result == 0;
    if (result == 0) {
        result = pthread_mutexattr_setpshared(&attributes, PTHREAD_PROCESS_SHARED);
    }
    if (result == 0) {
        result = pthread_mutex_init(&sw->stats->lock, &attributes);
    }
    if (attributes_ready) {
        int destroy_result = pthread_mutexattr_destroy(&attributes);
        if (destroy_result != 0 && result == 0) {
            result = destroy_result;
        }
    }
    if (result != 0) {
        fprintf(stderr, "shared stats mutex setup failed: %s\n", strerror(result));
        munmap(sw->stats, sizeof(*sw->stats));
        sw->stats = NULL;
        close_fd(sw->stats_fd);
        sw->stats_fd = -1;
        shm_unlink(SWITCH_STATS_NAME);
        return -1;
    }
    sw->stats->port_count = sw->port_count;
    sw->stats_created = true;
    return 0;
}

static void stats_add(Switch *sw, unsigned port, unsigned field)
{
    if (sw->stats == NULL || port >= sw->stats->port_count) {
        return;
    }
    PTHREAD_CHECK(pthread_mutex_lock(&sw->stats->lock));
    uint64_t *counter = field == 0 ? &sw->stats->ports[port].in :
                        field == 1 ? &sw->stats->ports[port].out :
                        field == 2 ? &sw->stats->ports[port].dropped :
                                     &sw->stats->ports[port].mirrored;
    (*counter)++;
    PTHREAD_CHECK(pthread_mutex_unlock(&sw->stats->lock));
}

static void stats_destroy(Switch *sw)
{
    if (sw->stats != NULL) {
        if (munmap(sw->stats, sizeof(*sw->stats)) < 0) {
            perror("munmap shared stats");
        }
        sw->stats = NULL;
    }
    if (sw->stats_fd >= 0) {
        close_fd(sw->stats_fd);
        sw->stats_fd = -1;
    }
    if (sw->stats_created && shm_unlink(SWITCH_STATS_NAME) < 0 && errno != ENOENT) {
        perror("shm_unlink");
    }
    sw->stats_created = false;
}

static void clock_gettime_check(clockid_t clock_id, struct timespec *value)
{
    if (clock_gettime(clock_id, value) < 0) {
        perror("clock_gettime");
        abort();
    }
}

static void socket_shutdown(int fd)
{
    if (shutdown(fd, SHUT_RDWR) < 0 && errno != ENOTCONN) {
        perror("shutdown");
        abort();
    }
}

static void close_fd(int fd)
{
    if (close(fd) < 0) {
        perror("close");
        abort();
    }
}

static void packet_unref(Packet *packet)
{
    bool free_packet;

    PTHREAD_CHECK(pthread_mutex_lock(&packet->refs_lock));
    packet->refs--;
    free_packet = packet->refs == 0;
    PTHREAD_CHECK(pthread_mutex_unlock(&packet->refs_lock));
    if (free_packet) {
        PTHREAD_CHECK(pthread_mutex_destroy(&packet->refs_lock));
        free(packet);
    }
}

static Packet *packet_create(const Frame *frame)
{
    Packet *packet = malloc(sizeof(*packet));
    if (packet == NULL) {
        return NULL;
    }
    packet->frame = *frame;
    packet->refs = 1;
    if (pthread_mutex_init(&packet->refs_lock, NULL) != 0) {
        free(packet);
        return NULL;
    }
    return packet;
}

static void packet_ref(Packet *packet)
{
    PTHREAD_CHECK(pthread_mutex_lock(&packet->refs_lock));
    packet->refs++;
    PTHREAD_CHECK(pthread_mutex_unlock(&packet->refs_lock));
}

static bool queue_put(OutputQueue *queue, Packet *packet)
{
    bool queued = false;
    unsigned priority = frame_priority(&packet->frame);

    PTHREAD_CHECK(pthread_mutex_lock(&queue->lock));
    if (!queue->closing && queue->total < QUEUE_CAPACITY &&
        queue->count[priority] < QUEUE_CAPACITY) {
        packet_ref(packet);
        queue->items[priority][queue->tail[priority]] = packet;
        clock_gettime_check(CLOCK_MONOTONIC,
                            &queue->queued_at[priority][queue->tail[priority]]);
        queue->tail[priority] = (queue->tail[priority] + 1U) % QUEUE_CAPACITY;
        queue->count[priority]++;
        queue->total++;
        PTHREAD_CHECK(pthread_cond_signal(&queue->ready));
        queued = true;
    } else {
        atomic_fetch_add(&queue->dropped, 1);
    }
    PTHREAD_CHECK(pthread_mutex_unlock(&queue->lock));
    return queued;
}

static bool port_take_token(Port *port)
{
    bool available = false;
    PTHREAD_CHECK(pthread_mutex_lock(&port->rate_lock));
    if (port->rate_limit == 0.0 || port->tokens >= 1.0) {
        if (port->rate_limit > 0.0) {
            port->tokens -= 1.0;
        }
        available = true;
    }
    PTHREAD_CHECK(pthread_mutex_unlock(&port->rate_lock));
    return available;
}

static unsigned next_priority(OutputQueue *queue)
{
    static const unsigned schedule[] = {0, 0, 0, 0, 0, 1, 1, 1, 2};
    struct timespec now;
    clock_gettime_check(CLOCK_MONOTONIC, &now);
    if (queue->count[2] != 0) {
        struct timespec queued = queue->queued_at[2][queue->head[2]];
        time_t seconds = now.tv_sec - queued.tv_sec;
        long nanoseconds = now.tv_nsec - queued.tv_nsec;
        if (nanoseconds < 0) {
            seconds--;
            nanoseconds += 1000000000L;
        }
        if (seconds > 0 || nanoseconds >= LOW_PRIORITY_AGE_MS * 1000000L) {
            return 2;
        }
    }
    for (size_t offset = 0; offset < sizeof(schedule) / sizeof(schedule[0]); offset++) {
        unsigned slot = (queue->schedule_cursor + (unsigned)offset) %
                        (unsigned)(sizeof(schedule) / sizeof(schedule[0]));
        unsigned priority = schedule[slot];
        if (queue->count[priority] != 0) {
            queue->schedule_cursor = (slot + 1U) %
                                     (unsigned)(sizeof(schedule) / sizeof(schedule[0]));
            return priority;
        }
    }
    return PRIORITY_CLASSES;
}

static Packet *queue_take(OutputQueue *queue, Port *port)
{
    Packet *packet = NULL;

    PTHREAD_CHECK(pthread_mutex_lock(&queue->lock));
    while ((queue->total == 0 || !port_take_token(port)) &&
           !(queue->closing && queue->total == 0)) {
        if (queue->total == 0) {
            PTHREAD_CHECK(pthread_cond_wait(&queue->ready, &queue->lock));
        } else {
            struct timespec deadline;
            clock_gettime_check(CLOCK_REALTIME, &deadline);
            deadline.tv_nsec += 200000000L;
            if (deadline.tv_nsec >= 1000000000L) {
                deadline.tv_sec++;
                deadline.tv_nsec -= 1000000000L;
            }
            int result = pthread_cond_timedwait(&queue->ready, &queue->lock,
                                                &deadline);
            if (result != 0 && result != ETIMEDOUT) {
                PTHREAD_CHECK(result);
            }
        }
    }
    if (queue->total != 0) {
        unsigned priority = next_priority(queue);
        packet = queue->items[priority][queue->head[priority]];
        queue->head[priority] = (queue->head[priority] + 1U) % QUEUE_CAPACITY;
        queue->count[priority]--;
        queue->total--;
    }
    PTHREAD_CHECK(pthread_mutex_unlock(&queue->lock));
    return packet;
}

static void queue_close(OutputQueue *queue)
{
    PTHREAD_CHECK(pthread_mutex_lock(&queue->lock));
    if (!queue->closing) {
        queue->closing = true;
        for (unsigned priority = 0; priority < PRIORITY_CLASSES; priority++) {
            while (queue->count[priority] != 0) {
                Packet *packet = queue->items[priority][queue->head[priority]];
                queue->head[priority] = (queue->head[priority] + 1U) % QUEUE_CAPACITY;
                queue->count[priority]--;
                queue->total--;
                packet_unref(packet);
            }
        }
        PTHREAD_CHECK(pthread_cond_broadcast(&queue->ready));
    }
    PTHREAD_CHECK(pthread_mutex_unlock(&queue->lock));
}

static void queue_drain(OutputQueue *queue)
{
    PTHREAD_CHECK(pthread_mutex_lock(&queue->lock));
    queue->closing = true;
    PTHREAD_CHECK(pthread_cond_broadcast(&queue->ready));
    PTHREAD_CHECK(pthread_mutex_unlock(&queue->lock));
}

static size_t mac_bucket_index(const uint8_t address[6], uint16_t vlan)
{
    uint32_t hash = 2166136261U;
    for (size_t i = 0; i < 6; i++) {
        hash = (hash ^ address[i]) * 16777619U;
    }
    hash = (hash ^ (uint8_t)(vlan >> 8)) * 16777619U;
    hash = (hash ^ (uint8_t)vlan) * 16777619U;
    return (size_t)(hash % MAC_BUCKETS);
}

static void mac_adjust_port(Switch *sw, unsigned port, int adjustment)
{
    if (adjustment > 0) {
        atomic_fetch_add(&sw->ports[port].mac_count, 1);
    } else {
        atomic_fetch_sub(&sw->ports[port].mac_count, 1);
    }
}

static void mac_remove_entry(Switch *sw, MacEntry **link)
{
    MacEntry *entry = *link;
    *link = entry->next;
    mac_adjust_port(sw, entry->port, -1);
    atomic_fetch_sub(&sw->mac_count, 1);
    free(entry);
}

static bool mac_evict_oldest(Switch *sw)
{
    for (;;) {
        bool found = false;
        uint8_t oldest_address[6] = {0};
        uint16_t oldest_vlan = 0;
        struct timespec oldest_seen = {0};
        size_t oldest_bucket = 0;

        for (size_t i = 0; i < MAC_BUCKETS; i++) {
            MacBucket *bucket = &sw->mac[i];
            PTHREAD_CHECK(pthread_mutex_lock(&bucket->lock));
            for (MacEntry *entry = bucket->head; entry != NULL;
                 entry = entry->next) {
                if (!found || entry->seen.tv_sec < oldest_seen.tv_sec ||
                    (entry->seen.tv_sec == oldest_seen.tv_sec &&
                     entry->seen.tv_nsec < oldest_seen.tv_nsec)) {
                    found = true;
                    oldest_seen = entry->seen;
                    oldest_bucket = i;
                    memcpy(oldest_address, entry->address, 6);
                    oldest_vlan = entry->vlan;
                }
            }
            PTHREAD_CHECK(pthread_mutex_unlock(&bucket->lock));
        }
        if (!found) {
            return false;
        }

        MacBucket *bucket = &sw->mac[oldest_bucket];
        PTHREAD_CHECK(pthread_mutex_lock(&bucket->lock));
        MacEntry **link = &bucket->head;
        while (*link != NULL &&
               ((*link)->vlan != oldest_vlan ||
                memcmp((*link)->address, oldest_address, 6) != 0)) {
            link = &(*link)->next;
        }
        if (*link != NULL && (*link)->seen.tv_sec == oldest_seen.tv_sec &&
            (*link)->seen.tv_nsec == oldest_seen.tv_nsec) {
            mac_remove_entry(sw, link);
            PTHREAD_CHECK(pthread_mutex_unlock(&bucket->lock));
            return true;
        }
        PTHREAD_CHECK(pthread_mutex_unlock(&bucket->lock));
    }
}

static int mac_learn(Switch *sw, const uint8_t address[6], uint16_t vlan,
                     unsigned port)
{
    size_t index = mac_bucket_index(address, vlan);
    MacBucket *bucket = &sw->mac[index];
    struct timespec now;
    clock_gettime_check(CLOCK_MONOTONIC, &now);

    PTHREAD_CHECK(pthread_mutex_lock(&bucket->lock));
    for (MacEntry *entry = bucket->head; entry != NULL; entry = entry->next) {
        if (entry->vlan == vlan && memcmp(entry->address, address, 6) == 0) {
            unsigned old_port = entry->port;
            entry->port = port;
            entry->seen = now;
            unsigned count = atomic_load(&sw->ports[port].mac_count);
            if (old_port != port) {
                mac_adjust_port(sw, old_port, -1);
                count = atomic_fetch_add(&sw->ports[port].mac_count, 1) + 1;
            }
            PTHREAD_CHECK(pthread_mutex_unlock(&bucket->lock));
            if (old_port != port) {
                log_enqueue(sw, "MAC moved to port %u", port + 1U);
                if (count > MACS_PER_PORT) {
                    log_enqueue(sw, "port %u shut down: MAC limit exceeded", port + 1U);
                    socket_shutdown(sw->ports[port].fd);
                    return -1;
                }
            }
            return 0;
        }
    }
    PTHREAD_CHECK(pthread_mutex_unlock(&bucket->lock));

    PTHREAD_CHECK(pthread_mutex_lock(&sw->mac_eviction_lock));
    PTHREAD_CHECK(pthread_mutex_lock(&bucket->lock));
    for (MacEntry *entry = bucket->head; entry != NULL; entry = entry->next) {
        if (entry->vlan == vlan && memcmp(entry->address, address, 6) == 0) {
            unsigned old_port = entry->port;
            entry->port = port;
            entry->seen = now;
            unsigned count = atomic_load(&sw->ports[port].mac_count);
            if (old_port != port) {
                mac_adjust_port(sw, old_port, -1);
                count = atomic_fetch_add(&sw->ports[port].mac_count, 1) + 1;
            }
            PTHREAD_CHECK(pthread_mutex_unlock(&bucket->lock));
            PTHREAD_CHECK(pthread_mutex_unlock(&sw->mac_eviction_lock));
            if (old_port != port) {
                log_enqueue(sw, "MAC moved to port %u", port + 1U);
                if (count > MACS_PER_PORT) {
                    socket_shutdown(sw->ports[port].fd);
                    return -1;
                }
            }
            return 0;
        }
    }
    PTHREAD_CHECK(pthread_mutex_unlock(&bucket->lock));

    if (atomic_load(&sw->mac_count) >= MAC_LIMIT) {
        (void)mac_evict_oldest(sw);
    }
    MacEntry *entry = malloc(sizeof(*entry));
    if (entry == NULL) {
        PTHREAD_CHECK(pthread_mutex_unlock(&sw->mac_eviction_lock));
        return -1;
    }
    memcpy(entry->address, address, 6);
    entry->vlan = vlan;
    entry->port = port;
    entry->seen = now;
    PTHREAD_CHECK(pthread_mutex_lock(&bucket->lock));
    entry->next = bucket->head;
    bucket->head = entry;
    PTHREAD_CHECK(pthread_mutex_unlock(&bucket->lock));
    atomic_fetch_add(&sw->mac_count, 1);
    unsigned count = atomic_fetch_add(&sw->ports[port].mac_count, 1) + 1;
    PTHREAD_CHECK(pthread_mutex_unlock(&sw->mac_eviction_lock));

    if (count > MACS_PER_PORT) {
        log_enqueue(sw, "port %u shut down: MAC limit exceeded", port + 1U);
        socket_shutdown(sw->ports[port].fd);
        return -1;
    }
    return 0;
}

static int mac_lookup(Switch *sw, const uint8_t address[6], uint16_t vlan,
                     unsigned *port)
{
    MacBucket *bucket = &sw->mac[mac_bucket_index(address, vlan)];
    int found = 0;
    struct timespec now;
    clock_gettime_check(CLOCK_MONOTONIC, &now);

    PTHREAD_CHECK(pthread_mutex_lock(&bucket->lock));
    for (MacEntry *entry = bucket->head; entry != NULL; entry = entry->next) {
        if (entry->vlan == vlan && memcmp(entry->address, address, 6) == 0) {
            entry->seen = now;
            *port = entry->port;
            found = 1;
            break;
        }
    }
    PTHREAD_CHECK(pthread_mutex_unlock(&bucket->lock));
    return found;
}

static void mac_age(Switch *sw)
{
    struct timespec now;
    clock_gettime_check(CLOCK_MONOTONIC, &now);

    for (size_t i = 0; i < MAC_BUCKETS; i++) {
        MacBucket *bucket = &sw->mac[i];
        PTHREAD_CHECK(pthread_mutex_lock(&bucket->lock));
        MacEntry **link = &bucket->head;
        while (*link != NULL) {
            time_t age = now.tv_sec - (*link)->seen.tv_sec;
            if (age >= MAC_AGE_SECONDS) {
                mac_remove_entry(sw, link);
            } else {
                link = &(*link)->next;
            }
        }
        PTHREAD_CHECK(pthread_mutex_unlock(&bucket->lock));
    }
}

static void mac_remove_port(Switch *sw, unsigned port)
{
    for (size_t i = 0; i < MAC_BUCKETS; i++) {
        MacBucket *bucket = &sw->mac[i];
        PTHREAD_CHECK(pthread_mutex_lock(&bucket->lock));
        MacEntry **link = &bucket->head;
        while (*link != NULL) {
            if ((*link)->port == port) {
                mac_remove_entry(sw, link);
            } else {
                link = &(*link)->next;
            }
        }
        PTHREAD_CHECK(pthread_mutex_unlock(&bucket->lock));
    }
}

static void *aging_thread(void *argument)
{
    Switch *sw = argument;
    PTHREAD_CHECK(pthread_mutex_lock(&sw->timer_lock));
    while (!atomic_load(&sw->stopping)) {
        struct timespec deadline;
        clock_gettime_check(CLOCK_REALTIME, &deadline);
        deadline.tv_sec += 1;
        int wait_result = pthread_cond_timedwait(&sw->timer_changed, &sw->timer_lock,
                                                 &deadline);
        if (wait_result != 0 && wait_result != ETIMEDOUT) {
            PTHREAD_CHECK(wait_result);
        }
        if (!atomic_load(&sw->stopping)) {
            PTHREAD_CHECK(pthread_mutex_unlock(&sw->timer_lock));
            mac_age(sw);
            PTHREAD_CHECK(pthread_mutex_lock(&sw->timer_lock));
        }
    }
    PTHREAD_CHECK(pthread_mutex_unlock(&sw->timer_lock));
    return NULL;
}

static void *rate_timer_thread(void *argument)
{
    Switch *sw = argument;
    PTHREAD_CHECK(pthread_mutex_lock(&sw->timer_lock));
    while (!atomic_load(&sw->stopping)) {
        struct timespec deadline;
        clock_gettime_check(CLOCK_REALTIME, &deadline);
        deadline.tv_nsec += 100000000L;
        if (deadline.tv_nsec >= 1000000000L) {
            deadline.tv_sec++;
            deadline.tv_nsec -= 1000000000L;
        }
        int result = pthread_cond_timedwait(&sw->timer_changed, &sw->timer_lock,
                                           &deadline);
        if (result != 0 && result != ETIMEDOUT) {
            PTHREAD_CHECK(result);
        }
        if (atomic_load(&sw->stopping)) {
            break;
        }
        PTHREAD_CHECK(pthread_mutex_unlock(&sw->timer_lock));
        for (unsigned i = 0; i < sw->port_count; i++) {
            Port *port = &sw->ports[i];
            PTHREAD_CHECK(pthread_mutex_lock(&port->rate_lock));
            if (port->rate_limit > 0.0) {
                port->tokens += port->rate_limit / 10.0;
                if (port->tokens > port->rate_limit) {
                    port->tokens = port->rate_limit;
                }
            }
            PTHREAD_CHECK(pthread_mutex_unlock(&port->rate_lock));
            PTHREAD_CHECK(pthread_mutex_lock(&port->output.lock));
            PTHREAD_CHECK(pthread_cond_broadcast(&port->output.ready));
            PTHREAD_CHECK(pthread_mutex_unlock(&port->output.lock));
        }
        PTHREAD_CHECK(pthread_mutex_lock(&sw->timer_lock));
    }
    PTHREAD_CHECK(pthread_mutex_unlock(&sw->timer_lock));
    return NULL;
}

static void forward_frame(Switch *sw, unsigned incoming, const Frame *frame)
{
    PTHREAD_CHECK(pthread_mutex_lock(&sw->ports_lock));
    Port *ingress = &sw->ports[incoming];
    if (ingress->state != PORT_ACTIVE) {
        PTHREAD_CHECK(pthread_mutex_unlock(&sw->ports_lock));
        return;
    }
    uint16_t vlan = ingress->trunk ? frame_vlan(frame) : ingress->vlan;
    int ingress_mirror = ingress->mirror_to;
    PTHREAD_CHECK(pthread_mutex_unlock(&sw->ports_lock));

    if (mac_learn(sw, frame->header + 2, vlan, incoming) < 0) {
        return;
    }

    bool broadcast = memcmp(frame->header + 8, "\xff\xff\xff\xff\xff\xff", 6) == 0;
    unsigned destination = 0;
    bool known = !broadcast &&
                 mac_lookup(sw, frame->header + 8, vlan, &destination);
    Frame tagged = *frame;
    uint16_t wire_vlan = htons(vlan);
    memcpy(tagged.header + 14, &wire_vlan, sizeof(wire_vlan));
    Packet *packet = packet_create(&tagged);
    if (packet == NULL) {
        return;
    }

    PTHREAD_CHECK(pthread_mutex_lock(&sw->ports_lock));
    bool forward[256] = {false};
    bool mirror[256] = {false};
    for (unsigned i = 0; i < sw->port_count; i++) {
        Port *port = &sw->ports[i];
        bool same_vlan = port->trunk || port->vlan == vlan;
        if (i != incoming && port->state == PORT_ACTIVE && same_vlan &&
            (broadcast || !known || i == destination)) {
            forward[i] = true;
            if (port->mirror_to >= 0 &&
                (unsigned)port->mirror_to < sw->port_count) {
                mirror[port->mirror_to] = true;
            }
        }
    }
    if (ingress_mirror >= 0 && (unsigned)ingress_mirror < sw->port_count) {
        mirror[ingress_mirror] = true;
    }
    for (unsigned i = 0; i < sw->port_count; i++) {
        if ((forward[i] || mirror[i]) && sw->ports[i].state == PORT_ACTIVE) {
            bool queued = queue_put(&sw->ports[i].output, packet);
            if (!queued) {
                stats_add(sw, i, 2);
            } else if (mirror[i]) {
                stats_add(sw, i, 3);
            }
        }
    }
    PTHREAD_CHECK(pthread_mutex_unlock(&sw->ports_lock));
    packet_unref(packet);
}

static void *sender_thread(void *argument)
{
    PortTask *task = argument;
    Switch *sw = task->sw;
    unsigned index = task->port;
    free(task);

    for (;;) {
        Packet *packet = queue_take(&sw->ports[index].output,
                                    &sw->ports[index]);
        if (packet == NULL) {
            break;
        }
        if (frame_write(sw->ports[index].fd, &packet->frame) < 0) {
            packet_unref(packet);
            stats_add(sw, index, 2);
            socket_shutdown(sw->ports[index].fd);
            queue_close(&sw->ports[index].output);
            break;
        }
        packet_unref(packet);
        stats_add(sw, index, 1);
    }
    return NULL;
}

static void *receiver_thread(void *argument)
{
    PortTask *task = argument;
    Switch *sw = task->sw;
    unsigned index = task->port;
    free(task);
    Frame frame;

    while (!atomic_load(&sw->stopping)) {
        int result = frame_read(sw->ports[index].fd, &frame);
        if (result != 1) {
            break;
        }
        stats_add(sw, index, 0);
        forward_frame(sw, index, &frame);
    }

    queue_close(&sw->ports[index].output);
    PTHREAD_CHECK(pthread_join(sw->ports[index].sender, NULL));
    mac_remove_port(sw, index);
    PTHREAD_CHECK(pthread_mutex_lock(&sw->ports_lock));
    int client_fd = sw->ports[index].fd;
    bool reusable = !sw->ports[index].admin_down &&
                    !atomic_load(&sw->stopping);
    sw->ports[index].fd = -1;
    sw->ports[index].state = PORT_FREE;
    PTHREAD_CHECK(pthread_mutex_unlock(&sw->ports_lock));
    socket_shutdown(client_fd);
    close_fd(client_fd);
    if (reusable) {
        SEM_POST_CHECK(&sw->free_ports);
    }
    return NULL;
}

static void stop_switch(Switch *sw)
{
    atomic_store(&sw->stopping, true);
    SEM_POST_CHECK(&sw->free_ports);
    socket_shutdown(sw->listener);
    close_fd(sw->listener);
    if (sw->admin_listener >= 0) {
        socket_shutdown(sw->admin_listener);
        close_fd(sw->admin_listener);
        if (unlink(SWITCH_ADMIN_PATH) < 0 && errno != ENOENT) {
            perror("unlink admin socket");
        }
    }
    PTHREAD_CHECK(pthread_mutex_lock(&sw->timer_lock));
    PTHREAD_CHECK(pthread_cond_broadcast(&sw->timer_changed));
    PTHREAD_CHECK(pthread_mutex_unlock(&sw->timer_lock));

    PTHREAD_CHECK(pthread_mutex_lock(&sw->ports_lock));
    for (unsigned i = 0; i < sw->port_count; i++) {
        if (sw->ports[i].state == PORT_ACTIVE) {
            queue_close(&sw->ports[i].output);
            socket_shutdown(sw->ports[i].fd);
        }
    }
    PTHREAD_CHECK(pthread_mutex_unlock(&sw->ports_lock));
}

static int load_config(Switch *sw)
{
    PortConfig config[256];
    PTHREAD_CHECK(pthread_mutex_lock(&sw->ports_lock));
    for (unsigned i = 0; i < sw->port_count; i++) {
        config[i].vlan = sw->ports[i].vlan;
        config[i].trunk = sw->ports[i].trunk;
        config[i].mirror_to = sw->ports[i].mirror_to;
        PTHREAD_CHECK(pthread_mutex_lock(&sw->ports[i].rate_lock));
        config[i].rate = sw->ports[i].rate_limit;
        PTHREAD_CHECK(pthread_mutex_unlock(&sw->ports[i].rate_lock));
    }
    PTHREAD_CHECK(pthread_mutex_unlock(&sw->ports_lock));

    FILE *file = fopen(sw->config_path, "r");
    if (file == NULL) {
        if (errno == ENOENT) {
            return 0;
        }
        perror("open config");
        return -1;
    }
    char line[256];
    unsigned line_number = 0;
    int failed = 0;
    while (fgets(line, sizeof(line), file) != NULL) {
        line_number++;
        char *text = line;
        while (*text == ' ' || *text == '\t') {
            text++;
        }
        if (*text == '\0' || *text == '\n' || *text == '#') {
            continue;
        }
        unsigned port_number = 0;
        unsigned value = 0;
        unsigned other = 0;
        char extra = '\0';
        char directive[16] = {0};
        int port_fields = sscanf(text, "port %u %15s %u %c", &port_number,
                                 directive, &value, &extra);
        int mirror_fields = sscanf(text, "mirror %u %u %c", &port_number,
                                   &other, &extra);
        if (port_fields == 3 && strcmp(directive, "vlan") == 0 &&
            port_number >= 1 && port_number <= sw->port_count &&
            value >= 1 && value <= 4094) {
            config[port_number - 1].vlan = (uint16_t)value;
            config[port_number - 1].trunk = false;
        } else if (port_fields == 2 && strcmp(directive, "trunk") == 0 &&
                   port_number >= 1 && port_number <= sw->port_count) {
            config[port_number - 1].trunk = true;
        } else if (port_fields == 3 && strcmp(directive, "rate") == 0 &&
                   port_number >= 1 && port_number <= sw->port_count &&
                   value <= 1000000U) {
            config[port_number - 1].rate = (double)value;
        } else if (mirror_fields == 2 &&
                   port_number >= 1 && port_number <= sw->port_count &&
                   other >= 1 && other <= sw->port_count && other != port_number) {
            config[port_number - 1].mirror_to = (int)other - 1;
        } else {
            fprintf(stderr, "invalid config at line %u\n", line_number);
            failed = 1;
            break;
        }
    }
    if (ferror(file)) {
        perror("read config");
        failed = 1;
    }
    if (fclose(file) < 0) {
        perror("close config");
        failed = 1;
    }
    if (failed) {
        return -1;
    }

    PTHREAD_CHECK(pthread_mutex_lock(&sw->ports_lock));
    for (unsigned i = 0; i < sw->port_count; i++) {
        sw->ports[i].vlan = config[i].vlan;
        sw->ports[i].trunk = config[i].trunk;
        sw->ports[i].mirror_to = config[i].mirror_to;
        PTHREAD_CHECK(pthread_mutex_lock(&sw->ports[i].rate_lock));
        sw->ports[i].rate_limit = config[i].rate;
        if (sw->ports[i].tokens > config[i].rate) {
            sw->ports[i].tokens = config[i].rate;
        }
        PTHREAD_CHECK(pthread_mutex_unlock(&sw->ports[i].rate_lock));
    }
    PTHREAD_CHECK(pthread_mutex_unlock(&sw->ports_lock));
    log_enqueue(sw, "configuration loaded from %s", sw->config_path);
    fprintf(stderr, "configuration loaded from %s\n", sw->config_path);
    return 0;
}

static void clear_mac_table(Switch *sw)
{
    for (size_t i = 0; i < MAC_BUCKETS; i++) {
        MacBucket *bucket = &sw->mac[i];
        PTHREAD_CHECK(pthread_mutex_lock(&bucket->lock));
        while (bucket->head != NULL) {
            mac_remove_entry(sw, &bucket->head);
        }
        PTHREAD_CHECK(pthread_mutex_unlock(&bucket->lock));
    }
}

static void dump_mac_table(Switch *sw)
{
    FILE *file = fopen("mac_table.dump", "w");
    if (file == NULL) {
        perror("open MAC dump");
        return;
    }
    for (size_t i = 0; i < MAC_BUCKETS; i++) {
        MacBucket *bucket = &sw->mac[i];
        PTHREAD_CHECK(pthread_mutex_lock(&bucket->lock));
        for (MacEntry *entry = bucket->head; entry != NULL; entry = entry->next) {
            if (fprintf(file,
                        "%02x:%02x:%02x:%02x:%02x:%02x vlan %u port %u\n",
                        entry->address[0], entry->address[1], entry->address[2],
                        entry->address[3], entry->address[4], entry->address[5],
                        entry->vlan, entry->port + 1U) < 0) {
                perror("write MAC dump");
                PTHREAD_CHECK(pthread_mutex_unlock(&bucket->lock));
                fclose(file);
                return;
            }
        }
        PTHREAD_CHECK(pthread_mutex_unlock(&bucket->lock));
    }
    if (fclose(file) < 0) {
        perror("close MAC dump");
    }
}

static int admin_send(int fd, const char *text)
{
    return write_full(fd, text, strlen(text));
}

static int show_mac(int fd, Switch *sw)
{
    char (*lines)[160] = calloc(MAC_LIMIT, sizeof(*lines));
    if (lines == NULL) {
        return -1;
    }
    size_t count = 0;
    for (size_t i = 0; i < MAC_BUCKETS; i++) {
        MacBucket *bucket = &sw->mac[i];
        PTHREAD_CHECK(pthread_mutex_lock(&bucket->lock));
        for (MacEntry *entry = bucket->head; entry != NULL; entry = entry->next) {
            if (count >= MAC_LIMIT) {
                break;
            }
            int length = snprintf(lines[count], sizeof(lines[count]),
                                  "%02x:%02x:%02x:%02x:%02x:%02x vlan %u port %u\n",
                                  entry->address[0], entry->address[1], entry->address[2],
                                  entry->address[3], entry->address[4], entry->address[5],
                                  entry->vlan, entry->port + 1U);
            if (length < 0 || (size_t)length >= sizeof(lines[count])) {
                PTHREAD_CHECK(pthread_mutex_unlock(&bucket->lock));
                free(lines);
                return -1;
            }
            count++;
        }
        PTHREAD_CHECK(pthread_mutex_unlock(&bucket->lock));
    }
    int result = 0;
    for (size_t i = 0; i < count; i++) {
        if (write_full(fd, lines[i], strlen(lines[i])) < 0) {
            result = -1;
            break;
        }
    }
    free(lines);
    return result < 0 ? result : admin_send(fd, "end\n");
}

static int show_stats(int fd, Switch *sw)
{
    char line[192];
    PortStats snapshot[SWITCH_MAX_PORTS];
    PTHREAD_CHECK(pthread_mutex_lock(&sw->stats->lock));
    unsigned count = sw->stats->port_count;
    memcpy(snapshot, sw->stats->ports, count * sizeof(snapshot[0]));
    PTHREAD_CHECK(pthread_mutex_unlock(&sw->stats->lock));
    for (unsigned i = 0; i < count; i++) {
        PortStats *stats = &snapshot[i];
        int length = snprintf(line, sizeof(line),
                              "port %u in %llu out %llu dropped %llu mirrored %llu\n",
                              i + 1U, (unsigned long long)stats->in,
                              (unsigned long long)stats->out,
                              (unsigned long long)stats->dropped,
                              (unsigned long long)stats->mirrored);
        if (length < 0 || (size_t)length >= sizeof(line) ||
            write_full(fd, line, (size_t)length) < 0) {
            return -1;
        }
    }
    return admin_send(fd, "end\n");
}

static int show_ports(int fd, Switch *sw)
{
    char lines[SWITCH_MAX_PORTS][256];
    PTHREAD_CHECK(pthread_mutex_lock(&sw->ports_lock));
    PTHREAD_CHECK(pthread_mutex_lock(&sw->stats->lock));
    bool valid = true;
    for (unsigned i = 0; i < sw->port_count; i++) {
        Port *port = &sw->ports[i];
        int length = snprintf(lines[i], sizeof(lines[i]),
                              "port %u %s vlan %u trunk %s rate %.0f macs %u in %llu out %llu dropped %llu mirrored %llu\n",
                              i + 1U,
                              port->state == PORT_ACTIVE ? "up" :
                              port->admin_down ? "down" : "free",
                              port->vlan, port->trunk ? "yes" : "no",
                              port->rate_limit,
                              atomic_load(&port->mac_count),
                              (unsigned long long)sw->stats->ports[i].in,
                              (unsigned long long)sw->stats->ports[i].out,
                              (unsigned long long)sw->stats->ports[i].dropped,
                              (unsigned long long)sw->stats->ports[i].mirrored);
        if (length < 0 || (size_t)length >= sizeof(lines[i])) {
            valid = false;
            break;
        }
    }
    PTHREAD_CHECK(pthread_mutex_unlock(&sw->stats->lock));
    PTHREAD_CHECK(pthread_mutex_unlock(&sw->ports_lock));
    if (!valid) {
        return -1;
    }
    for (unsigned i = 0; i < sw->port_count; i++) {
        if (write_full(fd, lines[i], strlen(lines[i])) < 0) {
            return -1;
        }
    }
    return admin_send(fd, "end\n");
}

static int set_port_down(Switch *sw, unsigned number)
{
    if (number < 1 || number > sw->port_count) {
        return -1;
    }
    unsigned index = number - 1U;
    pthread_t receiver;
    bool join_receiver = false;
    PTHREAD_CHECK(pthread_mutex_lock(&sw->ports_lock));
    Port *port = &sw->ports[index];
    if (!port->admin_down) {
        port->admin_down = true;
        if (port->state == PORT_FREE && sem_trywait(&sw->free_ports) < 0) {
            if (errno != EAGAIN) {
                perror("sem_trywait");
                PTHREAD_CHECK(pthread_mutex_unlock(&sw->ports_lock));
                return -1;
            }
        }
        if (port->state == PORT_ACTIVE) {
            queue_drain(&port->output);
            if (shutdown(port->fd, SHUT_RD) < 0 && errno != ENOTCONN) {
                perror("shutdown port read side");
            }
            receiver = port->receiver;
            join_receiver = port->has_receiver;
        }
    }
    PTHREAD_CHECK(pthread_mutex_unlock(&sw->ports_lock));
    if (join_receiver) {
        int result = pthread_join(receiver, NULL);
        if (result != 0) {
            fprintf(stderr, "could not stop port %u: %s\n", number, strerror(result));
            return -1;
        }
        PTHREAD_CHECK(pthread_mutex_lock(&sw->ports_lock));
        sw->ports[index].has_receiver = false;
        PTHREAD_CHECK(pthread_mutex_unlock(&sw->ports_lock));
    }
    log_enqueue(sw, "port %u shut down", number);
    return 0;
}

static int set_port_up(Switch *sw, unsigned number)
{
    if (number < 1 || number > sw->port_count) {
        return -1;
    }
    PTHREAD_CHECK(pthread_mutex_lock(&sw->ports_lock));
    Port *port = &sw->ports[number - 1U];
    if (port->admin_down) {
        port->admin_down = false;
        if (port->state == PORT_FREE) {
            SEM_POST_CHECK(&sw->free_ports);
        }
    }
    PTHREAD_CHECK(pthread_mutex_unlock(&sw->ports_lock));
    log_enqueue(sw, "port %u brought up", number);
    return 0;
}

static int admin_command(Switch *sw, int fd, char *command)
{
    unsigned port = 0;
    unsigned value = 0;
    unsigned extra = 0;
    if (strcmp(command, "show mac") == 0) {
        return show_mac(fd, sw);
    }
    if (strcmp(command, "show ports") == 0) {
        return show_ports(fd, sw);
    }
    if (strcmp(command, "stats") == 0) {
        return show_stats(fd, sw);
    }
    if (strcmp(command, "clear mac") == 0) {
        clear_mac_table(sw);
        return admin_send(fd, "ok\n");
    }
    if (sscanf(command, "shutdown %u", &port) == 1) {
        return set_port_down(sw, port) == 0 ? admin_send(fd, "ok\n") :
                                             admin_send(fd, "error\n");
    }
    if (sscanf(command, "up %u", &port) == 1) {
        return set_port_up(sw, port) == 0 ? admin_send(fd, "ok\n") :
                                           admin_send(fd, "error\n");
    }
    if (sscanf(command, "vlan %u %u", &port, &value) == 2 &&
        port >= 1 && port <= sw->port_count && value >= 1 && value <= 4094) {
        PTHREAD_CHECK(pthread_mutex_lock(&sw->ports_lock));
        sw->ports[port - 1U].vlan = (uint16_t)value;
        sw->ports[port - 1U].trunk = false;
        PTHREAD_CHECK(pthread_mutex_unlock(&sw->ports_lock));
        clear_mac_table(sw);
        return admin_send(fd, "ok\n");
    }
    if (sscanf(command, "mirror %u %u", &port, &value) == 2 &&
        port >= 1 && port <= sw->port_count && value >= 1 &&
        value <= sw->port_count && port != value) {
        PTHREAD_CHECK(pthread_mutex_lock(&sw->ports_lock));
        sw->ports[port - 1U].mirror_to = (int)value - 1;
        PTHREAD_CHECK(pthread_mutex_unlock(&sw->ports_lock));
        return admin_send(fd, "ok\n");
    }
    (void)extra;
    return admin_send(fd, "error\n");
}

static void *admin_thread(void *argument)
{
    Switch *sw = argument;
    while (!atomic_load(&sw->stopping)) {
        int client = accept(sw->admin_listener, NULL, NULL);
        if (client < 0) {
            if (atomic_load(&sw->stopping)) {
                break;
            }
            if (errno == EINTR) {
                continue;
            }
            perror("accept admin");
            continue;
        }
        char command[256];
        size_t used = 0;
        while (used + 1 < sizeof(command)) {
            ssize_t count = read(client, command + used, sizeof(command) - used - 1);
            if (count == 0) {
                break;
            }
            if (count < 0) {
                if (errno == EINTR) {
                    continue;
                }
                perror("read admin command");
                break;
            }
            used += (size_t)count;
            if (memchr(command, '\n', used) != NULL) {
                break;
            }
        }
        command[used] = '\0';
        char *newline = strchr(command, '\n');
        if (newline != NULL) {
            *newline = '\0';
        }
        (void)admin_command(sw, client, command);
        close_fd(client);
    }
    return NULL;
}

static int create_admin_listener(void)
{
    int fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd < 0) {
        return -1;
    }
    if (unlink(SWITCH_ADMIN_PATH) < 0 && errno != ENOENT) {
        close_fd(fd);
        return -1;
    }
    struct sockaddr_un address = {0};
    address.sun_family = AF_UNIX;
    if (strlen(SWITCH_ADMIN_PATH) >= sizeof(address.sun_path)) {
        close_fd(fd);
        errno = ENAMETOOLONG;
        return -1;
    }
    memcpy(address.sun_path, SWITCH_ADMIN_PATH, strlen(SWITCH_ADMIN_PATH) + 1);
    if (bind(fd, (struct sockaddr *)&address, sizeof(address)) < 0 ||
        chmod(SWITCH_ADMIN_PATH, 0600) < 0 || listen(fd, 16) < 0) {
        int saved = errno;
        close_fd(fd);
        unlink(SWITCH_ADMIN_PATH);
        errno = saved;
        return -1;
    }
    return fd;
}

static void *signal_thread(void *argument)
{
    Switch *sw = argument;
    sigset_t signals;
    if (sigemptyset(&signals) < 0 || sigaddset(&signals, SIGINT) < 0 ||
        sigaddset(&signals, SIGHUP) < 0 || sigaddset(&signals, SIGUSR1) < 0) {
        perror("signal set setup");
        stop_switch(sw);
        return NULL;
    }
    int signal_number;
    for (;;) {
        int result = sigwait(&signals, &signal_number);
        if (result != 0) {
            fprintf(stderr, "sigwait failed: %s\n", strerror(result));
            stop_switch(sw);
            break;
        }
        if (signal_number == SIGINT) {
            stop_switch(sw);
            break;
        }
        if (signal_number == SIGHUP) {
            if (load_config(sw) == 0) {
                clear_mac_table(sw);
            }
        }
        if (signal_number == SIGUSR1) {
            dump_mac_table(sw);
        }
    }
    return NULL;
}

static int create_listener(unsigned port)
{
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        return -1;
    }
    int reuse = 1;
    if (setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) < 0) {
        close_fd(fd);
        return -1;
    }
    struct sockaddr_in address = {0};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_ANY);
    address.sin_port = htons((uint16_t)port);
    if (bind(fd, (struct sockaddr *)&address, sizeof(address)) < 0 ||
        listen(fd, 64) < 0) {
        close_fd(fd);
        return -1;
    }
    return fd;
}

static int reserve_port(Switch *sw, int client)
{
    struct timespec deadline;
    clock_gettime_check(CLOCK_REALTIME, &deadline);
    deadline.tv_sec += PORT_WAIT_SECONDS;
    while (sem_timedwait(&sw->free_ports, &deadline) < 0) {
        if (errno == EINTR) {
            if (atomic_load(&sw->stopping)) {
                return -1;
            }
            continue;
        }
        if (errno == ETIMEDOUT) {
            if (write_full(client, "switch full\n", 12) < 0) {
                perror("write switch full reply");
            }
        } else {
            perror("sem_timedwait");
        }
        return -1;
    }
    if (atomic_load(&sw->stopping)) {
        SEM_POST_CHECK(&sw->free_ports);
        return -1;
    }

    PTHREAD_CHECK(pthread_mutex_lock(&sw->ports_lock));
    unsigned index = sw->port_count;
    for (unsigned i = 0; i < sw->port_count; i++) {
        if (sw->ports[i].state == PORT_FREE && !sw->ports[i].admin_down) {
            index = i;
            sw->ports[i].state = PORT_RESERVED;
            break;
        }
    }
    PTHREAD_CHECK(pthread_mutex_unlock(&sw->ports_lock));
    if (index == sw->port_count) {
        SEM_POST_CHECK(&sw->free_ports);
        return -1;
    }

    Port *port = &sw->ports[index];
    if (port->has_receiver) {
        PTHREAD_CHECK(pthread_join(port->receiver, NULL));
        port->has_receiver = false;
    }
    PTHREAD_CHECK(pthread_mutex_lock(&port->output.lock));
    memset(port->output.head, 0, sizeof(port->output.head));
    memset(port->output.tail, 0, sizeof(port->output.tail));
    memset(port->output.count, 0, sizeof(port->output.count));
    port->output.total = 0;
    port->output.schedule_cursor = 0;
    port->output.closing = false;
    PTHREAD_CHECK(pthread_mutex_unlock(&port->output.lock));
    atomic_store(&port->mac_count, 0);
    port->tokens = port->rate_limit;
    port->fd = client;

    PortTask *sender_task = malloc(sizeof(*sender_task));
    PortTask *receiver_task = malloc(sizeof(*receiver_task));
    if (sender_task == NULL || receiver_task == NULL) {
        free(sender_task);
        free(receiver_task);
        PTHREAD_CHECK(pthread_mutex_lock(&sw->ports_lock));
        port->fd = -1;
        port->state = PORT_FREE;
        PTHREAD_CHECK(pthread_mutex_unlock(&sw->ports_lock));
        SEM_POST_CHECK(&sw->free_ports);
        return -1;
    }
    sender_task->sw = sw;
    sender_task->port = index;
    receiver_task->sw = sw;
    receiver_task->port = index;
    if (pthread_create(&port->sender, NULL, sender_thread, sender_task) != 0) {
        free(sender_task);
        free(receiver_task);
        PTHREAD_CHECK(pthread_mutex_lock(&sw->ports_lock));
        port->fd = -1;
        port->state = PORT_FREE;
        PTHREAD_CHECK(pthread_mutex_unlock(&sw->ports_lock));
        SEM_POST_CHECK(&sw->free_ports);
        return -1;
    }
    PTHREAD_CHECK(pthread_mutex_lock(&sw->ports_lock));
    port->state = PORT_ACTIVE;
    int created = pthread_create(&port->receiver, NULL, receiver_thread,
                                 receiver_task);
    if (created == 0) {
        port->has_receiver = true;
    } else {
        port->state = PORT_RESERVED;
    }
    PTHREAD_CHECK(pthread_mutex_unlock(&sw->ports_lock));
    if (created != 0) {
        free(receiver_task);
        queue_close(&port->output);
        PTHREAD_CHECK(pthread_join(port->sender, NULL));
        PTHREAD_CHECK(pthread_mutex_lock(&sw->ports_lock));
        port->fd = -1;
        port->state = PORT_FREE;
        PTHREAD_CHECK(pthread_mutex_unlock(&sw->ports_lock));
        SEM_POST_CHECK(&sw->free_ports);
        return -1;
    }
    log_enqueue(sw, "host connected on port %u", index + 1U);
    fprintf(stderr, "host connected on port %u\n", index + 1U);
    return 0;
}

static int switch_init(Switch *sw, unsigned port_count)
{
    memset(sw, 0, sizeof(*sw));
    sw->port_count = port_count;
    sw->listener = -1;
    sw->admin_listener = -1;
    sw->stats_fd = -1;
    sw->ports = calloc(port_count, sizeof(*sw->ports));
    if (sw->ports == NULL) {
        return -1;
    }
    bool sem_ready = false;
    bool ports_lock_ready = false;
    bool eviction_lock_ready = false;
    bool timer_lock_ready = false;
    bool timer_cond_ready = false;
    bool log_lock_ready = false;
    bool log_cond_ready = false;
    unsigned ports_ready = 0;
    size_t buckets_ready = 0;
    int error = 0;

    if (sem_init(&sw->free_ports, 0, port_count) != 0) {
        perror("sem_init");
        goto fail;
    }
    sem_ready = true;
    error = pthread_mutex_init(&sw->ports_lock, NULL);
    if (error != 0) {
        goto fail;
    }
    ports_lock_ready = true;
    error = pthread_mutex_init(&sw->mac_eviction_lock, NULL);
    if (error != 0) {
        goto fail;
    }
    eviction_lock_ready = true;
    error = pthread_mutex_init(&sw->timer_lock, NULL);
    if (error != 0) {
        goto fail;
    }
    timer_lock_ready = true;
    error = pthread_cond_init(&sw->timer_changed, NULL);
    if (error != 0) {
        goto fail;
    }
    timer_cond_ready = true;
    error = pthread_mutex_init(&sw->log_lock, NULL);
    if (error != 0) {
        goto fail;
    }
    log_lock_ready = true;
    error = pthread_cond_init(&sw->log_ready, NULL);
    if (error != 0) {
        goto fail;
    }
    log_cond_ready = true;

    for (unsigned i = 0; i < port_count; i++) {
        sw->ports[i].fd = -1;
        atomic_init(&sw->ports[i].mac_count, 0);
        atomic_init(&sw->ports[i].output.dropped, 0);
        error = pthread_mutex_init(&sw->ports[i].output.lock, NULL);
        if (error != 0) {
            goto fail;
        }
        error = pthread_cond_init(&sw->ports[i].output.ready, NULL);
        if (error != 0) {
            PTHREAD_CHECK(pthread_mutex_destroy(&sw->ports[i].output.lock));
            goto fail;
        }
        error = pthread_mutex_init(&sw->ports[i].rate_lock, NULL);
        if (error != 0) {
            PTHREAD_CHECK(pthread_cond_destroy(&sw->ports[i].output.ready));
            PTHREAD_CHECK(pthread_mutex_destroy(&sw->ports[i].output.lock));
            goto fail;
        }
        sw->ports[i].vlan = 1;
        sw->ports[i].mirror_to = -1;
        ports_ready++;
    }
    for (size_t i = 0; i < MAC_BUCKETS; i++) {
        error = pthread_mutex_init(&sw->mac[i].lock, NULL);
        if (error != 0) {
            goto fail;
        }
        buckets_ready++;
    }
    atomic_init(&sw->stopping, false);
    atomic_init(&sw->mac_count, 0);
    return 0;

fail:
    if (error != 0) {
        fprintf(stderr, "switch synchronization setup failed: %s\n",
                strerror(error));
    }
    for (size_t i = 0; i < buckets_ready; i++) {
        PTHREAD_CHECK(pthread_mutex_destroy(&sw->mac[i].lock));
    }
    for (unsigned i = 0; i < ports_ready; i++) {
        PTHREAD_CHECK(pthread_cond_destroy(&sw->ports[i].output.ready));
        PTHREAD_CHECK(pthread_mutex_destroy(&sw->ports[i].output.lock));
        PTHREAD_CHECK(pthread_mutex_destroy(&sw->ports[i].rate_lock));
    }
    if (timer_cond_ready) {
        PTHREAD_CHECK(pthread_cond_destroy(&sw->timer_changed));
    }
    if (timer_lock_ready) {
        PTHREAD_CHECK(pthread_mutex_destroy(&sw->timer_lock));
    }
    if (log_cond_ready) {
        PTHREAD_CHECK(pthread_cond_destroy(&sw->log_ready));
    }
    if (log_lock_ready) {
        PTHREAD_CHECK(pthread_mutex_destroy(&sw->log_lock));
    }
    if (eviction_lock_ready) {
        PTHREAD_CHECK(pthread_mutex_destroy(&sw->mac_eviction_lock));
    }
    if (ports_lock_ready) {
        PTHREAD_CHECK(pthread_mutex_destroy(&sw->ports_lock));
    }
    if (sem_ready && sem_destroy(&sw->free_ports) < 0) {
        perror("sem_destroy");
    }
    free(sw->ports);
    sw->ports = NULL;
    return -1;
}

static void switch_destroy(Switch *sw)
{
    for (unsigned i = 0; i < sw->port_count; i++) {
        if (sw->ports[i].has_receiver) {
            PTHREAD_CHECK(pthread_join(sw->ports[i].receiver, NULL));
        }
        PTHREAD_CHECK(pthread_cond_destroy(&sw->ports[i].output.ready));
        PTHREAD_CHECK(pthread_mutex_destroy(&sw->ports[i].output.lock));
        PTHREAD_CHECK(pthread_mutex_destroy(&sw->ports[i].rate_lock));
    }
    for (size_t i = 0; i < MAC_BUCKETS; i++) {
        MacEntry *entry = sw->mac[i].head;
        while (entry != NULL) {
            MacEntry *next = entry->next;
            free(entry);
            entry = next;
        }
        PTHREAD_CHECK(pthread_mutex_destroy(&sw->mac[i].lock));
    }
    PTHREAD_CHECK(pthread_cond_destroy(&sw->timer_changed));
    PTHREAD_CHECK(pthread_mutex_destroy(&sw->timer_lock));
    PTHREAD_CHECK(pthread_cond_destroy(&sw->log_ready));
    PTHREAD_CHECK(pthread_mutex_destroy(&sw->log_lock));
    PTHREAD_CHECK(pthread_mutex_destroy(&sw->mac_eviction_lock));
    PTHREAD_CHECK(pthread_mutex_destroy(&sw->ports_lock));
    if (sem_destroy(&sw->free_ports) < 0) {
        perror("sem_destroy");
        abort();
    }
    stats_destroy(sw);
    if (sw->log_file != NULL && fclose(sw->log_file) < 0) {
        perror("close switch log");
    }
    free(sw->ports);
}

static void join_receivers(Switch *sw)
{
    for (unsigned i = 0; i < sw->port_count; i++) {
        if (sw->ports[i].has_receiver) {
            PTHREAD_CHECK(pthread_join(sw->ports[i].receiver, NULL));
            sw->ports[i].has_receiver = false;
        }
    }
}

int main(int argc, char **argv)
{
    if (argc < 3 || argc > 4) {
        fprintf(stderr, "usage: %s <tcp_port> <num_ports> [config_file]\n", argv[0]);
        return EXIT_FAILURE;
    }
    char *end = NULL;
    long tcp_port = strtol(argv[1], &end, 10);
    if (*argv[1] == '\0' || *end != '\0' || tcp_port < 1 || tcp_port > 65535) {
        fprintf(stderr, "invalid TCP port\n");
        return EXIT_FAILURE;
    }
    long requested_ports = strtol(argv[2], &end, 10);
    if (*argv[2] == '\0' || *end != '\0' || requested_ports < 1 ||
        requested_ports > 256) {
        fprintf(stderr, "invalid port count\n");
        return EXIT_FAILURE;
    }

    if (signal(SIGPIPE, SIG_IGN) == SIG_ERR) {
        perror("signal SIGPIPE");
        return EXIT_FAILURE;
    }
    sigset_t signals;
    if (sigemptyset(&signals) < 0 || sigaddset(&signals, SIGINT) < 0 ||
        sigaddset(&signals, SIGHUP) < 0 || sigaddset(&signals, SIGUSR1) < 0) {
        perror("signal set setup");
        return EXIT_FAILURE;
    }
    int mask_result = pthread_sigmask(SIG_BLOCK, &signals, NULL);
    if (mask_result != 0) {
        fprintf(stderr, "pthread_sigmask failed: %s\n", strerror(mask_result));
        return EXIT_FAILURE;
    }

    Switch sw;
    if (switch_init(&sw, (unsigned)requested_ports) < 0) {
        perror("switch initialization");
        return EXIT_FAILURE;
    }
    sw.log_file = fopen("switch.log", "a");
    if (sw.log_file == NULL) {
        perror("open switch.log");
        switch_destroy(&sw);
        return EXIT_FAILURE;
    }
    sw.config_path = argc == 4 ? argv[3] : "switch.conf";
    if (load_config(&sw) < 0) {
        switch_destroy(&sw);
        return EXIT_FAILURE;
    }
    if (stats_init(&sw) < 0) {
        switch_destroy(&sw);
        return EXIT_FAILURE;
    }
    sw.admin_listener = create_admin_listener();
    if (sw.admin_listener < 0) {
        perror("admin socket");
        switch_destroy(&sw);
        return EXIT_FAILURE;
    }
    sw.listener = create_listener((unsigned)tcp_port);
    if (sw.listener < 0) {
        perror("listen");
        close_fd(sw.admin_listener);
        unlink(SWITCH_ADMIN_PATH);
        sw.admin_listener = -1;
        switch_destroy(&sw);
        return EXIT_FAILURE;
    }
    pthread_t aging_worker;
    int create_result = pthread_create(&aging_worker, NULL, aging_thread, &sw);
    if (create_result != 0) {
        fprintf(stderr, "could not start aging thread: %s\n", strerror(create_result));
        stop_switch(&sw);
        switch_destroy(&sw);
        return EXIT_FAILURE;
    }
    pthread_t rate_worker;
    create_result = pthread_create(&rate_worker, NULL, rate_timer_thread, &sw);
    if (create_result != 0) {
        fprintf(stderr, "could not start rate timer: %s\n", strerror(create_result));
        stop_switch(&sw);
        PTHREAD_CHECK(pthread_join(aging_worker, NULL));
        switch_destroy(&sw);
        return EXIT_FAILURE;
    }
    pthread_t admin_worker;
    create_result = pthread_create(&admin_worker, NULL, admin_thread, &sw);
    if (create_result != 0) {
        fprintf(stderr, "could not start admin thread: %s\n", strerror(create_result));
        stop_switch(&sw);
        PTHREAD_CHECK(pthread_join(rate_worker, NULL));
        PTHREAD_CHECK(pthread_join(aging_worker, NULL));
        switch_destroy(&sw);
        return EXIT_FAILURE;
    }
    pthread_t logger_worker;
    create_result = pthread_create(&logger_worker, NULL, logger_thread, &sw);
    if (create_result != 0) {
        fprintf(stderr, "could not start logger thread: %s\n", strerror(create_result));
        stop_switch(&sw);
        PTHREAD_CHECK(pthread_join(admin_worker, NULL));
        PTHREAD_CHECK(pthread_join(rate_worker, NULL));
        PTHREAD_CHECK(pthread_join(aging_worker, NULL));
        switch_destroy(&sw);
        return EXIT_FAILURE;
    }
    pthread_t signals_worker;
    create_result = pthread_create(&signals_worker, NULL, signal_thread, &sw);
    if (create_result != 0) {
        fprintf(stderr, "could not start signal thread: %s\n", strerror(create_result));
        stop_switch(&sw);
        join_receivers(&sw);
        logger_stop(&sw);
        PTHREAD_CHECK(pthread_join(logger_worker, NULL));
        PTHREAD_CHECK(pthread_join(admin_worker, NULL));
        PTHREAD_CHECK(pthread_join(rate_worker, NULL));
        PTHREAD_CHECK(pthread_join(aging_worker, NULL));
        switch_destroy(&sw);
        return EXIT_FAILURE;
    }

    fprintf(stderr, "switch listening on TCP port %ld with %ld ports\n",
            tcp_port, requested_ports);
    while (!atomic_load(&sw.stopping)) {
        int client = accept(sw.listener, NULL, NULL);
        if (client < 0) {
            if (errno == EINTR) {
                continue;
            }
            if (atomic_load(&sw.stopping)) {
                break;
            }
            perror("accept");
            continue;
        }
        if (atomic_load(&sw.stopping) || reserve_port(&sw, client) < 0) {
            close_fd(client);
        }
    }

    if (!atomic_load(&sw.stopping)) {
        int signal_result = pthread_kill(signals_worker, SIGINT);
        if (signal_result != 0) {
            fprintf(stderr, "could not wake signal thread: %s\n",
                    strerror(signal_result));
            stop_switch(&sw);
        }
    }
    PTHREAD_CHECK(pthread_join(signals_worker, NULL));
    PTHREAD_CHECK(pthread_join(admin_worker, NULL));
    PTHREAD_CHECK(pthread_join(rate_worker, NULL));
    PTHREAD_CHECK(pthread_join(aging_worker, NULL));
    join_receivers(&sw);
    logger_stop(&sw);
    PTHREAD_CHECK(pthread_join(logger_worker, NULL));
    switch_destroy(&sw);
    return EXIT_SUCCESS;
}
