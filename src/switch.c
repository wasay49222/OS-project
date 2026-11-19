#define _POSIX_C_SOURCE 200809L

#include "frame.h"
#include "io.h"

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

typedef struct Packet {
    Frame frame;
    pthread_mutex_t refs_lock;
    unsigned refs;
} Packet;

typedef struct {
    Packet *items[QUEUE_CAPACITY];
    size_t head;
    size_t tail;
    size_t count;
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
} Port;

typedef struct MacEntry {
    uint8_t address[6];
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
} Switch;

typedef struct {
    Switch *sw;
    unsigned port;
} PortTask;

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

    PTHREAD_CHECK(pthread_mutex_lock(&queue->lock));
    if (!queue->closing && queue->count < QUEUE_CAPACITY) {
        packet_ref(packet);
        queue->items[queue->tail] = packet;
        queue->tail = (queue->tail + 1U) % QUEUE_CAPACITY;
        queue->count++;
        PTHREAD_CHECK(pthread_cond_signal(&queue->ready));
        queued = true;
    } else {
        atomic_fetch_add(&queue->dropped, 1);
    }
    PTHREAD_CHECK(pthread_mutex_unlock(&queue->lock));
    return queued;
}

static Packet *queue_take(OutputQueue *queue)
{
    Packet *packet = NULL;

    PTHREAD_CHECK(pthread_mutex_lock(&queue->lock));
    while (queue->count == 0 && !queue->closing) {
        PTHREAD_CHECK(pthread_cond_wait(&queue->ready, &queue->lock));
    }
    if (queue->count != 0) {
        packet = queue->items[queue->head];
        queue->head = (queue->head + 1U) % QUEUE_CAPACITY;
        queue->count--;
    }
    PTHREAD_CHECK(pthread_mutex_unlock(&queue->lock));
    return packet;
}

static void queue_close(OutputQueue *queue)
{
    PTHREAD_CHECK(pthread_mutex_lock(&queue->lock));
    if (!queue->closing) {
        queue->closing = true;
        while (queue->count != 0) {
            Packet *packet = queue->items[queue->head];
            queue->head = (queue->head + 1U) % QUEUE_CAPACITY;
            queue->count--;
            packet_unref(packet);
        }
        PTHREAD_CHECK(pthread_cond_broadcast(&queue->ready));
    }
    PTHREAD_CHECK(pthread_mutex_unlock(&queue->lock));
}

static size_t mac_bucket_index(const uint8_t address[6])
{
    uint32_t hash = 2166136261U;
    for (size_t i = 0; i < 6; i++) {
        hash = (hash ^ address[i]) * 16777619U;
    }
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
        while (*link != NULL && memcmp((*link)->address, oldest_address, 6) != 0) {
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

static int mac_learn(Switch *sw, const uint8_t address[6], unsigned port)
{
    size_t index = mac_bucket_index(address);
    MacBucket *bucket = &sw->mac[index];
    struct timespec now;
    clock_gettime_check(CLOCK_MONOTONIC, &now);

    PTHREAD_CHECK(pthread_mutex_lock(&bucket->lock));
    for (MacEntry *entry = bucket->head; entry != NULL; entry = entry->next) {
        if (memcmp(entry->address, address, 6) == 0) {
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
                fprintf(stderr, "MAC moved to port %u\n", port + 1U);
                if (count > MACS_PER_PORT) {
                    fprintf(stderr, "port %u shut down: MAC limit exceeded\n", port + 1U);
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
        if (memcmp(entry->address, address, 6) == 0) {
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
                fprintf(stderr, "MAC moved to port %u\n", port + 1U);
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
        fprintf(stderr, "port %u shut down: MAC limit exceeded\n", port + 1U);
        socket_shutdown(sw->ports[port].fd);
        return -1;
    }
    return 0;
}

static int mac_lookup(Switch *sw, const uint8_t address[6], unsigned *port)
{
    MacBucket *bucket = &sw->mac[mac_bucket_index(address)];
    int found = 0;
    struct timespec now;
    clock_gettime_check(CLOCK_MONOTONIC, &now);

    PTHREAD_CHECK(pthread_mutex_lock(&bucket->lock));
    for (MacEntry *entry = bucket->head; entry != NULL; entry = entry->next) {
        if (memcmp(entry->address, address, 6) == 0) {
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

static void forward_frame(Switch *sw, unsigned incoming, const Frame *frame)
{
    if (mac_learn(sw, frame->header + 2, incoming) < 0) {
        return;
    }

    bool broadcast = memcmp(frame->header + 8, "\xff\xff\xff\xff\xff\xff", 6) == 0;
    unsigned destination = 0;
    bool known = !broadcast && mac_lookup(sw, frame->header + 8, &destination);
    Packet *packet = packet_create(frame);
    if (packet == NULL) {
        return;
    }

    PTHREAD_CHECK(pthread_mutex_lock(&sw->ports_lock));
    for (unsigned i = 0; i < sw->port_count; i++) {
        if (i != incoming && sw->ports[i].state == PORT_ACTIVE &&
            (broadcast || !known || i == destination)) {
            (void)queue_put(&sw->ports[i].output, packet);
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
        Packet *packet = queue_take(&sw->ports[index].output);
        if (packet == NULL) {
            break;
        }
        if (frame_write(sw->ports[index].fd, &packet->frame) < 0) {
            packet_unref(packet);
            socket_shutdown(sw->ports[index].fd);
            queue_close(&sw->ports[index].output);
            break;
        }
        packet_unref(packet);
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
        forward_frame(sw, index, &frame);
    }

    queue_close(&sw->ports[index].output);
    PTHREAD_CHECK(pthread_join(sw->ports[index].sender, NULL));
    mac_remove_port(sw, index);
    PTHREAD_CHECK(pthread_mutex_lock(&sw->ports_lock));
    int client_fd = sw->ports[index].fd;
    sw->ports[index].fd = -1;
    sw->ports[index].state = PORT_FREE;
    PTHREAD_CHECK(pthread_mutex_unlock(&sw->ports_lock));
    socket_shutdown(client_fd);
    close_fd(client_fd);
    SEM_POST_CHECK(&sw->free_ports);
    return NULL;
}

static void stop_switch(Switch *sw)
{
    atomic_store(&sw->stopping, true);
    SEM_POST_CHECK(&sw->free_ports);
    socket_shutdown(sw->listener);
    close_fd(sw->listener);
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

static void *signal_thread(void *argument)
{
    Switch *sw = argument;
    sigset_t signals;
    if (sigemptyset(&signals) < 0 || sigaddset(&signals, SIGINT) < 0) {
        perror("signal set setup");
        stop_switch(sw);
        return NULL;
    }
    int signal_number;
    int result = sigwait(&signals, &signal_number);
    if (result != 0) {
        fprintf(stderr, "sigwait failed: %s\n", strerror(result));
    }
    stop_switch(sw);
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
        if (sw->ports[i].state == PORT_FREE) {
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
    port->output.head = 0;
    port->output.tail = 0;
    port->output.count = 0;
    port->output.closing = false;
    PTHREAD_CHECK(pthread_mutex_unlock(&port->output.lock));
    atomic_store(&port->mac_count, 0);
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
    fprintf(stderr, "host connected on port %u\n", index + 1U);
    return 0;
}

static int switch_init(Switch *sw, unsigned port_count)
{
    memset(sw, 0, sizeof(*sw));
    sw->port_count = port_count;
    sw->listener = -1;
    sw->ports = calloc(port_count, sizeof(*sw->ports));
    if (sw->ports == NULL) {
        return -1;
    }
    bool sem_ready = false;
    bool ports_lock_ready = false;
    bool eviction_lock_ready = false;
    bool timer_lock_ready = false;
    bool timer_cond_ready = false;
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
    }
    if (timer_cond_ready) {
        PTHREAD_CHECK(pthread_cond_destroy(&sw->timer_changed));
    }
    if (timer_lock_ready) {
        PTHREAD_CHECK(pthread_mutex_destroy(&sw->timer_lock));
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
    PTHREAD_CHECK(pthread_mutex_destroy(&sw->mac_eviction_lock));
    PTHREAD_CHECK(pthread_mutex_destroy(&sw->ports_lock));
    if (sem_destroy(&sw->free_ports) < 0) {
        perror("sem_destroy");
        abort();
    }
    free(sw->ports);
}

int main(int argc, char **argv)
{
    if (argc != 3) {
        fprintf(stderr, "usage: %s <tcp_port> <num_ports>\n", argv[0]);
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
    if (sigemptyset(&signals) < 0 || sigaddset(&signals, SIGINT) < 0) {
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
    sw.listener = create_listener((unsigned)tcp_port);
    if (sw.listener < 0) {
        perror("listen");
        switch_destroy(&sw);
        return EXIT_FAILURE;
    }
    pthread_t aging_worker;
    int create_result = pthread_create(&aging_worker, NULL, aging_thread, &sw);
    if (create_result != 0) {
        fprintf(stderr, "could not start aging thread: %s\n", strerror(create_result));
        close_fd(sw.listener);
        switch_destroy(&sw);
        return EXIT_FAILURE;
    }
    pthread_t signals_worker;
    create_result = pthread_create(&signals_worker, NULL, signal_thread, &sw);
    if (create_result != 0) {
        fprintf(stderr, "could not start signal thread: %s\n", strerror(create_result));
        stop_switch(&sw);
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
    PTHREAD_CHECK(pthread_join(aging_worker, NULL));
    switch_destroy(&sw);
    return EXIT_SUCCESS;
}
