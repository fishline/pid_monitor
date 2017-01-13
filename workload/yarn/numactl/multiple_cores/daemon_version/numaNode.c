/*
 * Numa aware CPU resource allocator
 */
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netdb.h>
#include <sys/sendfile.h>
#include <time.h>
#include <sys/poll.h>
#include <sys/time.h>
#include <pthread.h>
#include <stdarg.h>
#include <signal.h>
#include <semaphore.h>
#include <sys/queue.h>

struct usage {
    unsigned char container_id[40];
    int node_idx;
    int vcpu_start_idx;
    int vcpu_count;
    struct usage *next;
};

#define NUMA_NODE_CAP       4       // to my knowledge max is 4 numa nodes
#define VCPU_PER_NODE_CAP   96      // 12 core max * SMT8
#define HASH_SIZE           40      // how many hash entries
unsigned char vcpu_usage[NUMA_NODE_CAP][VCPU_PER_NODE_CAP] = {0};
pthread_mutex_t vcpu_usage_lock[NUMA_NODE_CAP];
unsigned int node_usage[NUMA_NODE_CAP] = {0};
pthread_mutex_t node_usage_lock;
unsigned char vcpu_idx[NUMA_NODE_CAP][VCPU_PER_NODE_CAP] = {0};
unsigned char node_vcpu_cnt[NUMA_NODE_CAP] = {0};
unsigned int node_cnt = 0;
struct usage *current_usage[HASH_SIZE] = {NULL};
pthread_mutex_t current_usage_lock[HASH_SIZE];

// hash function
unsigned long hash(unsigned char *str)
{
    unsigned long hash = 5381;
    int c;

    while (c = *str++)
        hash = ((hash << 5) + hash) + c; /* hash * 33 + c */

    return hash;
}

void *req_handler(void *info)
{
    int *sock_ptr = (int *)info;
    int rc = 1;
    int n;
    struct pollfd fds[1];
    int timeout = -1;
    int nfds = 1, char_read = 0;
    struct req_entry *req_ptr = NULL;
    char *msg_ptr = NULL;
    int seq = 0;
    char *ptr = NULL;
    char *tmp_str = NULL;
    unsigned char message[64] = {0};
    unsigned int *iptr = NULL;
    unsigned int req_count = NULL;
    unsigned char *req_pid = NULL;
    int group_account[VCPU_PER_NODE_CAP] = {0};
    char result[1024] = {0};

    n = read(*sock_ptr, &(message[0]), 4);
    if (n != 4) {
        perror("read failed!\n");
        goto session_shutdown;
    }
    int m_idx = 4;
    do {
        n = read(*sock_ptr, &(message[m_idx]), 1);
        if (message[m_idx] == '\n') {
            break;
        }
        m_idx += n;
    } while (n > 0);

    if (m_idx <= 0) {
        perror("read failed\n");
        goto session_shutdown;
    }
    iptr = (unsigned int *)message;
    req_count = *iptr;
    req_pid = &(message[4]);

    printf("req size: %d, id: %s", req_count, req_pid);
    if (req_count == 9999) {
        // Debug command
        printf("Node usage: ");
        pthread_mutex_lock(&node_usage_lock);
        for (int idx = 0; idx < node_cnt; idx++) {
            printf("%d ", node_usage[idx]);
        }
        printf("\n");
        pthread_mutex_unlock(&node_usage_lock);
    } else if (req_count != 0) {
        unsigned int min_node_usage = 9999;
        int min_node_idx = -1;
        pthread_mutex_lock(&node_usage_lock);
        for (int idx = 0; idx < node_cnt; idx++) {
            if (node_usage[idx] < min_node_usage) {
                min_node_idx = idx;
                min_node_usage = node_usage[idx];
            }
        }
        node_usage[min_node_idx] = node_usage[min_node_idx] + req_count;
        pthread_mutex_unlock(&node_usage_lock);

        unsigned char unit[4] = {0};
        for (int idx = 0; idx < node_vcpu_cnt[min_node_idx]; idx++) {
            sprintf(unit, "%d", vcpu_idx[min_node_idx][idx]);
            strcat(result, unit);
            if (idx != (node_vcpu_cnt[min_node_idx] -1)) {
                strcat(result, ",");
            }
        }

        int size = strlen(result) + 1;
        n = write(*sock_ptr, &(result[0]), size);
        if (n != size) {
            perror("write failed\n");
            goto session_shutdown;
        }

        // Keep track of usage
        int hash_idx = hash(req_pid) % HASH_SIZE;
        struct usage *usage_ptr = (struct usage *)calloc(sizeof(struct usage), 1);
        if (usage_ptr == NULL) {
            perror("calloc failed\n");
            goto session_shutdown;
        }
        strcpy(&(usage_ptr->container_id[0]), req_pid);
        usage_ptr->node_idx = min_node_idx;
        usage_ptr->vcpu_start_idx = 0;
        usage_ptr->vcpu_count = req_count;

        pthread_mutex_lock(&(current_usage_lock[hash_idx]));
        usage_ptr->next = current_usage[hash_idx];
        current_usage[hash_idx] = usage_ptr;
        pthread_mutex_unlock(&(current_usage_lock[hash_idx]));
    } else {
        int hash_idx = hash(req_pid) % HASH_SIZE;

        pthread_mutex_lock(&(current_usage_lock[hash_idx]));
        struct usage *usage_ptr = current_usage[hash_idx];
        struct usage *prev_usage_ptr = NULL;
        while ((usage_ptr != NULL) && (strcmp(req_pid, &(usage_ptr->container_id[0])) != 0)) {
            prev_usage_ptr = usage_ptr;
            usage_ptr = usage_ptr->next;
        }
        if (usage_ptr == NULL) {
            perror("BUG hash element not found\n");
            pthread_mutex_unlock(&(current_usage_lock[hash_idx]));
            goto session_shutdown;
        }
        if (prev_usage_ptr == NULL) {
            current_usage[hash_idx] = usage_ptr->next;
        } else {
            prev_usage_ptr->next = usage_ptr->next;
        }
        usage_ptr->next = NULL;
        pthread_mutex_unlock(&(current_usage_lock[hash_idx]));

        pthread_mutex_lock(&node_usage_lock);
        node_usage[usage_ptr->node_idx] = node_usage[usage_ptr->node_idx] - usage_ptr->vcpu_count;
        pthread_mutex_unlock(&node_usage_lock);

        free(usage_ptr);
    }

session_shutdown:
    free(info);
    close(*sock_ptr);
}

int main(int argc, char *argv[])
{
    unsigned char output[32] = {0};
    unsigned char *ptr = NULL;
    int cnt = 0;
    int flag_expect_num = 1;
    unsigned char this = 0;
    int node_idx = -1;
    int vcpu_i = 0;
    FILE *fp;
    int sock, sock_client;
    int ret;
    struct sockaddr_in serv_addr;
    int *int_ptr = NULL;
    pthread_t thread_id;

    // Query and initialize CPU resource
    fp = popen("numactl -H | grep cpus | awk -F\":\" '{print $2}' | tr ' ' '\\n'", "r");
    if (fp == NULL) {
        perror("Failed to invoke numactl, maybe not installed?\n");
        return -1;
    }

    flag_expect_num = 1;
    node_idx = -1;
    while (fgets(output, 32, fp) != NULL) {
    	cnt = 0;
        ptr = &(output[0]);
        while (*ptr != 0 && cnt < 32) {
            if (*ptr == 0x0a) {
                if (flag_expect_num == 1) {
                    flag_expect_num = 0;
                    node_idx++;
                    node_cnt++;
                    vcpu_i = 0;
                    this = 0;
                } else {
                    vcpu_idx[node_idx][vcpu_i] = this;
                    node_vcpu_cnt[node_idx] = node_vcpu_cnt[node_idx] + 1;
                    vcpu_i++;
                    this = 0;
                    flag_expect_num = 1;
                }
            } else {
                flag_expect_num = 0;
                this *= 10;
                this += (*ptr - 0x30);
            }
            ptr++;
            cnt++;
        }
    }
    pclose(fp);
    //printf("%d %d\n", node_cnt, node_vcpu_cnt[0]);

    for (int idx = 0; idx < node_cnt; idx++) {
        if (pthread_mutex_init(&(vcpu_usage_lock[idx]), NULL) != 0) {
            perror("mutex init failed");
            goto fail;
        }
    }
    if (pthread_mutex_init(&node_usage_lock, NULL) != 0) {
        perror("mutex init failed");
        goto fail;
    }
    for (int idx = 0; idx < HASH_SIZE; idx++) {
        if (pthread_mutex_init(&(current_usage_lock[idx]), NULL) != 0) {
            perror("mutex init failed");
            goto fail;
        }
    }

    sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) {
        perror("socket error\n");
        goto fail;
    }
    int enable = 1;
    if (setsockopt(sock, SOL_SOCKET, SO_REUSEADDR, &enable, sizeof(int)) < 0) {
        perror("setsockopt(SO_REUSEADDR) failed\n");
    }
    int port = 10101;
    bzero((char *)&serv_addr, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_addr.s_addr = INADDR_ANY;

    do {
        serv_addr.sin_port = htons(port);
        ret = bind(sock, (struct sockaddr *)&serv_addr, sizeof(serv_addr));
        if (ret < 0) {
            port++;
        } else {
            printf("Port: %d\n", port);
            break;
        }
    } while (1);
    listen(sock, 65536);

    while (1) {
        sock_client = accept(sock, NULL, NULL);
        if (sock_client < 0) {
            perror("Error on accept\n");
            continue;
        }
        int_ptr = (int *)malloc(sizeof(int));
        if (int_ptr == NULL) {
            perror("malloc failed\n");
            goto fail;
        }
        *int_ptr = sock_client;
        if (pthread_create(&thread_id, NULL, req_handler, (void *)int_ptr) < 0) {
            perror("pthread_create failed\n");
            goto fail;
        }
        printf("handling new request\n");
    }

    return 0;

fail:
    perror("Daemon failed, exit now...\n");
    return -1;
}
