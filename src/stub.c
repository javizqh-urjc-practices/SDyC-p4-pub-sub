/**
 * @file stub.c
 * @author Javier Izquierdo Hernandez (j.izquierdoh.2021@alumnos.urjc.es)
 * @brief 
 * @version 0.1
 * @date 2023-12-04
 * 
 * @copyright Copyright (c) 2023
 * 
 */

#include "stub.h"

#define ERROR(msg) { fprintf(stderr,"PROXY ERROR: %s",msg); perror("");}
#define INFO(msg) fprintf(stderr,"PROXY INFO: %s\n",msg)
#define LOG(...) { print_epoch(); fprintf(stdout, __VA_ARGS__);}

#define NS_TO_S 1000000000
#define NS_TO_MICROS 1000
#define MICROS_TO_MS 1000

// ------------------------ Global variables for client ------------------------
int sockfd;
int id;
char topic[MAX_TOPIC_SIZE];

char client_ip[MAX_IP_SIZE];
// ------------------------ Global variables for server ------------------------
int server_sockfd;
struct sockaddr *server_addr;
socklen_t server_len;

struct thread_info {
    pthread_t thread;
    int sockfd;
};
// -----------------------------------------------------------------------------

// ---------------------------- Initialize sockets -----------------------------

int load_config_broker(int port) {
    const int enable = 1;
    struct sockaddr_in servaddr;
    int sock_fd, counter_fd;
    socklen_t len;
    struct sockaddr * addr = malloc(sizeof(struct sockaddr));

    if (addr == NULL) err(EXIT_FAILURE, "failed to alloc memory");
    memset(addr, 0, sizeof(struct sockaddr));

    // Create socket and liste -------------------------------------------------
    setbuf(stdout, NULL);

    servaddr.sin_family = AF_INET;
    servaddr.sin_addr.s_addr = INADDR_ANY;
    servaddr.sin_port = htons(port);

    len = sizeof(servaddr);

    sockfd = socket(AF_INET, SOCK_STREAM, 0);

    if (sock_fd < 0) {
        ERROR("Socket failed");
        return 0;
    }

    if (setsockopt(sock_fd, SOL_SOCKET, SO_REUSEADDR,
        &enable, sizeof(int)) < 0) {
        ERROR("Setsockopt(SO_REUSEADDR) failed");
        return 0;
    }

    if (bind(sock_fd, (const struct sockaddr *) &servaddr, len) < 0) {
        ERROR("Unable to bind");
        return 0;
    }

    if (listen(sock_fd, 1000) < 0) {
        ERROR("Unable to listen");
        return 0;
    }

    // Set global variables ----------------------------------------------------
    server_sockfd = sock_fd;
    memcpy(addr, (struct sockaddr *) &servaddr, sizeof(struct sockaddr));
    server_addr = addr;
    server_len = len;

    return sock_fd > 0;
}

// ---------------------------- Private functions ------------------------------
int init_client(char broker_ip[MAX_IP_SIZE], int broker_port) {
    struct in_addr addr;
    struct sockaddr_in servaddr;
    int sock_fd;
    socklen_t len;

    // Create socket and connect -----------------------------------------------
    if (inet_pton(AF_INET, (char *) broker_ip, &addr) < 0) {
        return 0;
    }

    setbuf(stdout, NULL);

    servaddr.sin_family = AF_INET;
    servaddr.sin_addr.s_addr = addr.s_addr;
    servaddr.sin_port = htons(broker_port);

    len = sizeof(servaddr);

    sock_fd = socket(AF_INET, SOCK_STREAM, 0);

    if (sock_fd < 0) {
        ERROR("Socket failed\n");
        return 0;
    }

    if (connect(sock_fd, (const struct sockaddr *) &servaddr, len) < 0){
        ERROR("Unable to connect");
        return 0;
    }

    return sock_fd;
}

int send_request_to_broker(operations operation, char topic[MAX_TOPIC_SIZE]) {
    message msg;

    msg.action = operation;
    strcpy(msg.topic, topic);

    if (send(sockfd, &msg, sizeof(msg), MSG_WAITALL) < 0) {
        ERROR("Fail to send");
        return 0;
    }

    return 1;
}

int wait_ack_broker(char topic[MAX_TOPIC_SIZE]) {
    broker_response resp;
    int status = 0;

    if (recv(sockfd, &resp, sizeof(resp), MSG_WAITALL) < 0) {
        ERROR("Fail to received");
        close(sockfd);
        return 0;
    }

    // Print response ----------------------------------------------------------
    switch (resp.response_status) {
    case OK:
        LOG("Registrado correctamente con ID: %d para topic %s\n",
            resp.id, topic);
        id = resp.id;
        break;
    case LIMIT:
    case ERROR:
        LOG("Error al hacer el registro: error=%d\n", status);
        break;
    }

    return status;
}
// -------------------------- Functions for publisher --------------------------
int init_publisher(char broker_ip[MAX_IP_SIZE], int broker_port,
                   char topic[MAX_TOPIC_SIZE]) {
    sockfd = init_client(broker_ip, broker_port);

    // // Send request message ----------------------------------------------------
    send_request_to_broker(REGISTER_PUBLISHER, topic);

    // // Listen for response messages --------------------------------------------
    return wait_ack_broker(topic);
}

int publish(char topic[MAX_TOPIC_SIZE], char _data[MAX_DATA_SIZE]) {
    message msg;

    msg.action = PUBLISH_DATA;
    strcpy(msg.topic, topic);
    strcpy(msg.data.data, _data);
    clock_gettime(CLOCK_REALTIME, &msg.data.time_generated_data);

    if (send(sockfd, &msg, sizeof(msg), MSG_WAITALL) < 0) {
        ERROR("Fail to send");
        return 0;
    }

    LOG("Publicado mensaje topic: %s - mensaje: %s -Generó: %ld.%.9ld",
        msg.topic,
        msg.data.data,
        msg.data.time_generated_data.tv_sec,
        msg.data.time_generated_data.tv_nsec);

    return 1;
}

int end_publisher(int id) {
    return 1;
}

// -------------------------- Functions for subscriber ---------------------------
int init_subscriber(char broker_ip[MAX_IP_SIZE], int broker_port,
                   char topic[MAX_TOPIC_SIZE]) {
    sockfd = init_client(broker_ip, broker_port);

    // // Send request message ----------------------------------------------------
    send_request_to_broker(REGISTER_SUBSCRIBER, topic);

    // // Listen for response messages --------------------------------------------
    return wait_ack_broker(topic);                   
}


int end_subscriber(int id) {
    return 1;
}

// -----------------------------------------------------------------------------

void print_epoch() {
    struct timespec epoch;

    clock_gettime(CLOCK_REALTIME, &epoch);
    printf("[%ld.%.9ld] " , epoch.tv_sec, epoch.tv_nsec);
}