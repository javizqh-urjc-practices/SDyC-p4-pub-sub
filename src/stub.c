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
void print_epoch() {
    struct timespec epoch;

    clock_gettime(CLOCK_REALTIME, &epoch);
    printf("[%ld.%.9ld] " , epoch.tv_sec, epoch.tv_nsec);
}
#define LOG(...) { print_epoch(); fprintf(stdout, __VA_ARGS__);}

#define NS_TO_S 1000000000
#define NS_TO_MICROS 1000
#define MICROS_TO_MS 1000

// ------------------------ Global variables for client ------------------------
int sockfd;
int id = -1;
char topic[MAX_TOPIC_SIZE];

// For subscriber
fd_set readmask;
struct timeval timeout;

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

int unregister_from_broker(operations operation, int id,
                           char topic[MAX_TOPIC_SIZE]) {
    message msg;

    msg.action = operation;
    strcpy(msg.topic, topic);
    msg.id = id;

    if (send(sockfd, &msg, sizeof(msg), MSG_WAITALL) < 0) {
        ERROR("Fail to send");
        return 0;
    }

    return 1;
}

int wait_ack_broker(char topic[MAX_TOPIC_SIZE]) {
    broker_response resp;
    int status = 1;

    if (recv(sockfd, &resp, sizeof(resp), MSG_WAITALL) < 0) {
        ERROR("Fail to received");
        close(sockfd);
        return 1;
    }

    // Print response ----------------------------------------------------------
    switch (resp.response_status) {
    case OK:
        LOG("Registrado correctamente con ID: %d para topic %s\n",
            resp.id, topic);
        id = resp.id;
        status = 0;
        break;
    case LIMIT:
    case ERROR:
        LOG("Error al hacer el registro: error=%d\n", resp.response_status);
        break;
    }

    return status;
}

int wait_unregister_broker(int id) {
    broker_response resp;
    int status = 1;

    if (recv(sockfd, &resp, sizeof(resp), MSG_WAITALL) < 0) {
        ERROR("Fail to received");
        close(sockfd);
        return 1;
    }

    // Print response ----------------------------------------------------------
    if (resp.id != id) {
        printf("Id: %d, my %d\n", resp.id, id);
        LOG("Error al hacer el de-registro: error=ID_INCORRECTA\n");
        return 1;
    }

    switch (resp.response_status) {
    case OK:
        LOG("De-Registrado (%d) correctamente del broker.\n",
            resp.id);
        id = resp.id;
        status = 0;
        break;
    case LIMIT:
    case ERROR:
        LOG("Error al hacer el de-registro: error=%d\n", resp.response_status);
        break;
    }

    close(sockfd);

    return status;
}

// -------------------------- Functions for publisher --------------------------
int init_publisher(char broker_ip[MAX_IP_SIZE], int broker_port,
                   char _topic[MAX_TOPIC_SIZE]) {
    sockfd = init_client(broker_ip, broker_port);

    // // Send request message ----------------------------------------------------
    send_request_to_broker(REGISTER_PUBLISHER, _topic);
    strcpy(topic, _topic);

    // // Listen for response messages --------------------------------------------
    return wait_ack_broker(_topic);
}

int publish(char _data[MAX_DATA_SIZE]) {
    message msg;

    msg.action = PUBLISH_DATA;
    strcpy(msg.topic, topic);
    strcpy(msg.data.data, _data);
    clock_gettime(CLOCK_REALTIME, &msg.data.time_generated_data);

    if (send(sockfd, &msg, sizeof(msg), MSG_WAITALL) < 0) {
        ERROR("Fail to send");
        return 0;
    }

    LOG("Publicado mensaje topic: %s - mensaje: %s - Generó: %ld.%.9ld\n",
        msg.topic,
        msg.data.data,
        msg.data.time_generated_data.tv_sec,
        msg.data.time_generated_data.tv_nsec);

    return 1;
}

int end_publisher() {
    unregister_from_broker(UNREGISTER_PUBLISHER, id, topic);
    return wait_unregister_broker(id);
}

// -------------------------- Functions for subscriber ---------------------------
int subscribe(char broker_ip[MAX_IP_SIZE], int broker_port,
                   char _topic[MAX_TOPIC_SIZE]) {
    sockfd = init_client(broker_ip, broker_port);

    // // Send request message ----------------------------------------------------
    send_request_to_broker(REGISTER_SUBSCRIBER, _topic);
    strcpy(topic, _topic);

    // // Listen for response messages --------------------------------------------
    return wait_ack_broker(_topic);                   
}

char * listen_topic() {
    publish_msg resp;
    struct timespec recv_time, latency;

    FD_ZERO(&readmask); // Reset la mascara
    FD_SET(sockfd, &readmask); // Asignamos el nuevo descriptor
    // FD_SET(STDIN_FILENO, &readmask); // Entrada
    timeout.tv_sec=0; timeout.tv_usec=500000; // Timeout de 0.5 seg.

    if (select(sockfd+1, &readmask, NULL, NULL, &timeout )== -1) {
        return NULL;
    }

    if (FD_ISSET(sockfd, &readmask)) {
        if (recv(sockfd, &resp, sizeof(resp), MSG_DONTWAIT) < 0) {
            ERROR("Fail to received");
        }
        clock_gettime(CLOCK_REALTIME, &recv_time);

        latency.tv_sec = recv_time.tv_sec - resp.time_generated_data.tv_sec;
        latency.tv_nsec = recv_time.tv_nsec - resp.time_generated_data.tv_nsec;
        if (latency.tv_nsec < 0) {
            // If 1.9 and 2.1 get 0.2
            latency.tv_nsec = NS_TO_S - latency.tv_nsec;
            latency.tv_sec--;
        }

        LOG("Recibido mensaje topic: %s - mensaje: %s - Generó: %ld.%.9ld \
- Recibido: %ld.%.9ld - Latencia: %ld.%.6ld\n", topic, resp.data,
            resp.time_generated_data.tv_sec,
            resp.time_generated_data.tv_nsec, 
            recv_time.tv_sec,
            recv_time.tv_nsec,
            latency.tv_sec,
            latency.tv_nsec / NS_TO_MICROS
        );
    }
};

int end_subscriber() {
    unregister_from_broker(UNREGISTER_SUBSCRIBER, id, topic);
    return wait_unregister_broker(id);
}

// -----------------------------------------------------------------------------