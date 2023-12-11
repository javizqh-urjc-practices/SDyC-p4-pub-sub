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

#include <threads.h>
#include "stub_broker.h"
#include "stub.h"

#define ERROR(msg) { fprintf(stderr,"PROXY ERROR: %s",msg); perror("");}
#define INFO(msg) fprintf(stderr,"PROXY INFO: %s\n",msg)

#define NS_TO_S 1000000000
#define NS_TO_MICROS 1000
#define MICROS_TO_MS 1000

// ------------------------ Global variables for server ------------------------
// Data structure to store publishers and subscribers
typedef enum {
    UNDEFINED = 0,
    PUBLISHER,
    SUBSCRIBER
} client_types;

typedef struct client {
    client_types type;
    pthread_t thread;
    int socket_fd;
    int global_id, topic_id, id;
} client_t;

client_t * new_client() {
    client_t *client = malloc(sizeof(client_t));
    memset(client, 0, sizeof(client_t));
    if (client == NULL) {
        ERROR("Unable to allocate memory");
        return NULL;
    }
    
    client->type = UNDEFINED;
    client->id = -1;
    client->topic_id = -1;

    return client;
}

typedef struct topic {
    char name[MAX_TOPIC_SIZE];
    int n_pub;
    int n_sub;
} topic_t;

topic_t * new_topic(char name[MAX_TOPIC_SIZE]) {
    topic_t *topic = malloc(sizeof(topic_t));
    memset(topic, 0, sizeof(topic_t));
    if (topic == NULL) {
        ERROR("Unable to allocate memory");
        return NULL;
    }

    strcpy(topic->name, name);
    topic->n_pub = 0;
    topic->n_sub = 0;

    return topic;
};

typedef struct database {
    topic_t * topics[MAX_TOPICS];
    client_t *clients[MAX_PUBLISHERS + MAX_SUBSCRIBERS];
    int topics_size;
    int n_pub;
    int n_sub;
} database_t;

int sockfd;
struct sockaddr *server_addr;
socklen_t server_len;
database_t *database;

pthread_mutex_t mutex_database;

void add_to_database(client_t *client, char topic[MAX_TOPIC_SIZE]) {
    pthread_mutex_lock(&mutex_database);
    // Check topic first
    if (database->topics_size == 0) {
        database->topics[0] = new_topic(topic);
        client->topic_id = 0;
    } else if (database->topics_size < MAX_TOPIC_SIZE){
        for (int i = 0; i < database->topics_size; i++) {
            if (strstr(database->topics[i]->name, topic) == 0) {
                client->topic_id = i;
                database->topics_size++;
                break;
            }
        }
    }

    // Topic has not been found
    if (client->topic_id < 0) {
        if (database->topics_size < MAX_TOPIC_SIZE - 1) {
            database->topics[database->topics_size] = new_topic(topic);
            client->topic_id = database->topics_size;
            database->topics_size++;
        } else {
            // Limit reached in topics
            pthread_mutex_unlock(&mutex_database);
            return;
        }
    }
    

    if (client->type == PUBLISHER) {
        if (database->n_pub >= MAX_PUBLISHERS) {
            pthread_mutex_unlock(&mutex_database);
            return;
        }
    } else {
        if (database->n_sub >= MAX_SUBSCRIBERS) {
            pthread_mutex_unlock(&mutex_database);
            return;
        }
    }

    database->clients[database->n_pub + database->n_sub] = client;
    client->global_id = database->n_pub + database->n_sub;

    if (client->type == PUBLISHER) {
        database->topics[client->topic_id]->n_pub++;
        client->id = database->n_pub;
        database->n_pub++;
    } else {
        database->topics[client->topic_id]->n_sub++;
        database->n_sub++;
        client->id = database->n_sub;
    }

    pthread_mutex_unlock(&mutex_database);
}

void remove_from_database(client_t *client) {
    pthread_mutex_lock(&mutex_database);
    // Check topic first
    if (client->type == PUBLISHER) database->topics[client->topic_id]->n_pub--;
    else database->topics[client->topic_id]->n_sub--;

    if (database->topics[client->topic_id]->n_pub +
        database->topics[client->topic_id]->n_sub <= 0) {
        // Remove topic
        free(database->topics[client->topic_id]);
        for (int i = database->topics_size - 1; i >= client->topic_id; i--) {
            database->topics[i + 1] = database->topics[i];
        }
        database->topics_size--;
    }

    // Now shift all the other clients and free
    close(client->socket_fd);
    free(client);
    for (int i = database->n_pub + database->n_sub - 1;
         i >= client->global_id; i--) {
        database->clients[i + 1] = database->clients[i];
    }

    if (client->type == PUBLISHER) {
        database->n_pub--;
    } else {
        database->n_sub--;
    }

    pthread_mutex_unlock(&mutex_database);
}

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

    sock_fd = socket(AF_INET, SOCK_STREAM, 0);

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

    if (listen(sock_fd, MAX_PUBLISHERS + MAX_SUBSCRIBERS) < 0) {
        ERROR("Unable to listen");
        return 0;
    }

    // Set global variables ----------------------------------------------------
    sockfd = sock_fd;
    memcpy(addr, (struct sockaddr *) &servaddr, sizeof(struct sockaddr));
    server_addr = addr;
    server_len = len;
    database_t *data = malloc(sizeof(database_t));
    memset(data, 0, sizeof(database_t));
    if (data == NULL) {
        ERROR("Unable to allocate memory");
        return 0;
    }
    database = data;

    pthread_mutex_init(&mutex_database, NULL);

    return sock_fd > 0;
}

void * proccess_client_thread(void * args) {
    client_t * client = (client_t *) args;
    message msg;
    broker_response resp;

    // Listen for connection message -------------------------------------------
    if (recv(client->socket_fd, &msg, sizeof(msg), MSG_WAITALL) < 0) {
        ERROR("Fail to received");
        close(client->socket_fd);
        free(client);
        pthread_exit(NULL);
    }
    // See if we can add it ----------------------------------------------------
    switch (msg.action) {
    case REGISTER_PUBLISHER:
        client->type = PUBLISHER;
        break;
    case REGISTER_SUBSCRIBER:
        client->type = SUBSCRIBER;
        break;
    default: // Other message type exit with error
        ERROR("Incorrect message type");
        close(client->socket_fd);
        free(client);
        pthread_exit(NULL);
        break;
    }

    add_to_database(client, msg.topic);
    // Send acknowledge --------------------------------------------------------
    resp.id = client->id;
    
    if (resp.id < 0) resp.response_status = LIMIT;

    if (send(client->socket_fd, &resp, sizeof(resp), MSG_WAITALL) < 0) {
        ERROR("Failed to send");
        remove_from_database(client);
    }

    // Start normal function ---------------------------------------------------
    if (client->type == PUBLISHER) {
        while (1) {
            if (recv(client->socket_fd, &msg, sizeof(msg), MSG_WAITALL) < 0) {
                ERROR("Fail to received");
                remove_from_database(client);
                pthread_exit(NULL);
            }
            // Procces if PUBLISH or UNREGISTER
        }
        
    }
    // If msg is unregistered remove from list and sent unregister ack 

    // Free all memory and close sockets ---------------------------------------
    remove_from_database(client);
    // Allow another thread in -------------------------------------------------
    pthread_exit(NULL);
};

int wait_for_connection() {
    client_t * client = new_client();

    if (client == NULL) return 0;

    // Accept connection -------------------------------------------------------
    if ((client->socket_fd = accept(sockfd, server_addr, &server_len)) < 0) {
        ERROR("failed to accept socket");
        return 0;
    }

    pthread_create(&client->thread, NULL, proccess_client_thread, client);
    pthread_detach(client->thread); // To free thread memory after finish

    return 1;
}