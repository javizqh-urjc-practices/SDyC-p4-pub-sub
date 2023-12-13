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
void print_epoch() {
    struct timespec epoch;

    clock_gettime(CLOCK_REALTIME, &epoch);
    printf("[%ld.%.9ld] " , epoch.tv_sec, epoch.tv_nsec);
}
#define LOG(...) { print_epoch(); fprintf(stdout, __VA_ARGS__);}

#define NS_TO_S 1000000000
#define NS_TO_MICROS 1000
#define MICROS_TO_MS 1000

#define UNUSED 0
#define USED 1

// ------------------------ Global variables for server ------------------------
// Data structure to store publishers and subscribers
typedef struct hashtable {
    short publishers[100];
    short subscribers[900];
    short topics[10];
} id_hashtable_t;

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
    id_hashtable_t id_hashtable;
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
        database->id_hashtable.topics[0] = USED;
        database->topics_size++;
    } else if (database->topics_size < MAX_TOPIC_SIZE){
        for (int i = 0; i < database->topics_size; i++) {
            if (strcmp(database->topics[i]->name, topic) == 0) {
                client->topic_id = i;
                break;
            }
        }
    }

    // Topic has not been found
    if (client->topic_id < 0) {
        if (database->topics_size < MAX_TOPIC_SIZE - 1) {
            for (size_t i = 0; i < MAX_TOPICS; i++) {
                if (database->id_hashtable.topics[i] == UNUSED) {
                    database->id_hashtable.topics[i] = USED;
                    database->topics[i] = new_topic(topic);
                    client->topic_id = i;
                    database->topics_size++;
                    break;
                }
            }
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
        for (size_t i = 0; i < MAX_PUBLISHERS; i++) {
            if (database->id_hashtable.publishers[i] == UNUSED) {
                database->id_hashtable.publishers[i] = USED;
                database->topics[client->topic_id]->n_pub++;
                client->id = i;
                database->n_pub++;
                break;
            }
        }
    } else {
        for (size_t i = 0; i < MAX_SUBSCRIBERS; i++) {
            if (database->id_hashtable.subscribers[i] == UNUSED) {
                database->id_hashtable.subscribers[i] = USED;
                database->topics[client->topic_id]->n_sub++;
                database->n_sub++;
                client->id = i + 1;
                break;
            }
        }
    }

    pthread_mutex_unlock(&mutex_database);
}

void remove_from_database(client_t *client) {
    int client_id = client->global_id;

    pthread_mutex_lock(&mutex_database);
    // Check topic first
    if (client->type == PUBLISHER) database->topics[client->topic_id]->n_pub--;
    else database->topics[client->topic_id]->n_sub--;

    if (database->topics[client->topic_id]->n_pub +
        database->topics[client->topic_id]->n_sub <= 0) {
        // Remove topic
        database->id_hashtable.topics[client->topic_id] = UNUSED;
        free(database->topics[client->topic_id]);
        for (int i = database->topics_size - 2; i >= client->topic_id; i--) {
            fprintf(stderr,"Topic shifted %d, %s\n",i, database->topics[i + 1]->name);
            database->topics[i] = database->topics[i + 1];
        }
        database->topics_size--;
    }

    // Now shift all the other clients and free
    for (int i = database->n_pub + database->n_sub - 1;
        i >= client_id; i--) {
        database->clients[i]->global_id--;
        database->clients[i + 1] = database->clients[i];
    }

    if (client->type == PUBLISHER) {
        database->id_hashtable.publishers[client_id] = UNUSED;
        database->n_pub--;
    } else {
        database->id_hashtable.subscribers[client_id] = UNUSED;
        database->n_sub--;
    }

    close(client->socket_fd);
    free(client);
    pthread_mutex_unlock(&mutex_database);
}

void print_summary() {
    printf("Resumen:\n");
    for (size_t i = 0; i < database->topics_size; i++) {
       printf("\t%s: %d Suscriptores - %d Publicadores\n", 
              database->topics[i]->name,
              database->topics[i]->n_sub,
              database->topics[i]->n_pub);
    }
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
    else resp.response_status = OK;

    if (send(client->socket_fd, &resp, sizeof(resp), MSG_WAITALL) < 0) {
        ERROR("Failed to send");
        remove_from_database(client);
    }

    if (resp.response_status != OK) {
        close(client->socket_fd);
        free(client);
        pthread_exit(NULL);
    } else {
        if (client->type == PUBLISHER) {
            LOG("Nuevo cliente (%d) Publicador conectado : %s\n", client->id,
                msg.topic);
        } else {
            LOG("Nuevo cliente (%d) Subscriptot conectado : %s\n", client->id,
                msg.topic);
        }
        print_summary();
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
            if (msg.action == PUBLISH_DATA) {
                // Resend data Unleashed cond variable of topic subscribers
                LOG("Recibido mensaje para publicar en topic: %s - mensaje: %s \
- Generó: %ld.%.9ld\n", msg.topic, msg.data.data,
                    msg.data.time_generated_data.tv_sec,
                    msg.data.time_generated_data.tv_nsec);

            } else if (msg.action == UNREGISTER_PUBLISHER) {
                resp.response_status = OK;
                resp.id = client->id;
                if (send(client->socket_fd, &resp, sizeof(resp), MSG_WAITALL) < 0) {
                    ERROR("Failed to send");
                }
                remove_from_database(client);
                LOG("Eliminado cliente (%d) Publicador : %s\n", resp.id,
                    msg.topic);
                print_summary();
                pthread_exit(NULL);
            } else {
                // ERROR
            }
        }
    } else if (client->type == SUBSCRIBER) {
        //
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