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
#include <semaphore.h>
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
#define LOCK_GLOBAL(mutex) pthread_mutex_lock(&mutex);
#define UNLOCK_GLOBAL(mutex) pthread_mutex_unlock(&mutex);
#define LOCK_LOCAL(mutex) pthread_mutex_lock(&mutex);
#define UNLOCK_LOCAL(mutex) pthread_mutex_unlock(&mutex);

#define NS_TO_S 1000000000
#define NS_TO_MICROS 1000
#define MICROS_TO_MS 1000

#define UNUSED 0
#define USED 1

// ------------------------ Global variables for server ------------------------
// Queue structure for resending messages
typedef struct node {
    publish_msg data;
    struct node *next;
} node_msg_t;

typedef struct queue {
    node_msg_t * head; // Queue for resends
    node_msg_t * tail; // Queue for resends
    pthread_mutex_t mutex;
} queue;

queue * new_queue() {
    queue * q = (queue *) malloc(sizeof(queue));
    if (q == NULL) {
        ERROR("failed to allocate data");
        return NULL;
    }
    memset(q, 0, sizeof(queue));

    q->head = NULL;
    q->tail = NULL;

    pthread_mutex_init(&q->mutex, NULL);

    return q;
}

void free_queue(queue * q) {
    node_msg_t * to_free = q->head;
    node_msg_t * next = to_free;

    if (to_free != NULL) {
        if (to_free == q->tail) free(to_free);
        else {
            while (next) {
                to_free = next;
                next = to_free->next;
                free(to_free);
            }
        }
    }

    pthread_mutex_destroy(&q->mutex);
    free(q);
}

void queue_msg(queue * q, publish_msg data) {
    node_msg_t * new_msg = (node_msg_t *) malloc(sizeof(node_msg_t));
    if (new_msg == NULL) {
        ERROR("failed to allocate data");
        return;
    }
    memset(new_msg, 0, sizeof(node_msg_t));

    pthread_mutex_lock(&q->mutex);

    new_msg->data = data;
    new_msg->next = NULL;

    if (q->head == NULL) q->head = new_msg;
    else q->tail->next = new_msg;

    q->tail = new_msg;
    pthread_mutex_unlock(&q->mutex);
    return;
}

int has_msg(queue * q) {
    int return_value = 0;

    pthread_mutex_lock(&q->mutex);
    return_value = q->head != NULL;
    pthread_mutex_unlock(&q->mutex);

    return return_value;
}

void delete_msg(queue * q) {
    node_msg_t * to_free = q->head;

    pthread_mutex_lock(&q->mutex);
    if (q->head == q->tail) q->tail = NULL;

    q->head = to_free->next;
    free(to_free);
    pthread_mutex_unlock(&q->mutex);
}
// Queue structure for resending messages
typedef struct node_sub {
    int id;
    struct node_sub *next;
    struct node_sub *prev;
} node_sub_t;

typedef struct queue_sub {
    node_sub_t * head; // Queue for secuential resends
    node_sub_t * tail; // Queue for secuential resends
    node_sub_t * curr; // Queue for secuential resends
    int msg_resend; // Number of messages resended
} queue_sub;

queue_sub * new_queue_sub() {
    queue_sub * q = (queue_sub *) malloc(sizeof(queue_sub));
    if (q == NULL) {
        ERROR("failed to allocate data");
        return NULL;
    }
    memset(q, 0, sizeof(queue_sub));

    q->head = NULL;
    q->tail = NULL;
    q->curr = NULL;
    q->msg_resend = 0;

    return q;
}

void add_sub(queue_sub * q, int id) {
    node_sub_t * new_sub = (node_sub_t *) malloc(sizeof(node_sub_t));
    if (new_sub == NULL) {
        ERROR("failed to allocate data");
        return;
    }
    memset(new_sub, 0, sizeof(node_sub_t));

    new_sub->id = id;
    new_sub->next = NULL;
    new_sub->prev = NULL;

    if (q->head == NULL) q->head = new_sub;
    else q->tail->next = new_sub;

    new_sub->prev = q->tail;
    q->tail = new_sub;
    q->curr = q->head;

    return;
}

void delete_sub(queue_sub * q, int id) {
    node_sub_t * to_free;

    for (to_free = q->head; to_free != NULL; to_free = to_free->next) {
        if (to_free->id == id) break;
    }
    
    if (to_free == NULL) return; // Not found

    if (q->head == to_free) {
        q->head = q->head->next;
        if (q->head) q->head->prev = NULL;
    } else if (q->tail == to_free) {
        q->tail = q->tail->prev;
        if (q->tail) q->tail->next = NULL;
    } else {
        to_free->prev->next = to_free->next;
        to_free->next->prev = to_free->prev;
    }
    q->curr = q->head;
    free(to_free);
}


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
    queue_sub * sub_order; // Need FIFO of global_ids for secuential 
    pthread_mutex_t op_mutex; // Modifications mutex
    pthread_mutex_t utility_mutex;
    pthread_cond_t secuential; // Semaphore for secuential resends
    pthread_barrier_t sync;// Barrier for controlling subscribers
    queue * msg_queue;
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
    topic->msg_queue = new_queue();
    topic->sub_order = new_queue_sub();

    pthread_mutex_init(&topic->op_mutex, NULL);
    pthread_mutex_init(&topic->utility_mutex, NULL);
    pthread_cond_init(&topic->secuential, NULL);

    return topic;
};

void destroy_topic(topic_t * topic) {
    pthread_mutex_destroy(&topic->op_mutex);
    pthread_mutex_destroy(&topic->utility_mutex);
    pthread_cond_destroy(&topic->secuential);
    free(topic->sub_order); // No need to free nodes anymore
    free_queue(topic->msg_queue);
    free(topic);
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
broker_mode_t mode = MODE_SEQUENTIAL;

void add_to_database(client_t *client, char topic[MAX_TOPIC_SIZE]) {
    // Check topic first
    if (database->topics_size == 0) {
        client->topic_id = 0;
        LOCK_GLOBAL(mutex_database);
        database->topics[0] = new_topic(topic);
        database->id_hashtable.topics[0] = USED;
        database->topics_size++;
        UNLOCK_GLOBAL(mutex_database);
    } else if (database->topics_size < MAX_TOPIC_SIZE){
        for (int i = 0; i < database->topics_size; i++) {
            LOCK_LOCAL(database->topics[i]->op_mutex);
            if (strcmp(database->topics[i]->name, topic) == 0) {
                client->topic_id = i;
                UNLOCK_LOCAL(database->topics[i]->op_mutex);
                break;
            }
            UNLOCK_LOCAL(database->topics[i]->op_mutex);
        }
    }

    // Topic has not been found
    if (client->topic_id < 0) {
        if (database->topics_size < MAX_TOPIC_SIZE - 1) {
            LOCK_GLOBAL(mutex_database);
            for (size_t i = 0; i < MAX_TOPICS; i++) {
                if (database->id_hashtable.topics[i] == UNUSED) {
                    database->id_hashtable.topics[i] = USED;
                    database->topics[i] = new_topic(topic);
                    client->topic_id = i;
                    database->topics_size++;
                    break;
                }
            }
            UNLOCK_GLOBAL(mutex_database);
        } else {
            // Limit reached in topics
            return;
        }
    }

    LOCK_GLOBAL(mutex_database);
    if (client->type == PUBLISHER) {
        if (database->n_pub >= MAX_PUBLISHERS) {
            UNLOCK_GLOBAL(mutex_database);
            return;
        }
    } else {
        if (database->n_sub >= MAX_SUBSCRIBERS) {
            UNLOCK_GLOBAL(mutex_database);
            return;
        }
    }
    UNLOCK_GLOBAL(mutex_database);

    if (client->type == PUBLISHER) {
        for (size_t i = 0; i < MAX_PUBLISHERS; i++) {
            if (database->id_hashtable.publishers[i] == UNUSED) {
                LOCK_GLOBAL(mutex_database);
                database->clients[i] = client;
                database->id_hashtable.publishers[i] = USED;
                database->n_pub++;
                UNLOCK_GLOBAL(mutex_database);

                LOCK_LOCAL(database->topics[client->topic_id]->op_mutex);
                database->topics[client->topic_id]->n_pub++;
                UNLOCK_LOCAL(database->topics[client->topic_id]->op_mutex);

                client->global_id = i;
                client->id = i;
                break;
            }
        }
    } else {
        for (size_t i = 0; i < MAX_SUBSCRIBERS; i++) {
            if (database->id_hashtable.subscribers[i] == UNUSED) {
                LOCK_GLOBAL(mutex_database);
                database->clients[i + MAX_PUBLISHERS] = client;
                database->id_hashtable.subscribers[i] = USED;
                database->n_sub++;
                UNLOCK_GLOBAL(mutex_database);

                client->global_id = i + MAX_PUBLISHERS;
                client->id = i + 1;

                LOCK_LOCAL(database->topics[client->topic_id]->op_mutex);
                if (database->topics[client->topic_id]->n_sub > 0) {
                    pthread_barrier_destroy(&database->topics[client->topic_id]->sync); // So that next time the number of subs could change
                }
                database->topics[client->topic_id]->n_sub++;
                pthread_barrier_init(&database->topics[client->topic_id]->sync,
                                     NULL,
                                     database->topics[client->topic_id]->n_sub);
                add_sub(database->topics[client->topic_id]->sub_order, client->global_id);
                UNLOCK_LOCAL(database->topics[client->topic_id]->op_mutex);
                break;
            }
        }
    }
}

void remove_from_database(client_t *client) {
    int client_id = client->global_id;

    // Check topic first
    LOCK_LOCAL(database->topics[client->topic_id]->op_mutex);
    if (client->type == PUBLISHER) database->topics[client->topic_id]->n_pub--;
    else database->topics[client->topic_id]->n_sub--;

    if (database->topics[client->topic_id]->n_pub +
        database->topics[client->topic_id]->n_sub <= 0) {
        // Remove topic
        LOCK_GLOBAL(mutex_database);
        database->id_hashtable.topics[client->topic_id] = UNUSED;
        database->topics_size--;
        UNLOCK_GLOBAL(mutex_database);

        UNLOCK_LOCAL(database->topics[client->topic_id]->op_mutex);
        destroy_topic(database->topics[client->topic_id]);
    } else {
        UNLOCK_LOCAL(database->topics[client->topic_id]->op_mutex);
    }

    LOCK_GLOBAL(mutex_database);
    if (client->type == PUBLISHER) {
        database->id_hashtable.publishers[client_id] = UNUSED;
        database->n_pub--;
    } else {
        database->id_hashtable.subscribers[client_id - MAX_PUBLISHERS] = UNUSED;
        if (database->id_hashtable.topics[client->topic_id] == USED) {
            pthread_barrier_destroy(&database->topics[client->topic_id]->sync); // So that next time the number of subs could change
            pthread_barrier_init(&database->topics[client->topic_id]->sync,
                                    NULL,
                                    database->topics[client->topic_id]->n_sub);
            delete_sub(database->topics[client->topic_id]->sub_order, client_id);
        }
        database->n_sub--;
    }
    database->clients[client_id] = NULL;
    UNLOCK_GLOBAL(mutex_database);

    close(client->socket_fd);
    free(client);
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

int load_config_broker(int port, broker_mode_t _mode) {
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
    mode = _mode;
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
    topic_t * topic;
    message msg;
    broker_response resp;
    fd_set readmask;
    struct timeval timeout;

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
            LOG("Nuevo cliente (%d) Suscriptor conectado : %s\n", client->id,
                msg.topic);
        }
        print_summary();
    }

    topic = database->topics[client->topic_id];

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
                // Queue message and wait to mutex to unlock
                if (topic->n_sub > 0) queue_msg(topic->msg_queue, msg.data);
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
        while (1) {
            // Check if subscriber sent UNREGISTER_PUBLISHER
            FD_ZERO(&readmask); // Reset la mascara
            FD_SET(client->socket_fd, &readmask); // Asignamos el nuevo descriptor
            // FD_SET(STDIN_FILENO, &readmask); // Entrada
            timeout.tv_sec=0; timeout.tv_usec=5000; // Timeout de 0.005 seg.
            if (select(client->socket_fd+1, &readmask, NULL, NULL, &timeout )== -1) {
                continue;
            }
            
            if (FD_ISSET(client->socket_fd, &readmask)) {
                if (recv(client->socket_fd, &msg, sizeof(msg), MSG_DONTWAIT) < 0) {
                    ERROR("Fail to received");
                    remove_from_database(client);
                    pthread_exit(NULL);
                }
                if (msg.action == UNREGISTER_SUBSCRIBER) {
                    resp.response_status = OK;
                    resp.id = client->id;
                    if (send(client->socket_fd, &resp, sizeof(resp), MSG_WAITALL) < 0) {
                        ERROR("Failed to send");
                    }
                    remove_from_database(client);
                    LOG("Eliminado cliente (%d) Suscriptor : %s\n", resp.id,
                        msg.topic);
                    print_summary();
                    pthread_exit(NULL);
                } else {
                    // ERROR
                }
            }
            // If message in queue, send it
            if (has_msg(topic->msg_queue)) {
                LOCK_LOCAL(topic->utility_mutex);
                if (topic->sub_order->msg_resend == 0) {
                    // Do not allow for other publishers or subscribers to join this topic
                    LOCK_LOCAL(topic->op_mutex);
                    LOG("Enviando mensaje en topic %s a %d suscriptores.\n", topic->name, topic->n_sub);
                }
                topic->sub_order->msg_resend++;   
                UNLOCK_LOCAL(topic->utility_mutex);
                // Allow all of them and wait for all to reach the barrier at the end, then  destroy it and unlock mutex
                // For parallel only barrier at the end, for fair barrier at the beggining and at the end
                // Secuential is the hardest one
                // Wait for all subscribers to send
                if (mode == MODE_SEQUENTIAL) {
                    LOCK_LOCAL(topic->utility_mutex);
                    while (topic->sub_order->curr->id != client->global_id) {
                        pthread_cond_wait(&topic->secuential, &topic->utility_mutex);
                    }
                    UNLOCK_LOCAL(topic->utility_mutex);
                } else if (mode == MODE_FAIR) {
                    pthread_barrier_wait(&topic->sync);
                }

                if (send(client->socket_fd, &topic->msg_queue->head->data, sizeof(publish_msg), MSG_WAITALL) < 0) {
                    ERROR("Failed to send");
                    remove_from_database(client);
                }

                if (mode == MODE_SEQUENTIAL) {
                    // Allow next one and then wait for all to finish
                    topic->sub_order->curr = topic->sub_order->curr->next;
                    pthread_cond_broadcast(&topic->secuential);
                }

                pthread_barrier_wait(&topic->sync);
                LOCK_LOCAL(topic->utility_mutex);
                if (topic->sub_order->msg_resend == topic->n_sub) {
                    topic->sub_order->msg_resend = 0;
                    topic->sub_order->curr = topic->sub_order->head;
                    delete_msg(topic->msg_queue);
                }
                UNLOCK_LOCAL(topic->utility_mutex);
                UNLOCK_LOCAL(topic->op_mutex);
            }
        }
    }

    // Free all memory and close sockets ---------------------------------------
    remove_from_database(client);
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