/**
 * @file stub.h
 * @author Javier Izquierdo Hernandez (j.izquierdoh.2021@alumnos.urjc.es)
 * @brief 
 * @version 0.1
 * @date 2023-12-04
 * 
 * @copyright Copyright (c) 2023
 * 
 */

#include <time.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <pthread.h>
#include <err.h>
#include <string.h>
#include <semaphore.h>
#include <sys/stat.h>
#include <fcntl.h>

#define MAX_IP_SIZE 16
#define MAX_TOPIC_SIZE 100
#define MAX_DATA_SIZE 100

#define LOG(...) { print_epoch(); fprintf(stdout, __VA_ARGS__);}

typedef enum operations {
    REGISTER_PUBLISHER = 0,
    UNREGISTER_PUBLISHER,
    REGISTER_SUBSCRIBER,
    UNREGISTER_SUBSCRIBER,
    PUBLISH_DATA
} operations;

typedef struct publish {
    struct timespec time_generated_data;
    char data[MAX_DATA_SIZE];
} publish_msg;

typedef struct message {
    operations action;
    char topic[MAX_TOPIC_SIZE];
    // Solo utilizado en mensajes de UNREGISTER
    int id;
    // Solo utilizado en mensajes PUBLISH_DATA
    publish_msg data;
} message;

typedef enum {
    ERROR = 0,
    LIMIT,
    OK
} broker_status_t;

typedef struct broker_response{
    broker_status_t response_status;
    int id;
} broker_response;

/**
 * @brief Initializes the publisher
 * 
 * @param broker_ip 
 * @param broker_port 
 * @param topic 
 * @return int 0 = ERROR / 1 = OK
 */
int init_publisher(char broker_ip[MAX_IP_SIZE], int broker_port,
                   char topic[MAX_TOPIC_SIZE]);
/**
 * @brief 
 * 
 * @param topic 
 * @param data 
 * @return int 
 */
int publish(char topic[MAX_TOPIC_SIZE], char data[MAX_DATA_SIZE]);

/**
 * @brief 
 * 
 * @param id 
 * @return int 
 */
int end_publisher(int id);

/**
 * @brief 
 * 
 * @param broker_ip 
 * @param broker_port 
 * @param topic 
 * @return int 
 */
int subscribe(char broker_ip[MAX_IP_SIZE], int broker_port,
                   char topic[MAX_TOPIC_SIZE]);

/**
 * @brief 
 * 
 * @return char* 
 */
char * listen_topic();

/**
 * @brief 
 * 
 * @param id 
 * @return int 
 */
int end_subscriber(int id);

void print_epoch();