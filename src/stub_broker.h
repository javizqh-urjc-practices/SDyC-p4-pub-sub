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

#define MAX_TOPICS 10
#define MAX_PUBLISHERS 100
#define MAX_SUBSCRIBERS 900

/**
 * @brief 
 * 
 * @param port 
 * @return int 
 */
int load_config_broker(int port);

/**
 * @brief 
 * 
 * @return int 
 */
int wait_for_connection();