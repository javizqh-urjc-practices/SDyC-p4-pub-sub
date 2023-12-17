#include <unistd.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <getopt.h>
#include <err.h>
#include <signal.h>

#include "stub.h"

#define N_ARGS 6

typedef struct args {
    char ip [MAX_IP_SIZE];
    int port;
    char topic [MAX_TOPIC_SIZE];
} * Args;

int has_to_exit = 0;

Args check_args(int argc, char *const *argv);
void sig_handler(int sig);

void usage() {
    fprintf(stderr, "usage: ./publisher --ip BROKER_IP --port BROKER_PORT \
--topic TOPIC\n");

    exit(EXIT_FAILURE);
}

int main(int argc, char *const *argv) {
    Args arguments = check_args(argc, argv);
    char *data = malloc(MAX_DATA_SIZE);
    int proc_fd;
    
    signal(SIGINT, sig_handler);
    signal(SIGPIPE, sig_handler);

    init_publisher(arguments->ip, arguments->port, arguments->topic);
    while (!has_to_exit) {
        // Open file for reading
        proc_fd = open("/proc/loadavg", O_RDONLY);
        // Read /proc/loadavg
        read(proc_fd, data, MAX_DATA_SIZE);
        publish(data);
        // Close the file
        close(proc_fd);
        sleep(3);
    }
    end_publisher();
    
    free(arguments);
    return 0;
}

Args check_args(int argc, char *const *argv) {
    int c;
    int opt_index = 0;
    static struct option long_options[] = {
        {"ip",          required_argument, 0,  0 },
        {"port",        required_argument, 0,  0 },
        {"topic",       required_argument, 0,  0 },
        {0,             0,                 0,  0 }
    };
    Args arguments = malloc(sizeof(struct args));

    if (arguments == NULL) err(EXIT_FAILURE, "Failed to allocate arguments");
    memset(arguments, 0, sizeof(struct args));

    if (argc - 1 != N_ARGS) {
        free(arguments);
        usage();
    }

    while (1) {
        opt_index = 0;

        c = getopt_long(argc, argv, "", long_options, &opt_index);
        if (c == -1)
            break;

        switch (c) {
        case 0:
            if (strcmp(long_options[opt_index].name, "ip") == 0) {
                strcpy(arguments->ip ,optarg);
            } else if (strcmp(long_options[opt_index].name, "topic") == 0) {
                strcpy(arguments->topic ,optarg);
            } else if (strcmp(long_options[opt_index].name, "port") == 0) {
                arguments->port = atoi(optarg);
                if (arguments->port <= 0) {
                    free(arguments);
                    usage();
                }                
            }
            break;
        default:
            printf("?? getopt returned character code 0%o ??\n", c);
        }
    }

    if (optind < argc) {
        free(arguments);
        usage();
    }
    return arguments;
}

void sig_handler(int sig) {
	switch (sig) {
    case SIGINT:
    case SIGPIPE:
        has_to_exit = 1;
        break;
	default:
		break;
	}
}