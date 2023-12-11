#include <unistd.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <getopt.h>
#include <err.h>

#include "stub_broker.h"

#define N_ARGS 4
#define MODE_SEQUENTIAL_STR "secuencial"
#define MODE_PARALLEL_STR "paralelo"
#define MODE_FAIR_STR "justo"

typedef enum {
    MODE_SEQUENTIAL = 0,
    MODE_PARALLEL,
    MODE_FAIR
} broker_mode_t;

typedef struct {
    int port;
    broker_mode_t mode;
} broker_args;


broker_args * check_args(int argc, char *const *argv);

void usage() {
    fprintf(stderr, "usage: ./broker --port BROKER_PORT --mode MODE\n");
    exit(EXIT_FAILURE);
}

int main(int argc, char *const *argv) {
    broker_args * arguments = check_args(argc, argv);

    printf("Mode: %d\n", arguments->mode);
    load_config_broker(arguments->port);

    while (1) {
        wait_for_connection();
    }
    

    free(arguments);
    return 0;
}

broker_args * check_args(int argc, char *const *argv) {
    int c;
    int opt_index = 0;
    static struct option long_options[] = {
        {"port",        required_argument, 0,  0 },
        {"mode",        required_argument, 0,  0 },
        {0,             0,                 0,  0 }
    };
    broker_args * arguments = malloc(sizeof(broker_args));

    if (arguments == NULL) err(EXIT_FAILURE, "Failed to allocate arguments");
    memset(arguments, 0, sizeof(broker_args));

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
            if (strcmp(long_options[opt_index].name, "mode") == 0) {
                if (strcmp(optarg ,MODE_SEQUENTIAL_STR) == 0) {
                    arguments->mode = MODE_SEQUENTIAL;
                } else if (strcmp(optarg ,MODE_PARALLEL_STR) == 0) {
                    arguments->mode = MODE_PARALLEL;
                } else if (strcmp(optarg ,MODE_FAIR_STR) == 0) {
                    arguments->mode = MODE_FAIR;
                } else {
                    free(arguments);
                    usage();
                }
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