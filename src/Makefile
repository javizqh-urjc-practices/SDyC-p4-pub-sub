# Author: Javier Izquierdo Hernandez (javizqh@gmail.com)
# Also: j.izquierdoh.2021@alumnos.urjc.es
# Date: 2023-10-18

# If compile with debug change to this $(CC) -c -DDEBUG -o $@ $< $(CFLAGS)

CC=gcc
CFLAGS=-I.
DEPS =

%.o: %.c $(DEPS)
	$(CC) -g -c -o $@ $< $(CFLAGS)

all: stub.o publisher subscriber broker

publisher: publisher.c 
	gcc -g -Wall -o publisher publisher.c -lpthread stub.o

subscriber: subscriber.c 
	gcc -g -Wall -o subscriber subscriber.c -lpthread stub.o

broker: broker.c 
	gcc -g -Wall -o broker broker.c -lpthread stub.o

clean:
	rm -f *.o publisher subscriber broker
