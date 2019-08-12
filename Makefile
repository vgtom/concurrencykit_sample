CFLAGS=-D_XOPEN_SOURCE=600 -std=gnu99 -pedantic -Wall -W -Wundef -Wendif-labels -Wshadow -Wpointer-arith -Wcast-align -Wcast-qual -fstrict-aliasing -O2 -pipe -Wno-parentheses  -fPIC
PTHREAD_FLAGS=-lpthread -Wall

all:
	gcc $(CFLAGS) -o prog prog.c ../ck/src/ck_ht.c ../ck/src/ck_epoch.c $(PTHREAD_FLAGS)
