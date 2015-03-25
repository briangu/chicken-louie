CC=clang
LDIR=/opt/local/Cellar/libev/4.15/lib
IDIR=/opt/local/Cellar/libev/4.15/include
CFLAGS=-I$(IDIR)
LIBS=-lev

all: chpl_tcp_server

tcp_server: tcp_server.o
	$(CC) $(CFLAGS) -c -o tcp_server.o tcp_server.c

chpl_tcp_server: tcp_server.o
	chpl tcp_server.h tcp_server.o -L$(LDIR) $(LIBS) -o chpl_tcp_server chpl_tcp_server.chpl

