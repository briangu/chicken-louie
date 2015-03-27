CC=clang
LDIR=/opt/local/Cellar/libev/4.15/lib
IDIR=/opt/local/Cellar/libev/4.15/include
CHICKEN_IDIR=/opt/local/Cellar/chicken/4.9.0.1/include/chicken
CFLAGS=-I$(IDIR)
LIBS=-lev

all: chpl_tcp_server c_tcp_server chicken_ev

tcp_server: tcp_server.o
	$(CC) $(CFLAGS) -c -o tcp_server.o tcp_server.c

chpl_tcp_server: tcp_server.o
	chpl tcp_server.h tcp_server.o -L$(LDIR) $(LIBS) -o chpl_tcp_server chpl_tcp_server.chpl

c_tcp_server: tcp_server.o
	$(CC) -I$(IDIR) -L$(LDIR) $(LIBS) -o c_tcp_server tcp_server.c tcp_server_main.c

chicken_ev: tcp_server.o
	$(CC) -I$(CHICKEN_IDIR) -I$(IDIR) -c callbacks.c
	csc -I$(IDIR) -L$(LDIR) $(LIBS) -o chicken_ev callbacks.o libev.scm

clean:
	rm -f *.o
	rm -f chpl_tcp_server
	rm -f c_tcp_server
	rm -f chicken_ev
