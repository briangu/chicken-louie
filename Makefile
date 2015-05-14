CC=clang
LDIR=/opt/local/Cellar/libev/4.15/lib
IDIR=/opt/local/Cellar/libev/4.15/include
CHICKEN_IDIR=/opt/local/Cellar/chicken/4.9.0.1/include/chicken
CFLAGS=-I$(IDIR) -I.
LIBS=-lev

all: chpl_tcp_server c_tcp_server chicken_ev

tcp_server: tcp_server.o
	$(CC) $(CFLAGS) -c -o tcp_server.o tcp_server.c

chpl_tcp_server: tcp_server.o
	chpl --no-local --fast --print-passes tcp_server.h tcp_server.o chapel/callbacks.h chapel/callbacks.c -Mchapel -I$(IDIR) -L$(LDIR) $(LIBS) -o chpl_tcp_server chapel/chpl_tcp_server.chpl

crosstalk:
	chpl --no-local --print-passes -o crosstalk chapel/crosstalk.chpl 

c_tcp_server: tcp_server.o
	$(CC) $(CFLAGS) -L$(LDIR) $(LIBS) -o c_tcp_server tcp_server.c c/tcp_server_main.c

chicken_ev: tcp_server.o
	$(CC) -I$(CHICKEN_IDIR) $(CFLAGS) -c chicken/callbacks.c
	csc $(CFLAGS) -Ichicken -L$(LDIR) $(LIBS) -o chicken_ev callbacks.o chicken/libev.scm

clean:
	rm -f *.o
	rm -f chpl_tcp_server
	rm -f c_tcp_server
	rm -f chicken_ev
	rm -f crosstalk
