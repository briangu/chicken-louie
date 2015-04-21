use LibEv, IO, Search;

// TODO: port to pure chapel
extern proc initialize_socket(port: c_int): c_int;

// trampolines
extern var c_accept_cb: opaque;

extern proc send(sockfd:c_int, buffer: c_string, len: size_t, flags: c_int);

config var port: c_int = 3033;

export proc handle_received_data(fd: c_int, buffer: c_string, read: size_t, buffer_size: size_t) {
  // writeln("from chpl: " + buffer);
  // accumulate string buffer

  send(fd, buffer, read, 0);
}

proc initIndex() {
	writeln("This program is running on ", numLocales, " locales");
	writeln("It began running on locale #", here.id);
	writeln();

	initPartitions();

	indexWord("dog", 1);
	indexWord("cat", 2);
	indexWord("cat", 3);
	dumpPartition(partitionForWord("dog"));
	// dumpPostingTable("cat");
}

proc main(): c_long {

	writeln("creating socket...");
	var sd: ev_fd = initialize_socket(port);
	writeln("socket id = ", sd);
	if (sd == -1) {
		writeln("socket error");
		return -1;
	}

	writeln("initializing event loop...");

	var w_accept: ev_io = new ev_io();
	ev_io_init(w_accept, c_accept_cb, sd, EV_READ);
	ev_io_start(EV_DEFAULT, w_accept);

	while (1) {
		ev_loop_fn(EV_DEFAULT, 0);
	}

	return 0;
}
