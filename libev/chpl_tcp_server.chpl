extern proc start(): c_int;

export proc handle_received_data(fd: c_int, buffer: c_string, read: c_int, buffer_size: c_int) {
  writeln("from chpl: " + buffer);
}

proc main() {
  start();
}

