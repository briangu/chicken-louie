extern proc start(): c_int;

export echo proc echo(str: c_string) {
  writeln("from chpl: " + str);
}

proc main() {
  start();
}
