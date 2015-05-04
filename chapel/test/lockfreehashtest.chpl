use LockFreeHash;

proc storeItemTest(lfh: LockFreeHash, key: KeyType, value: ValueType) {
  writeln("attempting to store value: ", value, " with key ", key);
  lfh.setItem(key, value);
  writeln("attempting to get value with key ", key);
  var rItem = lfh.getItem(key);
  write("value == rItem\t", value == rItem);
}

proc main() {
  var lfh = new LockFreeHash(2);
  var item1: uint(32) = 32;
  var item2: uint(32) = 48;
  var item3: uint(32) = 64;
  storeItemTest(lfh, genHashKey32("hello1"), item1);
  storeItemTest(lfh, genHashKey32("hello2"), item2);
  storeItemTest(lfh, genHashKey32("hello3"), item3);
}
