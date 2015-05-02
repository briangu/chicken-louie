

// Thomas Wang's 64b mix function from http://www.concentric.net/~Ttwang/tech/inthash.htm
proc hash(i: int(64)): int(64) {
  var key = i;
  key += ~(key << 32);
  key = key ^ (key >> 22);
  key += ~(key << 13);
  key = key ^ (key >> 8);
  key += (key << 3);
  key = key ^ (key >> 15);
  key += ~(key << 27);
  key = key ^ (key >> 31);
  return (key & max(int(64))): int(64);  // YAH, make non-negative
}

inline proc hash(x : c_string): int(64) {
  var hash: int(64) = 0;
  for c in 1..(x.length) {
    hash = ((hash << 5) + hash) ^ ascii(x.substring(c));
  }
  return _gen_key(hash);
}

// for j in 0..1000 {
//   writeln(hash(j));
// }

writeln(hash(1));
writeln(hash("hello".c_str()));

