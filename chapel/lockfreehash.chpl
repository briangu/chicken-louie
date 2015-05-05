module LockFreeHash {
  
  use GenHashKey32;

  config const debug = false;

  // Lock Free Hash depends on a uint(32) key type.  To change this will requiring switching out the genHashKey algos (supply a class?)
  type KeyType = uint(32);
  type ValueType = uint(32);

  record TableEntry {
    var key: atomic KeyType;
    var value: atomic ValueType;

    proc TableEntry(outer: LockFreeHash) {
      key.write(0);
    }
  }

  class LockFreeHash {
    // type ValueType = uint(32);
    var hashSize: uint(32) = 1024*1024; // must be power of 2

    // record TableEntry {
    //   var key: atomic KeyType;
    //   var value: atomic ValueType;

    //   proc TableEntry(outer: LockFreeHash) {
    //     key.write(0);
    //   }
    // }

    var array: [0..hashSize-1] TableEntry;

    proc setItem(key: KeyType, value: ValueType): bool {
      var idx: uint(32) = genHashKey32(key);
      var count = 0;
      
      if (debug) then writeln("key: ", key, " value: ", value);
      if (debug) then writeln("count: ", count);

      while (count < array.size) {
        idx &= hashSize - 1;

        if (debug) then writeln("idx: ", idx);

        var probedKey = array[idx].key.read();
        if (debug) then writeln("probedKey: ", probedKey);
        if (probedKey != key) {
          // The entry was either free, or contains another key.
          if (probedKey != 0) {
            idx += 1;
            count += 1;
            continue; // Usually, it contains another key. Keep probing.
          }

          // The entry was free. Now let's try to take it using a CAS.
          var stored = array[idx].key.compareExchange(0, key);
          if (debug) then writeln("stored: ", stored);
          if (!stored) {
            idx += 1;
            count += 1;
            continue;       // Another thread just stole it from underneath us.
          }

          // Either we just added the key, or another thread did.
        }

        // Store the value in this array entry.
        array[idx].value.write(value);
        return true;
      }

      if (count == array.size) {
        // out of capacity
        if (debug) then writeln("hash out of capacity");
      }

      return false;
    }

    proc getItem(key: KeyType): ValueType {
      var count = 0;

      if (debug) then writeln("key: ", key);
      if (debug) then writeln("count: ", count);

      var idx: uint(32) = genHashKey32(key);
      while (count < array.size) {
        idx &= hashSize - 1;

        var probedKey = array[idx].key.read();
        if (debug) then writeln("probedKey: ", probedKey);
        if (probedKey == key) {
          return array[idx].value.read();
        }
        if (probedKey == 0) {
          return 0;
        }

        count += 1;
        
        if (debug) then writeln("count: ", count);
      }

      if (debug) then writeln("exhuastive search and key not found");

      return 0;
    }
  }
}