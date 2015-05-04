module LockFreeHash {
  
  use GenHashKey32;

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

/*
void HashTable1::SetItem(uint32_t key, uint32_t value)
{
    for (uint32_t idx = integerHash(key);; idx++)
    {
        idx &= m_arraySize - 1;

        // Load the key that was there.
        uint32_t probedKey = mint_load_32_relaxed(&m_entries[idx].key);
        if (probedKey != key)
        {
            // The entry was either free, or contains another key.
            if (probedKey != 0)
                continue;           // Usually, it contains another key. Keep probing.
                
            // The entry was free. Now let's try to take it using a CAS.
            uint32_t prevKey = mint_compare_exchange_strong_32_relaxed(&m_entries[idx].key, 0, key);
            if ((prevKey != 0) && (prevKey != key))
                continue;       // Another thread just stole it from underneath us.

            // Either we just added the key, or another thread did.
        }
        
        // Store the value in this array entry.
        mint_store_32_relaxed(&m_entries[idx].value, value);
        return;
    }
}*/
    proc setItem(key: KeyType, value: ValueType) {
      var idx: uint(32) = genHashKey32(key);
      var count = 0;
      
      writeln("key: ", key, " value: ", value);
      writeln("count: ", count);

      while (count < array.size) {
        idx &= hashSize - 1;

        writeln("idx: ", idx);

        var probedKey = array[idx].key.read();
        writeln("probedKey: ", probedKey);
        if (probedKey != key) {
          // The entry was either free, or contains another key.
          if (probedKey != 0) {
            idx += 1;
            count += 1;
            continue; // Usually, it contains another key. Keep probing.
          }

          // The entry was free. Now let's try to take it using a CAS.
          var stored = array[idx].key.compareExchange(0, key);
          writeln("stored: ", stored);
          if (!stored) {
            idx += 1;
            count += 1;
            continue;       // Another thread just stole it from underneath us.
          }

          // Either we just added the key, or another thread did.
        }

        // Store the value in this array entry.
        array[idx].value.write(value);
        return;
      }

      if (count == array.size) {
        // out of capacity
        writeln("hash out of capacity");
      }
    }

/*
uint32_t HashTable1::GetItem(uint32_t key)
{
    for (uint32_t idx = integerHash(key);; idx++)
    {
        idx &= m_arraySize - 1;

        uint32_t probedKey = mint_load_32_relaxed(&m_entries[idx].key);
        if (probedKey == key)
            return mint_load_32_relaxed(&m_entries[idx].value);
        if (probedKey == 0)
            return 0;          
    }
}*/
    proc getItem(key: KeyType): ValueType {
      var count = 0; // TODO: REMOVE

      writeln("key: ", key);
      writeln("count: ", count);

      var idx: uint(32) = genHashKey32(key);
      while (count < array.size) {
        idx &= hashSize - 1;

        var probedKey = array[idx].key.read();
        writeln("probedKey: ", probedKey);
        if (probedKey == key) {
          return array[idx].value.read();
        }
        if (probedKey == 0) {
          return 0;
        }

        count += 1;
        writeln("count: ", count);
      }

      // TODO: REMOVE
      if (count == array.size) {
        writeln("exhuastive search and key not found");
      }

      return 0;
    }
  }
}