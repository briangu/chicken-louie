use GenHashKey32, Logging, Memory, IO, ReplicatedDist, Time;

type WordType = uint(32);

class Node {
  var word: WordType;
  var next: Node;
}

class PartitionInfo {
  var head: Node;
  var count: atomic int;
}

// Separate the search parition strategy from locales.
// The reason that it's worth keeping partitions separate from locales is that
// it makes it easy to change locale counts without having to rebuild the partitions.
//
// Number of dimensions in the partition space.
// Each partition will be projected to a locale.  
// If the number of partitions exceeds the number of locales, 
// then the locales will be over-subscribed with possibly more than one
// partition per locale.
//
config const partitionDimensions = 16;

/*
const Dbase = {1..5};
const Drepl: domain(1) dmapped ReplicatedDist() = Dbase;
var Abase: [Dbase] int;
var Arepl: [Drepl] int;
*/

// Partition to locale mapping.  Zero-based to allow modulo to work conveniently.
const Space = {0..partitionDimensions-1};
const ReplicatedSpace = Space dmapped ReplicatedDist();
var Partitions: [ReplicatedSpace] PartitionInfo;

proc initPartitions() {
  var t: Timer;
  t.start();

  writeln("Partitions");
  writeln(Space);
  writeln(ReplicatedSpace);
  writeln(Partitions);
  writeln();

  // var tmpPartitions: [Space] PartitionInfo;
  // for i in tmpPartitions.domain {
  //   tmpPartitions[i] = new PartitionInfo();
  // }

  // // assign to the replicated Partitions array, causing a global replication of the array
  // Partitions = tmpPartitions;

  writeln("Partitions");
  writeln(Partitions.size);
  writeln(Partitions);
  writeln();

  t.stop();
  timing("initialized partitions in ",t.elapsed(TimeUnits.microseconds), " microseconds");
}

inline proc partitionIdForWord(word: WordType): int {
  return word % partitionDimensions;
} 

inline proc localeForWord(word: WordType): locale {
  return Locales[partitionIdForWord(word) % Locales.size];
}

// should be a local lookup
inline proc partitionInfoForWord(word: WordType): PartitionInfo {
  var info = Partitions[partitionIdForWord(word)];
  if (info == nil) {
    info = new PartitionInfo();
    Partitions[partitionIdForWord(word)] = info;
  }
  return info;
}

proc indexWord(word: WordType) {
  // first move the locale that should have the word.  There may be more than one active partition on a single locale.
  on localeForWord(word) {
    // locally operate on the partition info that the word maps to
    local {
      var info = partitionInfoForWord(word); // TODO: this should be already local w/o the local keyword
      var head = info.head;
      info.head = new Node(word, head);
      info.count.add(1);
    }
    // writeln(Partitions);
  }
}

proc main() {
  initPartitions();

  var t: Timer;
  t.start();

  var infile = open("words.txt", iomode.r);
  var reader = infile.reader();
  var word: string;

  // TODO: parallelize reads
  while (reader.readln(word)) {
    indexWord(genHashKey32(word));
  }

  writeln(Partitions);

  t.stop();
  timing("indexing complete in ",t.elapsed(TimeUnits.microseconds), " microseconds");
}

