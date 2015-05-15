use Common, Logging, IO, Partitions;

config const buffersize = 1024;
config const dir_prefix = "/ssd/words";

class PartitionIndexer {
  var partition: int;
  var buff$: [0..buffersize-1] sync IndexRequest;
  var bufferIndex: atomic int;
  var release$: single bool;

  proc PartitionIndexer() {
    partition = 0;
    // via nextBufferIndex, this is incremented to zero before first use
    bufferIndex.write(-1);
  }

  proc PartitionIndexer(idx: int) {
    partition = idx;
    // via nextBufferIndex, this is incremented to zero before first use
    bufferIndex.write(-1);
  }

  proc startConsumer() {
    begin {
      consumer();
    }
  }

  proc nextBufferIndex(): int {
    var idx: int;
    var success = false;
    while (!success) { 
      var originalValue = bufferIndex.read();
      idx = (originalValue + 1) % buffersize;
      success = bufferIndex.compareExchange(originalValue, idx);
    }
    return idx;
  }

  proc enqueueIndexRequest(indexRequest: IndexRequest) {
    const idx = nextBufferIndex();
    buff$(idx).writeEF(indexRequest);
    debug("enqueuing ", indexRequest);
  }

  proc waitForIndexer() {
    debug("waiting...");
    release$;
    debug("done waiting...");
  }

  proc markCompleteForIndexer() {
    debug("marking for completion");
    const idx = nextBufferIndex();
    buff$(idx).writeEF(nil);
    debug("halting consumer");
  }

  proc consumer() {
    var indexFile = open(dir_prefix + partition + ".txt", iomode.cwr);
    var writer = indexFile.writer();
    for indexRequest in readFromBuff() {
      writer.writeln(indexRequest.word, "\t", indexRequest.docId);
    }
  }

  iter readFromBuff() {
    var ind = 0;
    var nextVal = buff$(ind);

    while (nextVal != nil) {
      yield nextVal;

      ind = (ind + 1) % buffersize;
      nextVal = buff$(ind);
    }

    release$ = true;
  }
}

var indexers: [0..Partitions.size-1] PartitionIndexer;

proc initIndexer() {
  var t: Timer;
  t.start();
  for i in 0..Partitions.size-1 {
    on Partitions[i] {
      indexers[i] = new PartitionIndexer(i);
      indexers[i].startConsumer();
    }
  }
  t.stop();
  timing("initialized indexer in ",t.elapsed(TimeUnits.microseconds), " microseconds");
}

proc indexerForWord(word: string): PartitionIndexer {
  return indexers[partitionForWord(word)];
}

proc enqueueIndexRequest(indexRequest: IndexRequest) {
  debug("enqueuing ", indexRequest);
  var indexer = indexerForWord(indexRequest.word);
  // TODO: do we need to go onto the indexer locale for this?  or will it just automatically be on that locale?
  on indexer {
    indexer.enqueueIndexRequest(indexRequest);
  }
}

proc waitForIndexer() {
  markCompleteForIndexer();
  debug("waiting...");
  for indexer in indexers {
    // TODO: do we need to do this on the locale? or can we just call waitForIndexer and have it work?
    on indexer {
      indexer.waitForIndexer();
    }
  }
  debug("done waiting...");
}

proc markCompleteForIndexer() {
  debug("marking for completion");
  for indexer in indexers {
    // TODO: do we need to do this on the locale? or can we just call waitForIndexer and have it work?
    on indexer {
      indexer.markCompleteForIndexer();
    }
  }
  debug("halting consumer");
}

proc main() {
  initPartitions();
  initIndexer();

  var t: Timer;
  t.start();

  var infile = open("words.txt", iomode.r);
  var reader = infile.reader();
  var word: string;
  var docId: DocId = 0;
  while (reader.readln(word)) {
    enqueueIndexRequest(new IndexRequest(word, docId));
    docId = (docId + 1) % 1000 + 1; // fake doc ids
  }

  t.stop();
  timing("partition fanout complete in ",t.elapsed(TimeUnits.microseconds), " microseconds");

  waitForIndexer();
}
