module Indexer {

  use Config, Partitions, Search;
  
  class IndexRequest {
    var word: string;
    var docId: DocId;
  }

  config const buffersize = 1024;
  config const testAfterIndex: bool = true;

  class PartitionIndexer {
    var buff$: [0..buffersize-1] sync IndexRequest;
    var bufferIndex: atomic int;
    var release$: single bool;

    proc PartitionIndexer() {
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

    proc enqueueIndexRequest(word: string, docId: DocId) {
      var indexRequest = new IndexRequest(word, docId);
      const idx = nextBufferIndex();
      buff$(idx).writeEF(indexRequest);
      if (verbose) then writeln("enqueuing ", indexRequest);
    }

    proc waitForIndexer() {
      writeln("waiting...");
      release$;
      writeln("done waiting...");
    }

    proc markCompleteForIndexer() {
      writeln("marking for completion");
      const idx = nextBufferIndex();
      buff$(idx).writeEF(nil);
      if (verbose) then writeln("halting consumer");
    }

    proc consumer() {
      for indexRequest in readFromBuff() {
        //if (verbose) then writeln("Consumer got: ", indexRequest);
        write("Indexing: ", indexRequest, "...");
        indexWord(indexRequest.word, indexRequest.docId);
        if (testAfterIndex) {
          var entry = entryForWord(indexRequest.word);
          if (entry == nil || entry.word != indexRequest.word) {
            writeln("indexer: failed to index word ", indexRequest.word);
            exit(0);
          }
        }
        writeln();
        delete indexRequest;
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
    for i in 0..Partitions.size-1 {
      on Partitions[i] {
        indexers[i] = new PartitionIndexer();
        indexers[i].startConsumer();
      }
    }
  }

  proc indexerForWord(word: string): PartitionIndexer {
    return indexers[partitionForWord(word)];
  }

  proc enqueueIndexRequest(word: string, docId: DocId) {
    var indexRequest = new IndexRequest(word, docId);
    if (verbose) then writeln("enqueuing ", indexRequest);
    var indexer = indexerForWord(word);
    // TODO: do we need to go onto the indexer locale for this?  or will it just automatically be on that locale?
    on indexer {
      indexer.enqueueIndexRequest(word, docId);
    }
  }

  proc waitForIndexer() {
    markCompleteForIndexer();
    writeln("waiting...");
    for indexer in indexers {
      // TODO: do we need to do this on the locale? or can we just call waitForIndexer and have it work?
      on indexer {
        indexer.waitForIndexer();
      }
    }
    writeln("done waiting...");
  }

  proc markCompleteForIndexer() {
    writeln("marking for completion");
    for indexer in indexers {
      // TODO: do we need to do this on the locale? or can we just call waitForIndexer and have it work?
      on indexer {
        indexer.markCompleteForIndexer();
      }
    }
    if (verbose) then writeln("halting consumer");
  }
}