
module Indexer {

  use Search;
  
  class IndexRequest {
    var word: string;
    var docId: DocId;
  }

  config const buffersize = 2;
  config const verbose: bool = false;
  config const testAfterIndex: bool = true;

  var buff$: [0..buffersize-1] sync IndexRequest;
  var bufferIndex: atomic int;
  var release$: single bool;

  proc initIndexer() {
    bufferIndex.write(0);

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