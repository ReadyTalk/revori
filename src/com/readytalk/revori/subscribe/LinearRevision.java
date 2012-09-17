package com.readytalk.revori.subscribe;

import com.readytalk.revori.Revision;

class LinearRevision implements Comparable<LinearRevision> {
  final Revision revision;
  final long sequenceNumber;
  int referenceCount = 0;

  public LinearRevision(Revision revision, long sequenceNumber) {
    this.revision = revision;
    this.sequenceNumber = sequenceNumber;
  }

  public int compareTo(LinearRevision o) {
    return sequenceNumber > o.sequenceNumber
      ? 1 : (sequenceNumber < o.sequenceNumber ? -1 : 0);
  }
}