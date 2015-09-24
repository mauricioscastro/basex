package org.basex.io.random;

import org.basex.io.*;

/**
 * Simple buffer for disk blocks.
 *
 * @author BaseX Team 2005-15, BSD License
 * @author Christian Gruen
 */
public final class Buffer {
  /** Buffer data. */
  public byte[] data = new byte[IO.BLOCKSIZE];
  /** Disk offset, or block position. */
  public long pos = -1;
  /** Dirty flag. */
  public boolean dirty;
}
