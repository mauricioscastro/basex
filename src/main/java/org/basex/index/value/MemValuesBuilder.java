package org.basex.index.value;

import org.basex.core.MainOptions;
import org.basex.data.Data;
import org.basex.index.ValuesBuilder;
import org.basex.util.Util;

import java.io.IOException;

/**
 * <p>This class builds a main-memory index for attribute values and text contents.</p>
 *
 * @author BaseX Team 2005-15, BSD License
 * @author Christian Gruen
 */
public class MemValuesBuilder extends ValuesBuilder {
  /**
   * Constructor.
   * @param data data reference
   * @param options main options
   * @param text value type (text/attribute)
   */
  public MemValuesBuilder(final Data data, final MainOptions options, final boolean text) {
    super(data, options, text);
  }

  @Override
  public MemValues build() throws IOException {
    Util.debug(det());

    final MemValues index = new MemValues(data, text);
    for(pre = 0; pre < size; pre++) {
      if((pre & 0x0FFF) == 0) check();
      if(indexEntry() && data.textLen(pre, text) <= data.meta.maxlen) {
        index.add(data.text(pre, text), data.meta.updindex ? data.id(pre) : pre);
        count++;
      }
    }
    index.finish();
    finishIndex();
    return index;
  }

//  @Override
  protected void abort() {
  }
}
