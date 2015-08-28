package org.basex.query.up;

import org.basex.query.QueryContext;
import org.basex.query.QueryException;
import org.basex.query.up.primitives.DataUpdate;
import org.basex.query.up.primitives.Update;
import org.basex.query.up.primitives.name.NameUpdate;
import org.basex.util.Util;

/**
 * The database modifier holds all database updates during a snapshot.
 * Database permissions are checked to ensure that a user has enough privileges.
 *
 * @author BaseX Team 2005-15, BSD License
 * @author Lukas Kircher
 */
final class DatabaseModifier extends ContextModifier {
  @Override
  void add(final Update update, final QueryContext qc) throws QueryException {
    // check permissions
    if(update instanceof NameUpdate) {
//      if(!qc.context.perm(Perm.CREATE, ((NameUpdate) update).name()))
//        throw BASX_PERM_X.get(update.info(), Perm.CREATE);
    } else if(update instanceof DataUpdate) {
//      if(!qc.context.perm(Perm.WRITE, ((DataUpdate) update).data().meta.name))
//        throw BASX_PERM_X.get(update.info(), Perm.WRITE);
//    } else if(update instanceof UserUpdate) {
//      if(!qc.context.perm(Perm.ADMIN, (String) null))
//        throw BASX_PERM_X.get(update.info(), Perm.ADMIN);
    } else {
      throw Util.notExpected("Unknown update type: " + update);
    }
    super.add(update, qc);
  }
}
