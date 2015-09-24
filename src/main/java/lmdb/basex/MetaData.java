package lmdb.basex;

import org.basex.core.MainOptions;
import org.basex.core.StaticOptions;
import org.basex.io.in.DataInput;
import org.basex.util.Token;
import org.basex.util.ft.Language;

import java.io.IOException;

import static org.basex.data.DataText.DBATVIDX;
import static org.basex.data.DataText.DBATVINC;
import static org.basex.data.DataText.DBAUTOOPT;
import static org.basex.data.DataText.DBCHOP;
import static org.basex.data.DataText.DBCRTATV;
import static org.basex.data.DataText.DBCRTFTX;
import static org.basex.data.DataText.DBCRTTXT;
import static org.basex.data.DataText.DBENC;
import static org.basex.data.DataText.DBFNAME;
import static org.basex.data.DataText.DBFSIZE;
import static org.basex.data.DataText.DBFTCS;
import static org.basex.data.DataText.DBFTDC;
import static org.basex.data.DataText.DBFTLN;
import static org.basex.data.DataText.DBFTST;
import static org.basex.data.DataText.DBFTSW;
import static org.basex.data.DataText.DBFTXIDX;
import static org.basex.data.DataText.DBFTXINC;
import static org.basex.data.DataText.DBLASTID;
import static org.basex.data.DataText.DBMAXCATS;
import static org.basex.data.DataText.DBMAXLEN;
import static org.basex.data.DataText.DBNDOCS;
import static org.basex.data.DataText.DBPERM;
import static org.basex.data.DataText.DBPTHIDX;
import static org.basex.data.DataText.DBSIZE;
import static org.basex.data.DataText.DBTIME;
import static org.basex.data.DataText.DBTXTIDX;
import static org.basex.data.DataText.DBTXTINC;
import static org.basex.data.DataText.DBUPDIDX;
import static org.basex.data.DataText.DBUPTODATE;
import static org.basex.util.Strings.toInt;
import static org.basex.util.Strings.toLong;

public class MetaData extends org.basex.data.MetaData {

    MetaData(final String name, final MainOptions options, final StaticOptions sopts) {
        super(name, options, sopts);
    }

    public void read(final DataInput in) throws IOException {
        String storage = "", istorage = "";
        while(true) {
            final String k = Token.string(in.readToken());
            if(k.isEmpty()) break;
            if(k.equals(DBPERM)) {
                // legacy (Version < 8)
                for(int u = in.readNum(); u > 0; --u) { in.readToken(); in.readToken(); in.readNum(); }
            } else {
                final String v = Token.string(in.readToken());
                if(k.equals(DBFNAME))         original    = v;
                else if(k.equals(DBENC))      encoding    = v;
                else if(k.equals(DBFTSW))     stopwords   = v;
                else if(k.equals(DBFTLN))     language    = Language.get(v);
                else if(k.equals(DBSIZE))     size        = toInt(v);
                else if(k.equals(DBNDOCS))    ndocs       = toInt(v);
//                else if(k.equals(DBSCTYPE))   scoring     = toInt(v);
                else if(k.equals(DBMAXLEN))   maxlen      = toInt(v);
                else if(k.equals(DBMAXCATS))  maxcats     = toInt(v);
                else if(k.equals(DBLASTID))   lastid      = toInt(v);
                else if(k.equals(DBTIME))     time        = toLong(v);
                else if(k.equals(DBFSIZE))    filesize    = toLong(v);
                else if(k.equals(DBFTDC))     diacritics  = toBool(v);
                else if(k.equals(DBCHOP))     chop        = toBool(v);
                else if(k.equals(DBUPDIDX))   updindex    = toBool(v);
                else if(k.equals(DBAUTOOPT))  autoopt     = toBool(v);
                else if(k.equals(DBTXTIDX))   textindex   = toBool(v);
                else if(k.equals(DBATVIDX))   attrindex   = toBool(v);
                else if(k.equals(DBFTXIDX))   ftindex     = toBool(v);
                else if(k.equals(DBTXTINC))   textinclude = v;
                else if(k.equals(DBATVINC))   attrinclude = v;
                else if(k.equals(DBFTXINC))   ftinclude   = v;
                else if(k.equals(DBCRTTXT))   createtext  = toBool(v);
                else if(k.equals(DBCRTATV))   createattr  = toBool(v);
                else if(k.equals(DBCRTFTX))   createftxt  = toBool(v);
//                else if(k.equals(DBWCIDX))    wcindex     = toBool(v);
                else if(k.equals(DBFTST))     stemming    = toBool(v);
                else if(k.equals(DBFTCS))     casesens    = toBool(v);
                else if(k.equals(DBUPTODATE)) uptodate    = toBool(v);
                    // legacy: set up-to-date flag to false if path index does not exist
                else if(k.equals(DBPTHIDX) && !toBool(v)) uptodate = false;
            }
        }
    }
}
