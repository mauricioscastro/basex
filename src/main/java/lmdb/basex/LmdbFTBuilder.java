package lmdb.basex;

import org.basex.core.MainOptions;
import org.basex.data.Data;
import org.basex.index.ft.FTBuilder;
import org.basex.index.ft.FTIndex;

import java.io.IOException;

import static lmdb.basex.LmdbDataManager.ftindexxdb;
import static lmdb.basex.LmdbDataManager.ftindexydb;
import static lmdb.basex.LmdbDataManager.ftindexzdb;
import static lmdb.basex.LmdbValuesBuilder.copyIndex;
import static org.basex.data.DataText.DATAFTX;

public class LmdbFTBuilder extends FTBuilder {

    private byte[] docid;

    public LmdbFTBuilder(final byte[] docid, final Data data, final MainOptions options) throws IOException {
        super(data, options);
        this.docid = docid;
    }

    @Override
    public FTIndex build() throws IOException {
        _build();
        copyIndex(data.meta.dbfile(DATAFTX + 'x').file(), ftindexxdb, docid);
        copyIndex(data.meta.dbfile(DATAFTX + 'y').file(), ftindexydb, docid);
        copyIndex(data.meta.dbfile(DATAFTX + 'z').file(), ftindexzdb,docid);
        data.meta.dbfile("swl").file().delete();
        return null;
    }
}
