package lmdb.basex;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.basex.io.out.DataOutput;
import org.basex.io.out.TableOutput;

import java.io.FileOutputStream;
import java.io.IOException;

import static lmdb.basex.LmdbDataManager.tableaccessdb;

public class LmdbTableOutput extends TableOutput {

    private byte[] docid;

    public LmdbTableOutput(LmdbMetaData md, String fn, byte[] did) throws IOException {
        super(md);
        os = new FileOutputStream(fn);
        file = fn;
        docid = did;
    }

    @Override
    public void close() throws IOException {
        // store at least one page on disk
        final boolean empty = pages == 0 && pos == 0;
        if(empty) pos++;
        flush();
        os.close();

        try(ByteArrayOutputStream bos = new ByteArrayOutputStream(); final DataOutput out = new DataOutput(bos)) {
            out.writeNum(pages);
            out.writeNum(empty ? 0 : Integer.MAX_VALUE);
            out.flush();
            tableaccessdb.put(TableLmdbAccess.getStructKey(docid), bos.toByteArray());
        }
    }
}
