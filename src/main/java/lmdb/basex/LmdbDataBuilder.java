package lmdb.basex;

import org.basex.core.MainOptions;
import org.basex.data.Namespaces;
import org.basex.index.name.Names;
import org.basex.index.path.PathSummary;
import org.fusesource.lmdbjni.Database;
import org.fusesource.lmdbjni.Transaction;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

import static lmdb.basex.LmdbDataManager.createWriteTransaction;
import static lmdb.util.Byte.lmdbkey;

public class LmdbDataBuilder extends LmdbData {


    private DataOutputStream tempBuffer;
    private File tmpFile;

    public LmdbDataBuilder(final String name, final byte[] docid, final Database elemNames,
                    final Database attrNames, final Database paths, final Database nspaces,
                    final Database tableAccess, final MainOptions options) throws IOException {

        super(name, options);

        table = new TableLmdbAccessBuilder(meta,tableAccess,docid);
        elementdb = elemNames;
        attributedb = attrNames;
        pathsdb = paths;
        namespacedb = nspaces;

        tmpFile = File.createTempFile("txt.",".tmp",null);
        tmpFile.deleteOnExit();
        tempBuffer = new DataOutputStream(new FileOutputStream(tmpFile));

        this.elemNames = new Names(meta);
        this.attrNames = new Names(meta);
        this.paths = new PathSummary(this);
        this.nspaces = new Namespaces();
    }

    @Override
    public void close() {
        try {
            table.close();
            tempBuffer.close();

            Transaction t = createWriteTransaction();

            DataInputStream di = new DataInputStream(new FileInputStream(tmpFile));

            int c = 0;
            while(true) try {
                int len = di.readInt();
                byte[] key = new byte[8];
                di.readFully(key);
                byte[] value = new byte[len];
                di.readFully(value);
                boolean text = di.readBoolean();
                (text ? txtdb : attdb).put(t, key, value);
                c++;
                if (c > 1024 * 10) {
                    t.commit();
                    c = 0;
                    t = createWriteTransaction();
                }
            } catch(EOFException eofe) {
                break;
            }
            if (c > 0) t.commit();
            else t.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        tmpFile.delete();
    }

    @Override
    protected void put(int pre, byte[] value, boolean text) {
        try {
            tempBuffer.writeInt(value.length);
            tempBuffer.write(lmdbkey(docid, pre));
            tempBuffer.write(value);
            tempBuffer.writeBoolean(text);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
