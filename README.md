[BaseX](http://basex.org/) over [LMDB](http://symas.com/mdb/)
=============================================================

A long time wish PoC to make [BaseX](http://basex.org/) on disk base more robust.

BaseX Core was stripped to its bare essentials.
 
A LmdbData, TableLmdbAccess and related builder an indexes were created to work on top of [LMDB](http://symas.com/mdb/) with [lmdbjni](https://github.com/deephacks/lmdbjni).

The idea is to strengthen the BaseX store structure and later replicate it (with [BookKeeper](http://bookkeeper.apache.org/)?, [jgropus-raft](https://github.com/belaban/jgroups-raft/blob/master/doc/manual/overview.adoc)?) for further high availability.


##build
mvn clean install

##run
java -jar basex-lmdb.jar

from project basedir

##simple usage
In a browser or with curl, issue a HTTP GET request to http://localhost:8080/doc('file:///books.xml')

Any xquery will work after http://localhost:8080/

&nbsp;

###### All items below assumes you are in the project basedir:

>##### create a collection named etc:
>```curl -X PUT 'http://localhost:8080/etc'```

&nbsp;

>##### create a document named factbook inside etc collection:
>```curl --upload-file ./db/xml/etc/factbook.xml 'http://localhost:8080/etc/factbook'```

&nbsp;

>##### create a document named lakes inside etc collection as the result of a xquery:
>```curl -X PUT 'http://localhost:8080/etc/lakes/<lakes>\{doc("etc/factbook")//lake\}</lakes>'```

&nbsp;

>##### remove the document named factbook from the collection etc:
>```curl -X DELETE 'http://localhost:8080/etc/factbook'```

&nbsp;

>##### remove the etc collection:
>```curl -X DELETE 'http://localhost:8080/etc'```

&nbsp;

>##### some updates to factbook document:
>```curl -d 'rename node doc("etc/factbook")//lake[1] as "LAKE"' -X POST http://localhost:8080```

>```curl -d 'replace value of node doc("etc/factbook")//LAKE/@name with "Casper Sea"' -X POST http://localhost:8080```

>```curl -d "insert node <lake name='Lago da Paz'/> into doc('etc/factbook')/mondial" -X POST http://localhost:8080```


###todo:
- need to port indexing to LMDB
- need to port tests and improve LmdbDataManager tests
- more tests
- improve the return error codes in REST XQueryHandler 
- needs more documentation on ~~how to run~~, ~~create collections~~, ~~documents~~, ~~updates~~, etc
- document the URI's used in fn:doc(): bxl://, file://, jdbc:// and related configurations where it fits
- create new URI's accessed through fn:doc(): http:// with [HtmlUnit](http://htmlunit.sourceforge.net/) and extras with [commons VFS](https://commons.apache.org/proper/commons-vfs/filesystems.html) 
- replicate either [BookKeeper](http://bookkeeper.apache.org/) or [jgropus-raft](https://github.com/belaban/jgroups-raft/blob/master/doc/manual/overview.adoc). Ideas? 
- assuming above replication is using raft and we have a good cluster, what about distributing XQuery queries amongst the cluster members for load balancing?
- create a [Camel](http://camel.apache.org/) component for basex-lmdb and use it as a solid integration database (in the end canonical messages passing by are all xml anyway... right?);


###thanks
to the basex team!



