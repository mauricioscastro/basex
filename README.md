[BaseX](http://basex.org/) over [LMDB](http://symas.com/mdb/) 
=============================================================

A long time wish to make [BaseX](http://basex.org/) on disk base more robust.

BaseX Core was stripped to its bare essentials (not really, more can be removed to make it even skinnier).
 
A LmdbData, TableLmdbAccess and related builder an indexes were created to work on top of [LMDB](http://symas.com/mdb/) with [lmdbjni](https://github.com/deephacks/lmdbjni).

The idea is to strengthen the BaseX store structure and later replicate it (with [BookKeeper](http://bookkeeper.apache.org/)?, [jgropus-raft](https://github.com/belaban/jgroups-raft/blob/master/doc/manual/overview.adoc)?) for further high availability.

A replication project idea is now in progress [here](https://github.com/mauricioscastro/lldb).


##build
mvn clean install

##run
java -jar basex-lmdb.jar

from project basedir

##simple usage
In a browser or with curl, issue a HTTP GET request to http://localhost:8080/doc('file://etc/books.xml')

Any xquery will work after http://localhost:8080/

###XQuery details
The query string part of the URL will be interpreted as external variables to the XQuery context except 
for the following two:
 
**content-type:** what should be the resulting contents output type? default is "text/xml"

**indent-content:** if resulting content should be indented. default is "no". use "yes/no", "true/false".   


&nbsp;

###### All items below assumes you are in the project basedir:

>##### create a collection named etc:
>```curl -X PUT 'http://localhost:8080/etc'```

&nbsp;

>##### create a document named factbook inside etc collection:
>```curl --upload-file ./db/xml/etc/factbook.xml 'http://localhost:8080/etc/factbook'```

&nbsp;

>##### create a document named lakes inside etc collection as the result of an xquery:
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

###bigger things
If you want bigger examples, try db/xml/shakespeare.zip and db/xml/religion.zip from the base directory:

>```
cd db/xml
unzip shakespeare.zip
curl -X PUT 'http://localhost:8080/shakespeare'
cd shakespeare
ls | while read F; do N=`echo $F | cut -d '.' -f 1`; curl --upload-file $F "http://localhost:8080/shakespeare/$N" & done
unzip religion.zip
curl -X PUT 'http://localhost:8080/religion'
cd ../religion
ls | while read F; do N=`echo $F | cut -d '.' -f 1`; curl --upload-file $F "http://localhost:8080/religion/$N" & done
```

###even bigger things
I think this is not yet the hardest for basex-lmdb but it is a feasible real world example at hand. 
download National Library of Medicine (ftp://ftp.nlm.nih.gov/nlmdata/sample/medline/) data and try it like the shakespeare example above.
the biggest file there is over 150MB and has over 4.5 million XML nodes.

there's also [XMark's Benchmark Data Generator](http://www.xml-benchmark.org/generator.html) if you want to get serious.


##extra documentation
As stated by the title this is nothing less than [BaseX](http://basex.org/) itself, so any [BaseX documentation](http://docs.basex.org/) 
regarding XQuery and modules (with some exceptions yet to be listed) can be used as is. 

##todo
- considering [kafka](http://kafka.apache.org/) for replication. can I embed it?
- optimize xquery updates by writing to a LSM based solution before writing to LMDB thus freeing the sync client faster. 
  considering the idea is to (maybe) replicate by using [jgropus-raft](https://github.com/belaban/jgroups-raft/blob/master/doc/manual/overview.adoc)
  and once it uses LevelDB internally, would simply writing to the cluster do the trick?      
- create OS based maven profile for dealing with [lmdbjni](https://github.com/deephacks/lmdbjni) dependencies  
- need to port tests and improve LmdbDataManager tests
- improve the return error codes in REST XQueryHandler 
- migrate XQueryHandler to a servlet and create a maven WAR packaged project 
- needs more documentation about configuration and running standalone or servlet
- document the URI's used in fn:doc(): bxl://, file://, jdbc:// and related configurations where it fits
- create new URI's accessed through fn:doc(): http:// with [HtmlUnit](http://htmlunit.sourceforge.net/) and extras with [commons VFS](https://commons.apache.org/proper/commons-vfs/filesystems.html) 
- replicate with [jgropus-raft](https://github.com/belaban/jgroups-raft/blob/master/doc/manual/overview.adoc). Ideas? 
- assuming above replication is using raft and we have a good cluster, what about distributing XQuery queries amongst the cluster members for load balancing?
- create a [Camel](http://camel.apache.org/) component for basex-lmdb and use it as a solid integration database (in the end canonical messages passing by are all xml anyway... right?).




