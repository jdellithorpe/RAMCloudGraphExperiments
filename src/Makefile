all: bfs dbfs

bfs: bfs.cc
	g++ -std=c++0x -L../../ramcloud/obj.master -I/home/jdellit/ramcloud/src -I/home/jdellit/ramcloud/obj.master -I/home/jdellit/ramcloud/gtest/include -I/home/jdellit/ramcloud/logcabin -c -o bfs.o bfs.cc
	g++ -o bfs bfs.o ../../ramcloud/obj.master/libramcloud.a -L../../ramcloud/obj.master -L/home/jdellit/local/lib -lpcrecpp -lboost_program_options -lprotobuf -lrt -lboost_filesystem -lboost_system -lpthread -lssl -lcrypto -rdynamic -libverbs -lprofiler

dbfs: dbfs_slave.cc
	g++ -std=c++0x -L../../ramcloud/obj.master -I/home/jdellit/ramcloud/src -I/home/jdellit/ramcloud/obj.master -I/home/jdellit/ramcloud/gtest/include -I/home/jdellit/ramcloud/logcabin -c -o dbfs_slave.o dbfs_slave.cc
	g++ -o dbfs_slave dbfs_slave.o ../../ramcloud/obj.master/libramcloud.a -L../../ramcloud/obj.master -L/home/jdellit/local/lib -lpcrecpp -lboost_program_options -lprotobuf -lrt -lboost_filesystem -lboost_system -lpthread -lssl -lcrypto -rdynamic -libverbs

scratchwork: scratchwork.cc
	g++ -std=c++0x -L../../ramcloud/obj.master -I/home/jdellit/ramcloud/src -I/home/jdellit/ramcloud/obj.master -I/home/jdellit/ramcloud/gtest/include -I/home/jdellit/ramcloud/logcabin -g -c -o scratchwork.o scratchwork.cc
	g++ -o scratchwork scratchwork.o ../../ramcloud/obj.master/libramcloud.a -L../../ramcloud/obj.master -L/home/jdellit/local/lib -lpcrecpp -lboost_program_options -lprotobuf -lrt -lboost_filesystem -lboost_system -lpthread -lssl -lcrypto -rdynamic -libverbs -lprofiler
	./scratchwork

clean:
	rm bfs scratchwork *.o *.out
