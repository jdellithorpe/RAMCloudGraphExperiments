all: bfs

bfs: bfs.cc
	g++ -std=c++0x -L../../ramcloud/obj.master -I/home/jdellit/ramcloud/src -I/home/jdellit/ramcloud/obj.master -I/home/jdellit/ramcloud/gtest/include -I/home/jdellit/ramcloud/logcabin -c -o bfs.o bfs.cc
	g++ -o bfs bfs.o ../../ramcloud/obj.master/libramcloud.a -L../../ramcloud/obj.master -L/home/jdellit/local/lib -lpcrecpp -lboost_program_options -lprotobuf -lrt -lboost_filesystem -lboost_system -lpthread -lssl -lcrypto -rdynamic -libverbs -lprofiler

mt_bfs: mt_bfs.cc
	g++ -std=c++0x -L../../ramcloud/obj.master -I/home/jdellit/ramcloud/src -I/home/jdellit/ramcloud/obj.master -I/home/jdellit/ramcloud/gtest/include -I/home/jdellit/ramcloud/logcabin -c -o mt_bfs.o mt_bfs.cc
	g++ -o mt_bfs mt_bfs.o ../../ramcloud/obj.master/libramcloud.a -L../../ramcloud/obj.master -L/home/jdellit/local/lib -lpcrecpp -lboost_program_options -lprotobuf -lrt -lboost_filesystem -lboost_system -lboost_thread -lpthread -lssl -lcrypto -rdynamic -libverbs -lprofiler

scratchwork_01: scratchwork_01.cc
	g++ -std=c++0x -L../../ramcloud/obj.master -I/home/jdellit/ramcloud/src -I/home/jdellit/ramcloud/obj.master -I/home/jdellit/ramcloud/gtest/include -I/home/jdellit/ramcloud/logcabin -g -c -o scratchwork_01.o scratchwork_01.cc
	g++ -o scratchwork_01 scratchwork_01.o ../../ramcloud/obj.master/libramcloud.a -L../../ramcloud/obj.master -L/home/jdellit/local/lib -lpcrecpp -lboost_program_options -lprotobuf -lrt -lboost_filesystem -lboost_system -lpthread -lssl -lcrypto -rdynamic -libverbs -lprofiler
	./scratchwork_01

scratchwork_02: scratchwork_02.cc
	g++ -std=c++0x -L../../ramcloud/obj.master -I/home/jdellit/ramcloud/src -I/home/jdellit/ramcloud/obj.master -I/home/jdellit/ramcloud/gtest/include -I/home/jdellit/ramcloud/logcabin -g -c -o scratchwork_02.o scratchwork_02.cc
	g++ -o scratchwork_02 scratchwork_02.o ../../ramcloud/obj.master/libramcloud.a -L../../ramcloud/obj.master -L/home/jdellit/local/lib -lpcrecpp -lboost_program_options -lprotobuf -lrt -lboost_filesystem -lboost_system -lboost_thread -lpthread -lssl -lcrypto -rdynamic -libverbs -lprofiler
	./scratchwork_02

clean:
	rm -f mt_bfs bfs scratchwork_01 scratchwork_02 *.o *.out dbfs_slave
