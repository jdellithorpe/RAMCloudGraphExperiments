TARGETS = bfs mt_bfs reader

all: $(TARGETS)

bfs: bfs.cc
	g++ -std=c++0x -L../../ramcloud/obj.master -I/home/jdellit/ramcloud/src -I/home/jdellit/ramcloud/obj.master -I/home/jdellit/ramcloud/gtest/include -I/home/jdellit/ramcloud/logcabin -c -o bfs.o bfs.cc
	g++ -o bfs bfs.o ../../ramcloud/obj.master/libramcloud.a -L../../ramcloud/obj.master -L/home/jdellit/local/lib -lpcrecpp -lboost_program_options -lprotobuf -lrt -lboost_filesystem -lboost_system -lpthread -lssl -lcrypto -rdynamic -libverbs -lprofiler

adjacency_list.pb.o: adjacency_list.proto	
	protoc -I=. --cpp_out=. ./adjacency_list.proto
	g++ -std=c++0x -c -o adjacency_list.pb.o adjacency_list.pb.cc 

mt_bfs: mt_bfs.cc sandy.h adjacency_list.pb.o
	g++ -std=c++0x -L../../ramcloud/obj.master -I/home/jdellit/ramcloud/src -I/home/jdellit/ramcloud/obj.master -I/home/jdellit/ramcloud/gtest/include -I/home/jdellit/ramcloud/logcabin -c -o mt_bfs.o mt_bfs.cc
	g++ -o mt_bfs mt_bfs.o adjacency_list.pb.o ../../ramcloud/obj.master/libramcloud.a -L../../ramcloud/obj.master -L/home/jdellit/local/lib -lpcrecpp -lboost_program_options -lprotobuf -lrt -lboost_filesystem -lboost_system -lboost_thread -lpthread -lssl -lcrypto -rdynamic -libverbs -lprofiler

reader: reader.cc sandy.h adjacency_list.pb.o
	g++ -std=c++0x -L../../ramcloud/obj.master -I/home/jdellit/ramcloud/src -I/home/jdellit/ramcloud/obj.master -I/home/jdellit/ramcloud/gtest/include -I/home/jdellit/ramcloud/logcabin -c -o reader.o reader.cc 
	g++ -o reader reader.o adjacency_list.pb.o ../../ramcloud/obj.master/libramcloud.a -L../../ramcloud/obj.master -L/home/jdellit/local/lib -lpcrecpp -lboost_program_options -lprotobuf -lrt -lboost_filesystem -lboost_system -lboost_thread -lpthread -lssl -lcrypto -rdynamic -libverbs -lprofiler

rd: rd.cc sandy.h adjacency_list.pb.o
	g++ -std=c++0x -L../../ramcloud/obj.master -I/home/jdellit/ramcloud/src -I/home/jdellit/ramcloud/obj.master -I/home/jdellit/ramcloud/gtest/include -I/home/jdellit/ramcloud/logcabin -c -o rd.o rd.cc 
	g++ -o rd rd.o adjacency_list.pb.o ../../ramcloud/obj.master/libramcloud.a -L../../ramcloud/obj.master -L/home/jdellit/local/lib -lpcrecpp -lboost_program_options -lprotobuf -lrt -lboost_filesystem -lboost_system -lboost_thread -lpthread -lssl -lcrypto -rdynamic -libverbs -lprofiler

clean:
	rm -f bfs mt_bfs reader rd adjacency_list.pb.* *.o *.out
