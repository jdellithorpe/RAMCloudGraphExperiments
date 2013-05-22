TARGETS = bfs mt_bfs reader rd rc bfgg

all: $(TARGETS)

bfs: bfs.cc
	g++ -std=c++0x -L/home/jdellit/ramcloud/obj.master -I/home/jdellit/ramcloud/src -I/home/jdellit/ramcloud/obj.master -I/home/jdellit/ramcloud/gtest/include -I/home/jdellit/ramcloud/logcabin -c -o bfs.o bfs.cc
	g++ -o bfs bfs.o /home/jdellit/ramcloud/obj.master/libramcloud.a -L/home/jdellit/ramcloud/obj.master -L/home/jdellit/local/lib -lpcrecpp -lboost_program_options -lprotobuf -lrt -lboost_filesystem -lboost_system -lpthread -lssl -lcrypto -rdynamic -libverbs -lprofiler

adjacency_list.pb.o: adjacency_list.proto	
	protoc -I=. --cpp_out=. ./adjacency_list.proto
	g++ -std=c++0x -c -o adjacency_list.pb.o adjacency_list.pb.cc 

mt_bfs: mt_bfs.cc mt_bfs.h sandy.h adjacency_list.pb.o
	g++ -std=c++0x -L/home/jdellit/ramcloud/obj.master -I/home/jdellit/ramcloud/src -I/home/jdellit/ramcloud/obj.master -I/home/jdellit/ramcloud/gtest/include -I/home/jdellit/ramcloud/logcabin -c -o mt_bfs.o mt_bfs.cc
	g++ -o mt_bfs mt_bfs.o adjacency_list.pb.o /home/jdellit/ramcloud/obj.master/libramcloud.a -L/home/jdellit/ramcloud/obj.master -L/home/jdellit/local/lib -lpcrecpp -lboost_program_options -lprotobuf -lrt -lboost_filesystem -lboost_system -lboost_thread -lpthread -lssl -lcrypto -rdynamic -libverbs -lprofiler

reader: reader.cc reader.h sandy.h adjacency_list.pb.o
	g++ -std=c++0x -L/home/jdellit/ramcloud/obj.master -I/home/jdellit/ramcloud/src -I/home/jdellit/ramcloud/obj.master -I/home/jdellit/ramcloud/gtest/include -I/home/jdellit/ramcloud/logcabin -c -o reader.o reader.cc 
	g++ -o reader reader.o adjacency_list.pb.o /home/jdellit/ramcloud/obj.master/libramcloud.a -L/home/jdellit/ramcloud/obj.master -L/home/jdellit/local/lib -lpcrecpp -lboost_program_options -lprotobuf -lrt -lboost_filesystem -lboost_system -lboost_thread -lpthread -lssl -lcrypto -rdynamic -libverbs -lprofiler

rd: rd.cc sandy.h adjacency_list.pb.o
	g++ -std=c++0x -L/home/jdellit/ramcloud/obj.master -I/home/jdellit/ramcloud/src -I/home/jdellit/ramcloud/obj.master -I/home/jdellit/ramcloud/gtest/include -I/home/jdellit/ramcloud/logcabin -c -o rd.o rd.cc 
	g++ -o rd rd.o adjacency_list.pb.o /home/jdellit/ramcloud/obj.master/libramcloud.a -L/home/jdellit/ramcloud/obj.master -L/home/jdellit/local/lib -lpcrecpp -lboost_program_options -lprotobuf -lrt -lboost_filesystem -lboost_system -lboost_thread -lpthread -lssl -lcrypto -rdynamic -libverbs -lprofiler

rc: rc.cc sandy.h adjacency_list.pb.o
	g++ -std=c++0x -L/home/jdellit/ramcloud/obj.master -I/home/jdellit/ramcloud/src -I/home/jdellit/ramcloud/obj.master -I/home/jdellit/ramcloud/gtest/include -I/home/jdellit/ramcloud/logcabin -c -o rc.o rc.cc 
	g++ -o rc rc.o adjacency_list.pb.o /home/jdellit/ramcloud/obj.master/libramcloud.a -L/home/jdellit/ramcloud/obj.master -L/home/jdellit/local/lib -lpcrecpp -lboost_program_options -lprotobuf -lrt -lboost_filesystem -lboost_system -lboost_thread -lpthread -lssl -lcrypto -rdynamic -libverbs -lprofiler

bfgg: bfgg.cc
	g++ -std=c++0x -c -o bfgg.o bfgg.cc 
	g++ -o bfgg bfgg.o 

clean:
	rm -f $(TARGETS) adjacency_list.pb.* *.o *.out
