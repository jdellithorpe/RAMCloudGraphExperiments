TARGETS = bfs mt_bfs imager reader rd rc bfgg

all: $(TARGETS)

bfs: bfs.cc bfs.h sandy.h 
	g++ -std=c++0x -L/home/jdellit/ramcloud/obj.master -I/home/jdellit/ramcloud/src -I/home/jdellit/ramcloud/obj.master -I/home/jdellit/ramcloud/gtest/include -I/home/jdellit/ramcloud/logcabin -c -o bfs.o bfs.cc
	g++ -o bfs bfs.o /home/jdellit/ramcloud/obj.master/libramcloud.a -L/home/jdellit/ramcloud/obj.master -L/home/jdellit/local/lib -lpcrecpp -lboost_program_options -lprotobuf -lrt -lboost_filesystem -lboost_system -lpthread -lssl -lcrypto -rdynamic -libverbs -lprofiler -lboost_thread

adjacency_list.pb.o: adjacency_list.proto	
	protoc -I=. --cpp_out=. ./adjacency_list.proto
	g++ -std=c++0x -c -o adjacency_list.pb.o adjacency_list.pb.cc 

mt_bfs: mt_bfs.cc mt_bfs.h sandy.h adjacency_list.pb.o
	g++ -std=c++0x -L/home/jdellit/ramcloud/obj.master -I/home/jdellit/ramcloud/src -I/home/jdellit/ramcloud/obj.master -I/home/jdellit/ramcloud/gtest/include -I/home/jdellit/ramcloud/logcabin -c -o mt_bfs.o mt_bfs.cc
	g++ -o mt_bfs mt_bfs.o adjacency_list.pb.o /home/jdellit/ramcloud/obj.master/libramcloud.a -L/home/jdellit/ramcloud/obj.master -L/home/jdellit/local/lib -lpcrecpp -lboost_program_options -lprotobuf -lrt -lboost_filesystem -lboost_system -lboost_thread -lpthread -lssl -lcrypto -rdynamic -libverbs -lprofiler

imager: imager.cc imager.h sandy.h
	g++ -std=c++0x -L/home/jdellit/ramcloud/obj.master -I/home/jdellit/ramcloud/src -I/home/jdellit/ramcloud/obj.master -I/home/jdellit/ramcloud/gtest/include -I/home/jdellit/ramcloud/logcabin -c -o imager.o imager.cc
	g++ -o imager imager.o /home/jdellit/ramcloud/obj.master/libramcloud.a -L/home/jdellit/ramcloud/obj.master -L/home/jdellit/local/lib -lpcrecpp -lboost_program_options -lprotobuf -lrt -lboost_filesystem -lboost_system -lboost_thread -lpthread -lssl -lcrypto -rdynamic -libverbs -lprofiler

image_loader: image_loader.cc image_loader.h sandy.h
	g++ -std=c++0x -L/home/jdellit/ramcloud/obj.master -I/home/jdellit/ramcloud/src -I/home/jdellit/ramcloud/obj.master -I/home/jdellit/ramcloud/gtest/include -I/home/jdellit/ramcloud/logcabin -c -o image_loader.o image_loader.cc
	g++ -o image_loader image_loader.o /home/jdellit/ramcloud/obj.master/libramcloud.a -L/home/jdellit/ramcloud/obj.master -L/home/jdellit/local/lib -lpcrecpp -lboost_program_options -lprotobuf -lrt -lboost_filesystem -lboost_system -lboost_thread -lpthread -lssl -lcrypto -rdynamic -libverbs -lprofiler

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
