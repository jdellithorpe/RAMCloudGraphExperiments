#include <boost/algorithm/string.hpp>
#include <boost/lexical_cast.hpp>
#include <iostream>
#include <fstream>
#include "RamCloud.h"
#include "sandy.h"
#include "adjacency_list.pb.h"

using namespace RAMCloud;
using namespace Sandy::ProtoBuf;

int main(int argc, char* argv[]) {
  GOOGLE_PROTOBUF_VERIFY_VERSION;

  uint64_t graph_tableid;

  RamCloud client(COORDINATOR_LOCATION);

  client.dropTable(GRAPH_TABLE_NAME);
  client.createTable(GRAPH_TABLE_NAME, boost::lexical_cast<uint32_t>(argv[2]));

  graph_tableid = client.getTableId(GRAPH_TABLE_NAME);

  std::fstream edge_list_file(argv[1], std::fstream::in);

  string edge_str;
  std::vector<string> edge_vec;
  AdjacencyList adj_list_pb;
  string adj_list_str;
  string src;
  while(std::getline(edge_list_file, edge_str)) {
    boost::split(edge_vec, edge_str, boost::is_any_of(" \t"));
    if(!src.empty() and src != edge_vec[0]) {
      adj_list_pb.SerializeToString(&adj_list_str);
      try {
        client.write( graph_tableid,
                      src.c_str(),
                      src.length(),
                      adj_list_str.c_str(),
                      adj_list_str.length() );
      } catch (RAMCloud::ClientException& e) {
        fprintf(stderr, "RAMCloud exception: %s\n", e.str().c_str());
        return 1;
      }
      adj_list_pb.clear_neighbor();
    }
    src = edge_vec[0];
    adj_list_pb.add_neighbor(boost::lexical_cast<uint64_t>(edge_vec[1]));
  }

  if(!src.empty()) {
    adj_list_pb.SerializeToString(&adj_list_str);
    try {
      client.write( graph_tableid,
                    src.c_str(),
                    src.length(),
                    adj_list_str.c_str() );
    } catch (RAMCloud::ClientException& e) {
      fprintf(stderr, "RAMCloud exception: %s\n", e.str().c_str());
      return 1;
    }
    adj_list_pb.clear_neighbor();
  }

  return 0;
}
