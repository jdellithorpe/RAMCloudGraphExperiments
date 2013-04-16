#include <boost/algorithm/string.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/filesystem.hpp>
#include <boost/program_options.hpp>
#include <iostream>
#include <fstream>
#include "RamCloud.h"
#include "sandy.h"
#include "adjacency_list.pb.h"

using namespace RAMCloud;
using namespace Sandy::ProtoBuf;

int main(int argc, char* argv[]) {
  GOOGLE_PROTOBUF_VERIFY_VERSION;

  // Parse command line arguments.
  namespace fs = boost::filesystem;
  namespace po = boost::program_options;

  string prog_name = fs::basename(argv[0]);

  po::options_description desc("Allowed options");

  desc.add_options()
      ("help", "produce help message")
      ("input_format", po::value<string>()->required(), "set the input format")
      ("input_file", po::value<string>()->required(), "specify the input file");

  po::variables_map vm;

  try {
    po::store(po::parse_command_line(argc, argv, desc), vm);

    if(vm.count("help")) {
      std::cout << prog_name << ": read an input graph into RAMCloud.\n\n";
      std::cout << desc << "\n";
      return 1;
    }

    po::notify(vm);
  } catch(po::required_option& e) {
    std::cerr << "ERROR: " << e.what() << std::endl << std::endl;
    std::cout << desc << "\n";
    return -1;
  } catch(po::error& e) {
    std::cerr << "ERROR: " << e.what() << std::endl << std::endl;
    std::cout << desc << "\n";
    return -1;
  }

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
