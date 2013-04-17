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

  string input_format;
  string input_file;
  uint32_t server_span = 1;
  bool verbose = false;

  desc.add_options()
      ("help", "produce help message")
      ("verbose", "print additional messages")
      ("input_format", po::value<string>(&input_format)->required(), "specify the input file format (adj_list, edge_list)")
      ("input_file", po::value<string>(&input_file)->required(), "specify the input file")
      ("server_span", po::value<uint32_t>(&server_span), "set the number of servers to spread the table across (default 1)" );

  po::variables_map vm;

  try {
    po::store(po::parse_command_line(argc, argv, desc), vm);

    if(vm.count("help")) {
      std::cout << prog_name << ": read an input graph into RAMCloud.\n\n";
      std::cout << desc << "\n";
      return 1;
    }

    if(vm.count("verbose"))
      verbose = true;

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
  client.createTable(GRAPH_TABLE_NAME, server_span);

  if(verbose)
    std::cout << prog_name << ": INFO: created table with server span set to: " << server_span << "\n";

  graph_tableid = client.getTableId(GRAPH_TABLE_NAME);

  std::fstream graph_filestream(input_file, std::fstream::in);

  if(input_format == "edge_list") {
    string edge_str;
    std::vector<string> edge_vec;
    AdjacencyList adj_list_pb;
    string adj_list_str;
    string src;
    while(std::getline(graph_filestream, edge_str)) {
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
                      adj_list_str.c_str(),
                      adj_list_str.length() );
      } catch (RAMCloud::ClientException& e) {
        fprintf(stderr, "RAMCloud exception: %s\n", e.str().c_str());
        return 1;
      }
      adj_list_pb.clear_neighbor();
    }
  } else if(input_format == "adj_list") {
    string adj_str;
    std::vector<string> adj_vec;
    AdjacencyList adj_list_pb;
    string adj_list_str;
    string src;
    uint64_t node_write_count = 0;
    uint64_t bytes_written = 0;
    while(std::getline(graph_filestream, adj_str)) {
      boost::split(adj_vec, adj_str, boost::is_any_of(" "));
      src = adj_vec[0];
      for(int i = 1; i<adj_vec.size(); i++) {
        adj_list_pb.add_neighbor(boost::lexical_cast<uint64_t>(adj_vec[i]));
      }
      if(adj_list_pb.neighbor_size() > 0) {
        adj_list_pb.SerializeToString(&adj_list_str);
        try {
          client.write( graph_tableid,
                        src.c_str(),
                        src.length(),
                        adj_list_str.c_str(),
                        adj_list_str.length() );
          node_write_count++;
          bytes_written += adj_list_str.length();
          if(verbose && (node_write_count%100000)==0)
              std::cout << prog_name << ": INFO: wrote " << node_write_count << "th vertex to RAMCloud, total of " << bytes_written << "B written\n";
        } catch (RAMCloud::ClientException& e) {
          fprintf(stderr, "RAMCloud exception: %s\n", e.str().c_str());
          return 1;
        }
      }
      adj_list_pb.clear_neighbor();
    }
  } else {
    fprintf(stderr, "ERROR: Unrecognized value for input_format program option: %s\n", input_format.c_str());
    return -1;
  }

  return 0;
}
