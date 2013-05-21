#include <boost/algorithm/string.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/filesystem.hpp>
#include <boost/program_options.hpp>
#include <iostream>
#include <fstream>
#include "RamCloud.h"
#include "sandy.h"
#include "reader.h"
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
  string ramcloud_format;
  string input_file;
  bool verbose = false;
  uint32_t multiwrite_size = MULTIWRITE_REQ_MAX;

  desc.add_options()
      ("help", "produce help message")
      ("verbose", "print additional messages")
      ("input_format", po::value<string>(&input_format)->required(), "specify the input file format (adj_list, edge_list)")
      ("ramcloud_format", po::value<string>(&ramcloud_format)->required(), "specify the ramcloud internal format (protobuf, string)")
      ("input_file", po::value<string>(&input_file)->required(), "specify the input file")
      ("multiwrite_size", po::value<uint32_t>(&multiwrite_size), "set the number of objects to batch together in a write to RAMCloud (default/max 64, NOTE: multiWrite currently only implemented for input formats of type adj_list)");

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

  graph_tableid = client.getTableId(GRAPH_TABLE_NAME);

  std::fstream graph_filestream(input_file, std::fstream::in);

  if(input_format == "edge_list") {
    if( ramcloud_format == "protobuf" ) {
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
    } else if( ramcloud_format == "string" ) {
      fprintf(stderr, "ERROR: input_format program option %s does not support ramcloud_format program option %s\n", input_format.c_str(), ramcloud_format.c_str());
      return -1;
    } else {
      fprintf(stderr, "ERROR: Unrecognized value for ramcloud_format program option: %s\n", ramcloud_format.c_str());
      return -1; 
    }
  } else if(input_format == "adj_list") {
    string adj_str;
    std::vector<string> adj_vec;
    AdjacencyList adj_list_pb;
    string adj_list_str[MULTIWRITE_REQ_MAX];
    string src[MULTIWRITE_REQ_MAX];
    uint64_t node_write_count = 0;
    uint64_t bytes_written = 0;
    uint64_t stat_time_write_start;
    uint64_t stat_time_write_acc = 0;
    size_t multiwrite_queue_size = 0;
    MultiWriteObject* requests[MULTIWRITE_REQ_MAX];
    Tub<MultiWriteObject> objects[MULTIWRITE_REQ_MAX];
    while(std::getline(graph_filestream, adj_str)) {
      boost::split(adj_vec, adj_str, boost::is_any_of(" "));
      if(adj_vec.size() > 1) {
        src[multiwrite_queue_size] = adj_vec[0];

        if( ramcloud_format == "protobuf" ) {
          for(int i = 1; i<adj_vec.size(); i++) {
            adj_list_pb.add_neighbor(boost::lexical_cast<uint64_t>(adj_vec[i]));
          }
          adj_list_pb.SerializeToString(&adj_list_str[multiwrite_queue_size]);
        } else if( ramcloud_format == "string" ) {
          adj_list_str[multiwrite_queue_size] = adj_str.substr(adj_vec[0].length()+1);
        } else {
          fprintf(stderr, "ERROR: Unrecognized value for ramcloud_format program option: %s\n", ramcloud_format.c_str());
          return -1;
        }

        objects[multiwrite_queue_size].construct( graph_tableid,
                                                  src[multiwrite_queue_size].c_str(),
                                                  src[multiwrite_queue_size].length(),
                                                  adj_list_str[multiwrite_queue_size].c_str(),
                                                  adj_list_str[multiwrite_queue_size].length() );
        requests[multiwrite_queue_size] = objects[multiwrite_queue_size].get();

        node_write_count++;
        bytes_written += adj_list_str[multiwrite_queue_size].length();
        multiwrite_queue_size++;

        if(multiwrite_queue_size == multiwrite_size) {
          stat_time_write_start = Cycles::rdtsc();
          try {
            client.multiWrite(requests, multiwrite_queue_size);
          } catch(RAMCloud::ClientException& e) {
            fprintf(stderr, "RAMCloud exception: %s\n", e.str().c_str());
            return 1;
          }
          stat_time_write_acc += Cycles::rdtsc() - stat_time_write_start;

          multiwrite_queue_size = 0;
        }

        if(verbose && (node_write_count%100000)==0)
          std::cout << prog_name << ": INFO: wrote " << node_write_count << "th vertex to RAMCloud, total of " << bytes_written << "B written @ " << bytes_written/Cycles::toSeconds(stat_time_write_acc) << "B/s, " << Cycles::toMicroseconds(stat_time_write_acc)/(double)node_write_count << "us/node\n";
      }
      adj_list_pb.clear_neighbor();
    }

    if(multiwrite_queue_size != 0) {
      try {
        client.multiWrite(requests, multiwrite_queue_size);
      } catch(RAMCloud::ClientException& e) {
        fprintf(stderr, "RAMCloud exception: %s\n", e.str().c_str());
        return 1;
      }

      multiwrite_queue_size = 0;
    }

  } else {
    fprintf(stderr, "ERROR: Unrecognized value for input_format program option: %s\n", input_format.c_str());
    return -1;
  }

  return 0;
}
