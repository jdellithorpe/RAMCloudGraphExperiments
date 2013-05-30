#include <boost/algorithm/string.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/filesystem.hpp>
#include <boost/program_options.hpp>
#include <boost/thread.hpp>
#include <iostream>
#include <fstream>
#include "ShortMacros.h"
#include "RamCloud.h"
#include "Cycles.h"
#include "sandy.h"
#include "reader.h"
#include "adjacency_list.pb.h"

using namespace RAMCloud;
using namespace Sandy::ProtoBuf;

bool terminate = false;

uint64_t stat_time_misc0_start;
uint64_t stat_time_misc0_acc = 0;
uint64_t stat_time_misc1_start;
uint64_t stat_time_misc1_acc = 0;
uint64_t stat_time_misc2_start;
uint64_t stat_time_misc2_acc = 0;
uint64_t stat_time_misc3_start;
uint64_t stat_time_misc3_acc = 0;
uint64_t stat_time_misc4_start;
uint64_t stat_time_misc4_acc = 0;
uint64_t stat_time_misc5_start;
uint64_t stat_time_misc5_acc = 0;

uint64_t node_write_count = 0;
uint64_t bytes_written = 0;
uint64_t stat_time_write_start;
uint64_t stat_time_write_acc = 0;

void report_stats() {
  LOG(NOTICE, "Total time for program execution   : %f seconds", Cycles::toSeconds(Cycles::rdtsc() - stat_time_misc5_start));
  LOG(NOTICE, "Total time for doing writes        : %f seconds", Cycles::toSeconds(stat_time_write_acc));
  LOG(NOTICE, "Total number of nodes written      : %lu nodes", node_write_count);
  LOG(NOTICE, "Total number of bytes written      : %lu bytes", bytes_written);
  LOG(NOTICE, "Average number of bytes per node   : %lu bytes/node", bytes_written/node_write_count);
  LOG(NOTICE, "Multiwrite average bandwidth (B/s) : %f B/s", bytes_written/Cycles::toSeconds(stat_time_write_acc));
  LOG(NOTICE, "Multiwrite average bandwidth (N/s) : %f N/s", (float)node_write_count/Cycles::toSeconds(stat_time_write_acc));
  LOG(NOTICE, "Multiwrite average latency (s/N)   : %f microseconds", Cycles::toSeconds(stat_time_write_acc)/float(node_write_count) * 1000000.0);
  LOG(NOTICE, "Program average bandwidth (B/s)    : %f B/s", bytes_written/Cycles::toSeconds(Cycles::rdtsc() - stat_time_misc5_start));
  LOG(NOTICE, "Program average bandwidth (N/s)    : %f N/s", node_write_count/Cycles::toSeconds(Cycles::rdtsc() - stat_time_misc5_start));
  LOG(NOTICE, "Total time doing misc0: %f seconds", Cycles::toSeconds(stat_time_misc0_acc));
  LOG(NOTICE, "Total time doing misc1: %f seconds", Cycles::toSeconds(stat_time_misc1_acc));
  LOG(NOTICE, "Total time doing misc2: %f seconds", Cycles::toSeconds(stat_time_misc2_acc));
  LOG(NOTICE, "Total time doing misc3: %f seconds", Cycles::toSeconds(stat_time_misc3_acc));
  LOG(NOTICE, "Total time doing misc4: %f seconds", Cycles::toSeconds(stat_time_misc4_acc));
  LOG(NOTICE, "Total time doing misc5: %f seconds", Cycles::toSeconds(stat_time_misc5_acc));
}

/**
 * Main function of the reporter thread
 * Reports some stats in fixed intervals of time.
 **/
void reporter() {
  while(true) {
    sleep(REPORTER_NAP_TIME);
    if(terminate) 
      return;
    LOG(NOTICE, "-------------------- Reporter Output Start --------------------");
    report_stats();
    LOG(NOTICE, "-------------------- Reporter Output Stop --------------------");
  }
}

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
      ("ramcloud_format", po::value<string>(&ramcloud_format)->required(), "specify the ramcloud internal format (v1:string objects and string keys, v2:protobuf objects and string keys, v3:protobuf objects and integer keys, v4: integer array objects and integer keys)")
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

  // Start up the reporter thread
  boost::thread reporter_thread(reporter);

  if(input_format == "edge_list") {
    if( ramcloud_format == "v2" ) {
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
    } else if( ramcloud_format == "v1" ) {
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
    std::vector<uint64_t> adj_list_intvec[MULTIWRITE_REQ_MAX];
    string src_str[MULTIWRITE_REQ_MAX];
    uint64_t src_int[MULTIWRITE_REQ_MAX];
    size_t multiwrite_queue_size = 0;
    MultiWriteObject* requests[MULTIWRITE_REQ_MAX];
    Tub<MultiWriteObject> objects[MULTIWRITE_REQ_MAX];

    stat_time_misc5_start = Cycles::rdtsc();
    while(true) {
      
      stat_time_misc0_start = Cycles::rdtsc();
      if(!std::getline(graph_filestream, adj_str))
        break;
      stat_time_misc0_acc += Cycles::rdtsc() - stat_time_misc0_start;    
  
      stat_time_misc1_start = Cycles::rdtsc();
      boost::split(adj_vec, adj_str, boost::is_any_of(" "));
      stat_time_misc1_acc += Cycles::rdtsc() - stat_time_misc1_start;
      if(adj_vec.size() > 1) {
        if( ramcloud_format == "v4" ) {        
          stat_time_misc2_start = Cycles::rdtsc();
          src_int[multiwrite_queue_size] = boost::lexical_cast<uint64_t>(adj_vec[0]);
          stat_time_misc2_acc += Cycles::rdtsc() - stat_time_misc2_start;          

          uint64_t append_int;
          append_int = (uint64_t)adj_vec.size()-1;
          
          stat_time_misc3_start = Cycles::rdtsc();
          adj_list_str[multiwrite_queue_size].clear();          
          adj_list_str[multiwrite_queue_size].append((char*)&append_int, sizeof(uint64_t));

          for(int i = 1; i<adj_vec.size(); i++) {
            append_int = boost::lexical_cast<uint64_t>(adj_vec[i]);
            adj_list_str[multiwrite_queue_size].append((char*)&append_int, sizeof(uint64_t));
          }
          stat_time_misc3_acc += Cycles::rdtsc() - stat_time_misc3_start;

          stat_time_misc4_start = Cycles::rdtsc();
          objects[multiwrite_queue_size].construct( graph_tableid,
                                                    (char*)&src_int[multiwrite_queue_size],
                                                    sizeof(uint64_t),
                                                    adj_list_str[multiwrite_queue_size].c_str(),
                                                    adj_list_str[multiwrite_queue_size].length() );
          stat_time_misc4_acc += Cycles::rdtsc() - stat_time_misc4_start;
        } else if( ramcloud_format == "v3" ) {
          src_int[multiwrite_queue_size] = boost::lexical_cast<uint64_t>(adj_vec[0]);
          adj_list_pb.clear_neighbor();
          for(int i = 1; i<adj_vec.size(); i++) {
            adj_list_pb.add_neighbor(boost::lexical_cast<uint64_t>(adj_vec[i]));
          }
          adj_list_pb.SerializeToString(&adj_list_str[multiwrite_queue_size]);
          objects[multiwrite_queue_size].construct( graph_tableid,
                                                    (char*)&src_int[multiwrite_queue_size],
                                                    sizeof(uint64_t),
                                                    adj_list_str[multiwrite_queue_size].c_str(),
                                                    adj_list_str[multiwrite_queue_size].length() );
        } else if( ramcloud_format == "v2" ) {
          src_str[multiwrite_queue_size] = adj_vec[0];
          adj_list_pb.clear_neighbor();
          for(int i = 1; i<adj_vec.size(); i++) {
            adj_list_pb.add_neighbor(boost::lexical_cast<uint64_t>(adj_vec[i]));
          }
          adj_list_pb.SerializeToString(&adj_list_str[multiwrite_queue_size]);
          objects[multiwrite_queue_size].construct( graph_tableid,
                                                    src_str[multiwrite_queue_size].c_str(),
                                                    src_str[multiwrite_queue_size].length(),
                                                    adj_list_str[multiwrite_queue_size].c_str(),
                                                    adj_list_str[multiwrite_queue_size].length() );          
        } else if( ramcloud_format == "v1" ) {
          src_str[multiwrite_queue_size] = adj_vec[0];
          adj_list_str[multiwrite_queue_size] = adj_str.substr(adj_vec[0].length()+1);
          objects[multiwrite_queue_size].construct( graph_tableid,
                                                    src_str[multiwrite_queue_size].c_str(),
                                                    src_str[multiwrite_queue_size].length(),
                                                    adj_list_str[multiwrite_queue_size].c_str(),
                                                    adj_list_str[multiwrite_queue_size].length() ); 
        } else {
          fprintf(stderr, "ERROR: Unrecognized value for ramcloud_format program option: %s\n", ramcloud_format.c_str());
          return -1;
        }

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

      }
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
    stat_time_misc5_acc += Cycles::rdtsc() - stat_time_misc5_start;
  } else {
    fprintf(stderr, "ERROR: Unrecognized value for input_format program option: %s\n", input_format.c_str());
    return -1;
  }

  // Send terminate signal to the reporter thread
  terminate = true;
  reporter_thread.join();
  
  return 0;
}
