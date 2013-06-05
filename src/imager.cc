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
#include "imager.h"

using namespace RAMCloud;

bool terminate = false;

uint64_t stat_nodes_read = 0;
uint64_t stat_bytes_written = 0;
uint64_t stat_time_total_start;
uint64_t stat_time_total_acc = 0;
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

void report_stats() {
  LOG(NOTICE, "Total time for program execution   : %f seconds", Cycles::toSeconds(Cycles::rdtsc() - stat_time_total_start));
  LOG(NOTICE, "Total number of nodes read         : %lu nodes", stat_nodes_read);
  LOG(NOTICE, "Total number of bytes written      : %lu bytes", stat_bytes_written);
  LOG(NOTICE, "Program average bandwidth (B/s)    : %f B/s", stat_bytes_written/Cycles::toSeconds(Cycles::rdtsc() - stat_time_total_start));
  LOG(NOTICE, "Program average bandwidth (N/s)    : %f N/s", stat_nodes_read/Cycles::toSeconds(Cycles::rdtsc() - stat_time_total_start));
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
  // Parse command line arguments.
  namespace fs = boost::filesystem;
  namespace po = boost::program_options;

  string prog_name = fs::basename(argv[0]);

  po::options_description desc("Allowed options");

  bool verbose;
  string input_file;
  string output_file;

  desc.add_options()
      ("help", "produce help message")
      ("verbose", po::value<bool>(&verbose)->default_value(false), "verbose")
      ("input_file", po::value<string>(&input_file)->required(), "specify the input file")
      ("output_file", po::value<string>(&output_file)->required(), "specify the output file");

  po::variables_map vm;

  try {
    po::store(po::parse_command_line(argc, argv, desc), vm);

    if(vm.count("help")) {
      std::cout << prog_name << ": convert input graph adjacency list file into RAMCloud image.\n\n";
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

  std::fstream input_filestream(input_file, std::fstream::in);
  std::fstream output_filestream(output_file, std::fstream::out);

  // Start up the reporter thread
  boost::thread reporter_thread(reporter);

  string adj_str;
  std::vector<string> adj_vec;
  uint64_t buffer[WRITE_BUFFER_SIZE];  
  uint64_t buffer_index = 0;
 
  stat_time_total_start = Cycles::rdtsc(); 
  while(true) {
    if(!std::getline(input_filestream, adj_str))
      break;

    boost::split(adj_vec, adj_str, boost::is_any_of(" "));

    if(adj_vec.size() > 1) {
      // Write the key
      buffer[buffer_index] = boost::lexical_cast<uint64_t>(adj_vec[0]);
      buffer_index++;
      if(buffer_index == WRITE_BUFFER_SIZE) {
        output_filestream.write((char*)buffer, WRITE_BUFFER_SIZE*sizeof(uint64_t));
        buffer_index = 0;
        stat_bytes_written += WRITE_BUFFER_SIZE*sizeof(uint64_t);
      }

      // Write the neighbor count
      buffer[buffer_index] = (uint64_t)adj_vec.size()-1;
      buffer_index++;
      if(buffer_index == WRITE_BUFFER_SIZE) {
        output_filestream.write((char*)buffer, WRITE_BUFFER_SIZE*sizeof(uint64_t));
        buffer_index = 0;
        stat_bytes_written += WRITE_BUFFER_SIZE*sizeof(uint64_t);
      }

      // Write the neighbors
      for(int i = 1; i<adj_vec.size(); i++) {
        buffer[buffer_index] = boost::lexical_cast<uint64_t>(adj_vec[i]);
        buffer_index++;
        if(buffer_index == WRITE_BUFFER_SIZE) {
          output_filestream.write((char*)buffer, WRITE_BUFFER_SIZE*sizeof(uint64_t));
          buffer_index = 0;
          stat_bytes_written += WRITE_BUFFER_SIZE*sizeof(uint64_t);
        } 
      }
    }
    stat_nodes_read++;
  }

  if(buffer_index != 0) {
    output_filestream.write((char*)buffer, buffer_index*sizeof(uint64_t));
    stat_bytes_written += buffer_index*sizeof(uint64_t);
  }
  stat_time_total_acc += Cycles::rdtsc() - stat_time_total_start;  

  input_filestream.close();
  output_filstream.close();
    
  // Terminate terminate reporter thread
  terminate = true;
  reporter_thread.join();

  return 0;
}
