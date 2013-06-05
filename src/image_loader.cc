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
#include "image_loader.h"

using namespace RAMCloud;

bool terminate = false;

uint64_t stat_nodes_written = 0;
uint64_t stat_bytes_written = 0;
uint64_t stat_time_write_start;
uint64_t stat_time_write_acc = 0;
uint64_t stat_time_prog_start;

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
  LOG(NOTICE, "Total time for program execution   : %f seconds", Cycles::toSeconds(Cycles::rdtsc() - stat_time_prog_start));
  LOG(NOTICE, "Total time for doing writes        : %f seconds", Cycles::toSeconds(stat_time_write_acc));
  LOG(NOTICE, "Total number of nodes written      : %lu nodes", stat_nodes_written);
  LOG(NOTICE, "Total number of bytes written      : %lu bytes", stat_bytes_written);
  LOG(NOTICE, "Average number of bytes per node   : %lu bytes/node", stat_bytes_written/stat_nodes_written);
  LOG(NOTICE, "Multiwrite average bandwidth (B/s) : %f B/s", stat_bytes_written/Cycles::toSeconds(stat_time_write_acc));
  LOG(NOTICE, "Multiwrite average bandwidth (N/s) : %f N/s", (float)stat_nodes_written/Cycles::toSeconds(stat_time_write_acc));
  LOG(NOTICE, "Multiwrite average latency (s/N)   : %f microseconds", Cycles::toSeconds(stat_time_write_acc)/float(stat_nodes_written) * 1000000.0);
  LOG(NOTICE, "Program average bandwidth (B/s)    : %f B/s", stat_bytes_written/Cycles::toSeconds(Cycles::rdtsc() - stat_time_prog_start));
  LOG(NOTICE, "Program average bandwidth (N/s)    : %f N/s", stat_nodes_written/Cycles::toSeconds(Cycles::rdtsc() - stat_time_prog_start));
//  LOG(NOTICE, "Total time doing misc0: %f seconds", Cycles::toSeconds(stat_time_misc0_acc));
//  LOG(NOTICE, "Total time doing misc1: %f seconds", Cycles::toSeconds(stat_time_misc1_acc));
//  LOG(NOTICE, "Total time doing misc2: %f seconds", Cycles::toSeconds(stat_time_misc2_acc));
//  LOG(NOTICE, "Total time doing misc3: %f seconds", Cycles::toSeconds(stat_time_misc3_acc));
//  LOG(NOTICE, "Total time doing misc4: %f seconds", Cycles::toSeconds(stat_time_misc4_acc));
//  LOG(NOTICE, "Total time doing misc5: %f seconds", Cycles::toSeconds(stat_time_misc5_acc));
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

  // Start up the reporter thread
  boost::thread reporter_thread(reporter);

  // Parse command line arguments.
  namespace fs = boost::filesystem;
  namespace po = boost::program_options;

  string prog_name = fs::basename(argv[0]);

  po::options_description desc("Allowed options");

  bool verbose;
  string input_file;
  uint32_t multiwrite_size;

  desc.add_options()
      ("help", "produce help message")
      ("verbose", po::value<bool>(&verbose)->default_value(false), "print additional messages")
      ("input_file", po::value<string>(&input_file)->required(), "specify the input file")
      ("multiwrite_size", po::value<uint32_t>(&multiwrite_size)->default_value(MULTIWRITE_REQ_MAX), "set the number of objects to batch together in a write to RAMCloud");

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

  graph_tableid = client.getTableId(GRAPH_TABLE_NAME);

  std::fstream rcimage_filestream(input_file, std::fstream::in);

  uint64_t read_buffer[READ_BUFFER_SIZE];
  uint64_t buf_index = 0;
  uint64_t bytes_in_buf;
  uint64_t segment_length;
  uint64_t req_index = 0;
  Tub<MultiWriteObject> objects[MULTIWRITE_REQ_MAX];
  MultiWriteObject* requests[MULTIWRITE_REQ_MAX];

  stat_time_prog_start = Cycles::rdtsc();

  rcimage_filestream.read((char*)read_buffer, READ_BUFFER_SIZE*sizeof(uint64_t));
  bytes_in_buf = rcimage_filestream.gcount();

  // Loop through this as long as the read_buffer has been filled completely
  while(bytes_in_buf == READ_BUFFER_SIZE*sizeof(uint64_t)) {
    // Check if there's a non-truncated segment ahead in the buffer
    if((buf_index + 1 < READ_BUFFER_SIZE) && (buf_index + 1 + read_buffer[buf_index + 1] < READ_BUFFER_SIZE)) {
      // Segment is len(key) + len(value)
      segment_length = 2 + read_buffer[buf_index + 1];
   
      //LOG(NOTICE, "Forming MW request for new key %d with segment length %d", read_buffer[buf_index], segment_length);
   
      objects[req_index].construct( graph_tableid,
                                    (char*)&read_buffer[buf_index],
                                    sizeof(uint64_t),
                                    (char*)&read_buffer[buf_index + 1],
                                    (segment_length-1)*sizeof(uint64_t) );
      requests[req_index] = objects[req_index].get();      
      req_index++;
 
      stat_bytes_written += (segment_length-1)*sizeof(uint64_t);
     
      if(req_index == MULTIWRITE_REQ_MAX) {
        //LOG(NOTICE, "Writing to RAMCloud %d requests...", req_index);
        stat_time_write_start = Cycles::rdtsc();
        try {
          client.multiWrite(requests, MULTIWRITE_REQ_MAX);
        } catch(RAMCloud::ClientException& e) {
          fprintf(stderr, "RAMCloud exception: %s\n", e.str().c_str());
          return 1;
        }
        stat_time_write_acc += Cycles::rdtsc() - stat_time_write_start;

        stat_nodes_written += MULTIWRITE_REQ_MAX;

        req_index = 0;
      }       

      buf_index += segment_length;
    } else { // The end of the read buffer truncates the segment
      //LOG(NOTICE, "Encountered partial segment... undergoing analysis...");
      //LOG(NOTICE, "\tbuf_index: %d", buf_index);
      if(buf_index < READ_BUFFER_SIZE) {
        //LOG(NOTICE, "\tkey: %d", read_buffer[buf_index]);
        if(buf_index + 1 < READ_BUFFER_SIZE) {
          //LOG(NOTICE, "\tneighbors: %d", read_buffer[buf_index + 1]);
        }
      }
      //LOG(NOTICE, "Flushing %d requests...", req_index);
      // Flush currently formed requests, since we're about to re-fill the read buffer
      stat_time_write_start = Cycles::rdtsc();
      try {
        client.multiWrite(requests, req_index);
      } catch(RAMCloud::ClientException& e) {
        fprintf(stderr, "RAMCloud exception: %s\n", e.str().c_str());
        return 1;
      }
      stat_time_write_acc += Cycles::rdtsc() - stat_time_write_start;

      stat_nodes_written += req_index;

      req_index = 0;
   
      //LOG(NOTICE, "Copying partial segment of size %d to front of buffer", (READ_BUFFER_SIZE - buf_index));
 
      // Copy the partial segment to the head of the read buffer
      memcpy((char*)read_buffer, (char*)&read_buffer[buf_index], (READ_BUFFER_SIZE - buf_index)*sizeof(uint64_t));

      //LOG(NOTICE, "\tPost copy key: %d", read_buffer[0]);
      //LOG(NOTICE, "\tPost copy neighbors: %d", read_buffer[1]);
      
      // Read more segments into the remaining portion of the read buffer
      rcimage_filestream.read((char*)&read_buffer[READ_BUFFER_SIZE - buf_index], buf_index*sizeof(uint64_t));

      // Recalculate bytes_in_buf
      bytes_in_buf = (READ_BUFFER_SIZE - buf_index)*sizeof(uint64_t) + rcimage_filestream.gcount();

      buf_index = 0; 

      //LOG(NOTICE, "\tPost read key: %d", read_buffer[buf_index]);
      //LOG(NOTICE, "\tPost read neighbors: %d", read_buffer[buf_index + 1]);
    } 
  }

  // See if there are segments remaining
  if(bytes_in_buf) {
    // Sanity check -- last block contains integer number of uint64_t's
    if(bytes_in_buf % sizeof(uint64_t)) {
      fprintf(stderr, "ERROR: last image block of size %d is not a multiple of %d\n", bytes_in_buf, sizeof(uint64_t));
      return 1;
    }
    
    uint64_t read_buffer_size = bytes_in_buf/sizeof(uint64_t);

    while(buf_index < read_buffer_size) {
      segment_length = 2 + read_buffer[buf_index + 1];
      
      objects[req_index].construct( graph_tableid,
                                    (char*)&read_buffer[buf_index],
                                    sizeof(uint64_t),
                                    (char*)&read_buffer[buf_index + 1],
                                    (segment_length-1)*sizeof(uint64_t) );
      requests[req_index] = objects[req_index].get();      
      req_index++;
     
      stat_bytes_written += (segment_length-1)*sizeof(uint64_t);
 
      if(req_index == MULTIWRITE_REQ_MAX) {
        stat_time_write_start = Cycles::rdtsc();
        try {
          client.multiWrite(requests, MULTIWRITE_REQ_MAX);
        } catch(RAMCloud::ClientException& e) {
          fprintf(stderr, "RAMCloud exception: %s\n", e.str().c_str());
          return 1;
        }
        stat_time_write_acc += Cycles::rdtsc() - stat_time_write_start;

        stat_nodes_written += MULTIWRITE_REQ_MAX;

        req_index = 0;
      }       

      buf_index += segment_length;      
    }

    stat_time_write_start = Cycles::rdtsc();
    try {
      client.multiWrite(requests, req_index);
    } catch(RAMCloud::ClientException& e) {
      fprintf(stderr, "RAMCloud exception: %s\n", e.str().c_str());
      return 1;
    }
    stat_time_write_acc += Cycles::rdtsc() - stat_time_write_start;

    stat_nodes_written += req_index;

    req_index = 0;
  }
  
  // Send terminate signal to the reporter thread
  terminate = true;
  reporter_thread.join();
  
  return 0;
} 
