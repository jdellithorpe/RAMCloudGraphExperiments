#include <boost/algorithm/string.hpp>
#include <boost/dynamic_bitset.hpp>
#include <boost/tuple/tuple.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/thread.hpp>
#include <algorithm>
#include <iostream>
#include <queue>
#include <bitset>
#include <set>
#include <map>
#include "Cycles.h"
#include "ShortMacros.h"
#include "RamCloud.h"
#include "sandy.h"
#include "adjacency_list.pb.h"

using namespace RAMCloud;
using namespace Sandy::ProtoBuf;

boost::condition_variable frontier_edge_q_condvar;
boost::condition_variable frontier_node_q_condvar;
boost::condition_variable node_distance_q_condvar;
boost::mutex frontier_edge_q_mutex;
boost::mutex frontier_node_q_mutex;
boost::mutex node_distance_q_mutex;
std::queue<boost::tuple<string,AdjacencyList,uint64_t>> frontier_edge_q;
std::queue<boost::tuple<string,uint64_t>> frontier_node_q;
std::queue<boost::tuple<string,string>> node_distance_q;

/// Global program termination bit
bool terminate = false;

/// Stat globals
size_t stat_max_frontier_edge_q_size = 0;
size_t stat_max_frontier_node_q_size = 0;
size_t stat_max_node_distance_q_size = 0;
uint64_t stat_time_alg_start;
uint64_t stat_time_alg_acc = 0;
uint64_t stat_time_term_start;
uint64_t stat_time_term_acc = 0;
uint64_t stat_time_read_start;
uint64_t stat_time_read_acc = 0;
uint64_t stat_time_write_start;
uint64_t stat_time_write_acc = 0;
uint64_t stat_time_edge_trav_start;
uint64_t stat_time_edge_trav_acc = 0;
uint64_t stat_nodes_read_acc = 0;
uint64_t stat_nodes_write_acc = 0;
uint64_t stat_bytes_read_acc = 0;
uint64_t stat_time_frontier_node_q_wait_start;
uint64_t stat_time_frontier_node_q_wait_acc = 0;
uint64_t stat_time_frontier_edge_q_wait_start;
uint64_t stat_time_frontier_edge_q_wait_acc = 0;
uint64_t stat_time_node_distance_q_wait_start;
uint64_t stat_time_node_distance_q_wait_acc = 0;
uint64_t stat_time_frontier_node_q_enqueue_start;
uint64_t stat_time_frontier_node_q_enqueue_acc = 0;
uint64_t stat_time_frontier_edge_q_enqueue_start;
uint64_t stat_time_frontier_edge_q_enqueue_acc = 0;
uint64_t stat_time_node_distance_q_enqueue_start;
uint64_t stat_time_node_distance_q_enqueue_acc = 0;
uint64_t stat_time_frontier_node_q_access_start;
uint64_t stat_time_frontier_node_q_access_acc = 0;
uint64_t stat_time_frontier_edge_q_access_start;
uint64_t stat_time_frontier_edge_q_access_acc = 0;
uint64_t stat_time_node_distance_q_access_start;
uint64_t stat_time_node_distance_q_access_acc = 0;
uint64_t stat_time_misc_start;
uint64_t stat_time_misc_acc = 0;
uint64_t stat_time_reporter_start;

/**
 * Report program statistics
 * Called just before program termination.
 */
void report_stats() {
  LOG(NOTICE, "Max frontier edge queue size: %d items", stat_max_frontier_edge_q_size); 
  LOG(NOTICE, "Max frontier node queue size: %d items", stat_max_frontier_node_q_size);
  LOG(NOTICE, "Max node distance queue size: %d items", stat_max_node_distance_q_size);
  LOG(NOTICE, "Total time for algorithm execution: %f seconds", Cycles::toSeconds(stat_time_alg_acc));
  LOG(NOTICE, "Total time for program termination: %f seconds", Cycles::toSeconds(stat_time_term_acc));
  LOG(NOTICE, "Total time for reading graph edges: %f seconds", Cycles::toSeconds(stat_time_read_acc));
  LOG(NOTICE, "Total time for writing node distances: %f seconds", Cycles::toSeconds(stat_time_write_acc));
  LOG(NOTICE, "Total bytes read from ramcloud: %lu bytes", stat_bytes_read_acc);
  LOG(NOTICE, "Total nodes read from ramcloud: %lu nodes", stat_nodes_read_acc);
  LOG(NOTICE, "Average bytes read per node: %lu bytes/node", stat_bytes_read_acc/stat_nodes_read_acc);
  LOG(NOTICE, "Average time per node read: %lu microseconds", Cycles::toMicroseconds(stat_time_read_acc / stat_nodes_read_acc));
  LOG(NOTICE, "Average time for distance write: %lu microseconds", Cycles::toMicroseconds(stat_time_write_acc / stat_nodes_write_acc));
  LOG(NOTICE, "Total time spent waiting on frontier node queue: %f seconds", Cycles::toSeconds(stat_time_frontier_node_q_wait_acc));
  LOG(NOTICE, "Total time spent waiting on frontier edge queue: %f seconds", Cycles::toSeconds(stat_time_frontier_edge_q_wait_acc));
  LOG(NOTICE, "Total time spent waiting on node distance queue: %f seconds", Cycles::toSeconds(stat_time_node_distance_q_wait_acc));
  LOG(NOTICE, "Total time spent enqueueing in frontier node queue: %f seconds", Cycles::toSeconds(stat_time_frontier_node_q_enqueue_acc));
  LOG(NOTICE, "Total time spent enqueueing in frontier edge queue: %f seconds", Cycles::toSeconds(stat_time_frontier_edge_q_enqueue_acc));
  LOG(NOTICE, "Total time spent enqueueing in node distance queue: %f seconds", Cycles::toSeconds(stat_time_node_distance_q_enqueue_acc));
  LOG(NOTICE, "Total time spent accessing frontier node queue: %f seconds", Cycles::toSeconds(stat_time_frontier_node_q_access_acc));
  LOG(NOTICE, "Total time spent accessing frontier edge queue: %f seconds", Cycles::toSeconds(stat_time_frontier_edge_q_access_acc));
  LOG(NOTICE, "Total time spent accessing node distance queue: %f seconds", Cycles::toSeconds(stat_time_node_distance_q_access_acc));  
  LOG(NOTICE, "Total time for edge traversal: %f seconds", Cycles::toSeconds(stat_time_edge_trav_acc));
  LOG(NOTICE, "Total time for edge traversal minus enqueue time: %f seconds", Cycles::toSeconds(stat_time_edge_trav_acc - stat_time_frontier_node_q_enqueue_acc - stat_time_node_distance_q_enqueue_acc)); 
  LOG(NOTICE, "Total time doing misc: %f seconds", Cycles::toSeconds(stat_time_misc_acc));
}

/**
 * Main function of the reporter thread
 * Reports some stats in fixed intervals of time.
 */
void reporter() {
  stat_time_reporter_start = Cycles::rdtsc();
  while(true) {
    sleep(REPORTER_NAP_TIME);
    if(terminate)
      return;
    LOG(NOTICE, "-------------------- Reporter Output Start --------------------");
    report_stats();
    LOG(NOTICE, "Reporter time: %f seconds", Cycles::toSeconds(Cycles::rdtsc() - stat_time_reporter_start));
    LOG(NOTICE, "-------------------- Reporter Output Stop --------------------");
  }
}

/**
 * Main function of the reader thread
 * Reads nodes out of RAMCloud as fast as possible.
 */
void reader() {
  uint64_t graph_tableid;
  RamCloud client(COORDINATOR_LOCATION);
  
  graph_tableid = client.getTableId(GRAPH_TABLE_NAME);
  
  string node;
  uint64_t node_dist;
  Buffer rc_read_buf;
  AdjacencyList adj_list_pb;
  uint32_t buf_len;
  size_t batch_size;
  boost::tuple<string,uint64_t> read_q[MULTIREAD_REQ_MAX];    
  MultiReadObject* requests[MULTIREAD_REQ_MAX];
  while(true) {
    Tub<Buffer> values[MULTIREAD_REQ_MAX];
    Tub<MultiReadObject> objects[MULTIREAD_REQ_MAX];

    stat_time_frontier_node_q_access_start = Cycles::rdtsc();
    {
      boost::system_time timeout = boost::get_system_time() + boost::posix_time::milliseconds(1000);
      boost::unique_lock<boost::mutex> lock(frontier_node_q_mutex);
      
      stat_time_frontier_node_q_wait_start = Cycles::rdtsc();
      while(frontier_node_q.empty()) {
        if(!frontier_node_q_condvar.timed_wait(lock, timeout)) {
          if(terminate)
            return;
          else
            timeout = boost::get_system_time() + boost::posix_time::milliseconds(1000);
        }
      }
      stat_time_frontier_node_q_wait_acc += Cycles::rdtsc() - stat_time_frontier_node_q_wait_start;

      batch_size = std::min(frontier_node_q.size(), MULTIREAD_REQ_MAX);
      for(int i = 0; i < batch_size; i++) {
        read_q[i] = frontier_node_q.front();
        frontier_node_q.pop();
      }
    }
    stat_time_frontier_node_q_access_acc += Cycles::rdtsc() - stat_time_frontier_node_q_access_start;

    for(int i = 0; i < batch_size; i++) {
      objects[i].construct( graph_tableid, 
                            read_q[i].get<0>().c_str(), 
                            read_q[i].get<0>().length(),
                            &values[i] );
      requests[i] = objects[i].get();
    }

    stat_time_read_start = Cycles::rdtsc(); 
    try {
      client.multiRead(requests, batch_size);
    } catch(RAMCloud::ClientException& e) {
      continue;
    }
    stat_time_read_acc += Cycles::rdtsc() - stat_time_read_start;

    {
      boost::system_time timeout = boost::get_system_time() + boost::posix_time::milliseconds(1000);
      boost::unique_lock<boost::mutex> lock(frontier_edge_q_mutex);

      while(frontier_edge_q.size() > FRONTIER_EDGE_Q_SIZE_MAX) {
        if(!frontier_edge_q_condvar.timed_wait(lock, timeout)) {
          if(terminate)
            return;
          else
            timeout = boost::get_system_time() + boost::posix_time::milliseconds(1000);
        }
      }
    
      stat_time_frontier_edge_q_enqueue_start = Cycles::rdtsc(); 
      for(int i = 0; i < batch_size; i++) {
        if(values[i].get()) {
          stat_nodes_read_acc++;
          
          buf_len = values[i].get()->getTotalLength();
          adj_list_pb.ParseFromArray(values[i].get()->getRange(0, buf_len), buf_len);
          stat_bytes_read_acc += buf_len;
           
          frontier_edge_q.push(boost::tuple<string,AdjacencyList,uint64_t>(read_q[i].get<0>(), adj_list_pb, read_q[i].get<1>()));
        }
      }
      stat_max_frontier_edge_q_size = std::max(stat_max_frontier_edge_q_size, frontier_edge_q.size());
      frontier_edge_q_condvar.notify_all();
      stat_time_frontier_edge_q_enqueue_acc += Cycles::rdtsc() - stat_time_frontier_edge_q_enqueue_start; 
    }
  }
}

/**
 * Main function of the writer thread
 * Writes distance value to RAMCloud as fast as possible.
 */
void writer() {
  uint64_t dist_tableid;
  RamCloud client(COORDINATOR_LOCATION);
 
  dist_tableid = client.getTableId(DIST_TABLE_NAME);
  
  string node;
  string distance;
  size_t batch_size;
  boost::tuple<string,string> write_q[MULTIWRITE_REQ_MAX];    
  MultiWriteObject* requests[MULTIWRITE_REQ_MAX];
  while(true) {
    Tub<MultiWriteObject> objects[MULTIWRITE_REQ_MAX];

    stat_time_node_distance_q_access_start = Cycles::rdtsc();
    {
      boost::system_time timeout = boost::get_system_time() + boost::posix_time::milliseconds(1000);
      boost::unique_lock<boost::mutex> lock(node_distance_q_mutex);

      stat_time_node_distance_q_wait_start = Cycles::rdtsc();
      while(node_distance_q.empty()) {
        if(!node_distance_q_condvar.timed_wait(lock, timeout)) {
          if(terminate)
            return;
          else
            timeout = boost::get_system_time() + boost::posix_time::milliseconds(1000);
        }
      }
      stat_time_node_distance_q_wait_acc += Cycles::rdtsc() - stat_time_node_distance_q_wait_start;
      
      batch_size = std::min(node_distance_q.size(), MULTIWRITE_REQ_MAX);
      for(int i = 0; i < batch_size; i++) {
        write_q[i] = node_distance_q.front();
        node_distance_q.pop();
      }
    }
    stat_time_node_distance_q_access_acc += Cycles::rdtsc() - stat_time_node_distance_q_access_start;

    for(int i = 0; i < batch_size; i++) {
      objects[i].construct( dist_tableid, 
                            write_q[i].get<0>().c_str(), 
                            write_q[i].get<0>().length(),
                            write_q[i].get<1>().c_str(),
                            write_q[i].get<1>().length() );
      requests[i] = objects[i].get();
    }

    stat_time_write_start = Cycles::rdtsc();
    try {
      client.multiWrite(requests, batch_size);
      //client.write( dist_tableid,
      //              node.c_str(),
      //              node.length(),
      //              distance.c_str() );
    } catch(RAMCloud::ClientException& e) {
      fprintf(stderr, "RAMCloud exception: %s\n", e.str().c_str());
      return;
    }
    stat_time_write_acc += Cycles::rdtsc() - stat_time_write_start;
    stat_nodes_write_acc += batch_size;
  }
}

int main(int argc, char* argv[]) {
  GOOGLE_PROTOBUF_VERIFY_VERSION;

  stat_time_misc_start = Cycles::rdtsc();

  boost::dynamic_bitset<> seen_list(boost::lexical_cast<boost::dynamic_bitset<>::size_type>(argv[2]));
  string source(argv[1]);
  
  boost::thread reader_thread(reader);
  boost::thread writer_thread(writer);
  boost::thread reporter_thread(reporter);

  seen_list.set(boost::lexical_cast<boost::dynamic_bitset<>::size_type>(source));

  stat_time_misc_acc += Cycles::rdtsc() - stat_time_misc_start;

  stat_time_alg_start = Cycles::rdtsc();
  {
    boost::lock_guard<boost::mutex> lock(node_distance_q_mutex);
    node_distance_q.push(boost::tuple<string,string>(source, boost::lexical_cast<string>(0)));
    node_distance_q_condvar.notify_all();
  }

  {
    boost::lock_guard<boost::mutex> lock(frontier_node_q_mutex);
    frontier_node_q.push(boost::tuple<string,uint64_t>(source, 0));
    frontier_node_q_condvar.notify_all();
  }

  string node;
  uint64_t node_dist;
  AdjacencyList adj_list_pb;
  size_t batch_size;
  boost::tuple<string,AdjacencyList,uint64_t> edge_batch_q[EDGE_BATCH_MAX];  
  while(true) {
    stat_time_alg_acc += Cycles::rdtsc() - stat_time_alg_start;
    stat_time_alg_start = Cycles::rdtsc();
    
    stat_time_frontier_edge_q_access_start = Cycles::rdtsc();
    {
      stat_time_term_start = Cycles::rdtsc();
      boost::system_time const timeout = boost::get_system_time() + boost::posix_time::milliseconds(1000);
      boost::unique_lock<boost::mutex> lock(frontier_edge_q_mutex);

      stat_time_frontier_edge_q_wait_start = Cycles::rdtsc();
      while(frontier_edge_q.empty()) {
        if(!frontier_edge_q_condvar.timed_wait(lock, timeout)) {
          terminate = true;
          reader_thread.join();
          writer_thread.join();
          reporter_thread.join();
          stat_time_term_acc += Cycles::rdtsc() - stat_time_term_start;
          report_stats();
          return 0;
        }
      }
      stat_time_frontier_edge_q_wait_acc += Cycles::rdtsc() - stat_time_frontier_edge_q_wait_start;

      batch_size = std::min(frontier_edge_q.size(), EDGE_BATCH_MAX);
      for(int i = 0; i < batch_size; i++) {
        edge_batch_q[i] = frontier_edge_q.front();
        frontier_edge_q.pop();
      }
 
      //node = frontier_edge_q.front().get<0>();
      //adj_list_pb = frontier_edge_q.front().get<1>();
      //node_dist = frontier_edge_q.front().get<2>();
      //frontier_edge_q.pop();
    }
    stat_time_frontier_edge_q_access_acc += Cycles::rdtsc() - stat_time_frontier_edge_q_access_start;

    stat_time_edge_trav_start = Cycles::rdtsc();    
    for(int i = 0; i < batch_size; i++) {
      node = edge_batch_q[i].get<0>();
      adj_list_pb = edge_batch_q[i].get<1>();
      node_dist = edge_batch_q[i].get<2>();
      for(int j = 0; j < adj_list_pb.neighbor_size(); j++) {
        uint64_t neighbor = adj_list_pb.neighbor(j);
        if(!seen_list[neighbor]) {
          seen_list.set(neighbor);

          string neighbor_str = boost::lexical_cast<string>(neighbor);

          stat_time_node_distance_q_enqueue_start = Cycles::rdtsc();
          {
            boost::lock_guard<boost::mutex> lock(node_distance_q_mutex);
            node_distance_q.push(boost::tuple<string,string>(neighbor_str, boost::lexical_cast<string>(node_dist+1)));
            stat_max_node_distance_q_size = std::max(stat_max_node_distance_q_size, node_distance_q.size());
            node_distance_q_condvar.notify_all();
          }
          stat_time_node_distance_q_enqueue_acc += Cycles::rdtsc() - stat_time_node_distance_q_enqueue_start;

          stat_time_frontier_node_q_enqueue_start = Cycles::rdtsc();
          {
            boost::lock_guard<boost::mutex> lock(frontier_node_q_mutex);
            frontier_node_q.push(boost::tuple<string,uint64_t>(neighbor_str, node_dist+1));
            stat_max_frontier_node_q_size = std::max(stat_max_frontier_node_q_size, frontier_node_q.size());
            frontier_node_q_condvar.notify_all();
          } 
          stat_time_frontier_node_q_enqueue_acc += Cycles::rdtsc() - stat_time_frontier_node_q_enqueue_start;
        }
      }
    }
    stat_time_edge_trav_acc += Cycles::rdtsc() - stat_time_edge_trav_start; 
  }
  
  return 0;
}
