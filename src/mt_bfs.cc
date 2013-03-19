#include <boost/algorithm/string.hpp>
#include <boost/dynamic_bitset.hpp>
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

using namespace RAMCloud;

boost::condition_variable frontier_edge_q_condvar;
boost::condition_variable frontier_node_q_condvar;
boost::condition_variable node_distance_q_condvar;
boost::mutex frontier_edge_q_mutex;
boost::mutex frontier_node_q_mutex;
boost::mutex node_distance_q_mutex;
std::queue<pair<string,string>> frontier_edge_q;
std::queue<string> frontier_node_q;
std::queue<pair<string,string>> node_distance_q;

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
uint64_t stat_time_misc_start;
uint64_t stat_time_misc_acc = 0;

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
  LOG(NOTICE, "Total time for edge traversal: %f seconds", Cycles::toSeconds(stat_time_edge_trav_acc));
  LOG(NOTICE, "Total time for edge traversal minus enqueue time: %f seconds", Cycles::toSeconds(stat_time_edge_trav_acc - stat_time_frontier_node_q_enqueue_acc - stat_time_node_distance_q_enqueue_acc)); 
  LOG(NOTICE, "Total time doing misc: %f seconds", Cycles::toSeconds(stat_time_misc_acc));
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
  Buffer rc_read_buf;
  string edge_list;
  uint32_t buf_len;
  while(true) {
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
      
      node = frontier_node_q.front();
      frontier_node_q.pop();
    }
   
    stat_time_read_start = Cycles::rdtsc(); 
    try {
      client.read(  graph_tableid,
                    node.c_str(),
                    node.length(),
                    &rc_read_buf );
    } catch(RAMCloud::ClientException& e) {
      continue;
    }
    stat_time_read_acc += Cycles::rdtsc() - stat_time_read_start;
    stat_nodes_read_acc++;

    buf_len = rc_read_buf.getTotalLength();
    edge_list = string(static_cast<const char*>(rc_read_buf.getRange(0, buf_len)), buf_len);

    stat_bytes_read_acc += buf_len;
   
    stat_time_frontier_edge_q_enqueue_start = Cycles::rdtsc(); 
    {
      boost::lock_guard<boost::mutex> lock(frontier_edge_q_mutex);
      frontier_edge_q.push(pair<string,string>(node, edge_list));
      stat_max_frontier_edge_q_size = std::max(stat_max_frontier_edge_q_size, frontier_edge_q.size());
      frontier_edge_q_condvar.notify_all();
    }
    stat_time_frontier_edge_q_enqueue_acc += Cycles::rdtsc() - stat_time_frontier_edge_q_enqueue_start;
  }
}

/**
 * Main function of the writer thread
 * Writes distance value to RAMCloud as fast as possible.
 */
void writer() {
  uint64_t dist_tableid;
  RamCloud client(COORDINATOR_LOCATION);
 
  client.dropTable(DIST_TABLE_NAME);
  client.createTable(DIST_TABLE_NAME);

  dist_tableid = client.getTableId(DIST_TABLE_NAME);
  
  string node;
  string distance;
  while(true) {
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

      node = node_distance_q.front().first;
      distance = node_distance_q.front().second;
      node_distance_q.pop();
    }

    stat_time_write_start = Cycles::rdtsc();
    try {
      client.write( dist_tableid,
                    node.c_str(),
                    node.length(),
                    distance.c_str() );
    } catch(RAMCloud::ClientException& e) {
      fprintf(stderr, "RAMCloud exception: %s\n", e.str().c_str());
      return;
    }
    stat_time_write_acc += Cycles::rdtsc() - stat_time_write_start;
    stat_nodes_write_acc++;
  }
}

int main(int argc, char* argv[]) {
  //std::set<string> seen_list;
  boost::dynamic_bitset<> seen_list(boost::lexical_cast<boost::dynamic_bitset<>::size_type>(argv[2]));
  std::map<string,uint64_t> distance_map;
  string source(argv[1]);
  boost::thread reader_thread(reader);
  boost::thread writer_thread(writer);
  
  distance_map[source] = 0;
  seen_list.set(boost::lexical_cast<boost::dynamic_bitset<>::size_type>(source));

  stat_time_alg_start = Cycles::rdtsc();
  {
    boost::lock_guard<boost::mutex> lock(node_distance_q_mutex);
    node_distance_q.push(pair<string,string>(source, boost::lexical_cast<string>(0)));
    node_distance_q_condvar.notify_all();
  }

  {
    boost::lock_guard<boost::mutex> lock(frontier_node_q_mutex);
    frontier_node_q.push(source);
    frontier_node_q_condvar.notify_all();
  }

  string node;
  uint64_t node_dist;
  string edge_list_str;
  std::vector<string> edge_list_vec; 
  while(true) {
    stat_time_alg_acc += Cycles::rdtsc() - stat_time_alg_start;
    stat_time_alg_start = Cycles::rdtsc();
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
          stat_time_term_acc += Cycles::rdtsc() - stat_time_term_start;
          report_stats();
          return 0;
        }
      }
      stat_time_frontier_edge_q_wait_acc += Cycles::rdtsc() - stat_time_frontier_edge_q_wait_start;

      node = frontier_edge_q.front().first;
      edge_list_str = frontier_edge_q.front().second;
      frontier_edge_q.pop();
    }

    //node_dist = distance_map[node];
    //distance_map.erase(node);
    node_dist = 0;    

    boost::split(edge_list_vec, edge_list_str, boost::is_any_of(" "));

    stat_time_edge_trav_start = Cycles::rdtsc();    
    for(std::vector<string>::iterator it = edge_list_vec.begin(); it != edge_list_vec.end(); ++it) {
      
      if(!seen_list[boost::lexical_cast<boost::dynamic_bitset<>::size_type>(*it)]) {
        //distance_map[*it] = node_dist+1;
        seen_list.set(boost::lexical_cast<boost::dynamic_bitset<>::size_type>(*it));


        stat_time_node_distance_q_enqueue_start = Cycles::rdtsc();
        {
          boost::lock_guard<boost::mutex> lock(node_distance_q_mutex);
          node_distance_q.push(pair<string,string>(*it, boost::lexical_cast<string>(node_dist+1)));
          stat_max_node_distance_q_size = std::max(stat_max_node_distance_q_size, node_distance_q.size());
          node_distance_q_condvar.notify_all();
        }
        stat_time_node_distance_q_enqueue_acc += Cycles::rdtsc() - stat_time_node_distance_q_enqueue_start;

        stat_time_frontier_node_q_enqueue_start = Cycles::rdtsc();
        {
          boost::lock_guard<boost::mutex> lock(frontier_node_q_mutex);
          frontier_node_q.push(*it);
          stat_max_frontier_node_q_size = std::max(stat_max_frontier_node_q_size, frontier_node_q.size());
          frontier_node_q_condvar.notify_all();
        } 
        stat_time_frontier_node_q_enqueue_acc += Cycles::rdtsc() - stat_time_frontier_node_q_enqueue_start;
      }
    }
    stat_time_edge_trav_acc += Cycles::rdtsc() - stat_time_edge_trav_start; 
  }
  
  return 0;
}
