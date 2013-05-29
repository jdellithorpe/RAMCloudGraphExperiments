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
#include "SpinLock.h"
#include "sandy.h"
#include "mt_bfs.h"
#include "adjacency_list.pb.h"

using namespace RAMCloud;
using namespace Sandy::ProtoBuf;

boost::condition_variable frontier_edge_q_condvar;
boost::condition_variable frontier_node_q_condvar;
boost::condition_variable node_distance_q_condvar;
boost::mutex frontier_edge_q_mutex;
boost::mutex frontier_node_q_mutex;
boost::mutex node_distance_q_mutex;
SpinLock frontier_edge_q_lock;
SpinLock frontier_node_q_lock;
SpinLock node_distance_q_lock;
std::queue<boost::tuple<string,uint64_t>> frontier_edge_q;
std::queue<boost::tuple<uint64_t,uint64_t>> frontier_node_q;
std::queue<boost::tuple<uint64_t,uint64_t>> node_distance_q;

/// Queue ticket numbers
uint64_t frontier_node_q_dequeue_ticket_number = 0;
uint64_t frontier_edge_q_enqueue_ticket_number = 0;

/// Global program termination bit
bool terminate = false;

/// Termination indicators for threads
bool reader_terminated = false;
bool writer_terminated = false;

/// Stat globals
size_t stat_max_frontier_edge_q_size = 0;
size_t stat_max_frontier_node_q_size = 0;
size_t stat_max_node_distance_q_size = 0;
uint64_t stat_time_alg_start;
uint64_t stat_time_alg_acc = 0;
uint64_t stat_time_thread_join_start;
uint64_t stat_time_thread_join_acc = 0;
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
uint64_t stat_time_misc6_start;
uint64_t stat_time_misc6_acc = 0;
uint64_t stat_time_misc7_start;
uint64_t stat_time_misc7_acc = 0;
uint64_t stat_time_reporter_start;

/**
 * Report program statistics
 * Called just before program termination.
 */
void report_stats() {
  LOG(NOTICE, "Max frontier edge queue size: %d items", stat_max_frontier_edge_q_size); 
  LOG(NOTICE, "Cur frontier edge queue size: %d items", frontier_edge_q.size()); 
  LOG(NOTICE, "Max frontier node queue size: %d items", stat_max_frontier_node_q_size);
  LOG(NOTICE, "Cur frontier node queue size: %d items", frontier_node_q.size());
  LOG(NOTICE, "Max node distance queue size: %d items", stat_max_node_distance_q_size);
  LOG(NOTICE, "Cur node distance queue size: %d items", node_distance_q.size());
  LOG(NOTICE, "Total time for algorithm execution: %f seconds", Cycles::toSeconds(stat_time_alg_acc));
  LOG(NOTICE, "Total time for program termination: %f seconds", Cycles::toSeconds(stat_time_term_acc));
  LOG(NOTICE, "Total time for thread join: %f seconds", Cycles::toSeconds(stat_time_thread_join_acc));
  LOG(NOTICE, "Total time for reading graph edges: %f seconds", Cycles::toSeconds(stat_time_read_acc));
  LOG(NOTICE, "Total time for writing node distances: %f seconds", Cycles::toSeconds(stat_time_write_acc));
  LOG(NOTICE, "Total bytes read from ramcloud: %lu bytes", stat_bytes_read_acc);
  LOG(NOTICE, "Total nodes read from ramcloud: %lu nodes", stat_nodes_read_acc);
  LOG(NOTICE, "Total dist written to ramcloud: %lu dists", stat_nodes_write_acc);
  LOG(NOTICE, "Average bytes read per node: %lu bytes/node", stat_bytes_read_acc/stat_nodes_read_acc);
  LOG(NOTICE, "Average time per node read: %f microseconds", Cycles::toSeconds(stat_time_read_acc) / (float)stat_nodes_read_acc * 1000000.0);
  LOG(NOTICE, "Average time per node proc: %f microseconds", Cycles::toSeconds(stat_time_alg_acc) / (float)stat_nodes_read_acc * 1000000.0);
  LOG(NOTICE, "Average time for distance write: %f microseconds", Cycles::toSeconds(stat_time_write_acc) / (float)stat_nodes_write_acc * 1000000.0);
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
  LOG(NOTICE, "Total time doing misc1: %f seconds", Cycles::toSeconds(stat_time_misc1_acc));
  LOG(NOTICE, "Total time doing misc2: %f seconds", Cycles::toSeconds(stat_time_misc2_acc));
  LOG(NOTICE, "Total time doing misc3: %f seconds", Cycles::toSeconds(stat_time_misc3_acc));
  LOG(NOTICE, "Total time doing misc4: %f seconds", Cycles::toSeconds(stat_time_misc4_acc));
  LOG(NOTICE, "Total time doing misc5: %f seconds", Cycles::toSeconds(stat_time_misc5_acc));
  LOG(NOTICE, "Total time doing misc6: %f seconds", Cycles::toSeconds(stat_time_misc6_acc));
  LOG(NOTICE, "Total time doing misc7: %f seconds", Cycles::toSeconds(stat_time_misc7_acc));
  LOG(NOTICE, "Program termination signal: %d", terminate);
  LOG(NOTICE, "Reader thread terminated  : %d", reader_terminated);
  LOG(NOTICE, "Writer thread terminated  : %d", writer_terminated);
}

/**
 * Main function of the reporter thread
 * Reports some stats in fixed intervals of time.
 */
void reporter() {
  stat_time_reporter_start = Cycles::rdtsc();
  while(true) {
    sleep(REPORTER_NAP_TIME);
    if(terminate && reader_terminated && writer_terminated)
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
 
  uint64_t queue_ticket_number; 
  uint64_t node;
  uint64_t node_dist;
  Buffer rc_read_buf;
  AdjacencyList adj_list_pb;
  uint32_t buf_len;
  size_t batch_size;
  boost::tuple<uint64_t,uint64_t> read_q[MULTIREAD_REQ_MAX];    
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
          if(terminate) {
            reader_terminated = true;
            return;
          } else
            timeout = boost::get_system_time() + boost::posix_time::milliseconds(1000);
        }
      }
      stat_time_frontier_node_q_wait_acc += Cycles::rdtsc() - stat_time_frontier_node_q_wait_start;

      queue_ticket_number = frontier_node_q_dequeue_ticket_number;
      frontier_node_q_dequeue_ticket_number++;

      stat_time_misc1_start = Cycles::rdtsc();
      batch_size = std::min(frontier_node_q.size(), MULTIREAD_REQ_MAX);
      for(int i = 0; i < batch_size; i++) {
        read_q[i] = frontier_node_q.front();
        frontier_node_q.pop();
      }
      stat_time_misc1_acc += Cycles::rdtsc() - stat_time_misc1_start;
    }
    stat_time_frontier_node_q_access_acc += Cycles::rdtsc() - stat_time_frontier_node_q_access_start;
    
    for(int i = 0; i < batch_size; i++) {
      objects[i].construct( graph_tableid, 
                            (char*)&read_q[i].get<0>(), 
                            sizeof(uint64_t),
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

    while(true) {
      std::lock_guard<SpinLock> guard(frontier_edge_q_lock);
      if((frontier_edge_q.size() >= FRONTIER_EDGE_Q_SIZE_MAX) || (frontier_edge_q_enqueue_ticket_number != queue_ticket_number))
        continue;
      
      stat_time_frontier_edge_q_enqueue_start = Cycles::rdtsc(); 
      for(int i = 0; i < batch_size; i++) {
        if(values[i].get()) {

          buf_len = values[i].get()->getTotalLength();
          frontier_edge_q.push(boost::tuple<string,uint64_t>(string(static_cast<const char*>(values[i].get()->getRange(0, buf_len)), buf_len), read_q[i].get<1>()));

          stat_nodes_read_acc++;
          stat_bytes_read_acc += buf_len;
        }
      }

      stat_max_frontier_edge_q_size = std::max(stat_max_frontier_edge_q_size, frontier_edge_q.size());
      frontier_edge_q_enqueue_ticket_number++;
      stat_time_frontier_edge_q_enqueue_acc += Cycles::rdtsc() - stat_time_frontier_edge_q_enqueue_start; 
      
      break; 
    }
  }
}

void reader_sidekick() {
  uint64_t graph_tableid;
  RamCloud client(COORDINATOR_LOCATION);
  
  graph_tableid = client.getTableId(GRAPH_TABLE_NAME);
 
  uint64_t queue_ticket_number; 
  uint64_t node;
  uint64_t node_dist;
  Buffer rc_read_buf;
  AdjacencyList adj_list_pb;
  uint32_t buf_len;
  size_t batch_size;
  boost::tuple<uint64_t,uint64_t> read_q[MULTIREAD_REQ_MAX];    
  MultiReadObject* requests[MULTIREAD_REQ_MAX];
  while(true) {
    Tub<Buffer> values[MULTIREAD_REQ_MAX];
    Tub<MultiReadObject> objects[MULTIREAD_REQ_MAX];

    {
      boost::system_time timeout = boost::get_system_time() + boost::posix_time::milliseconds(1000);
      boost::unique_lock<boost::mutex> lock(frontier_node_q_mutex);
      
      while(frontier_node_q.empty()) {
        if(!frontier_node_q_condvar.timed_wait(lock, timeout)) {
          if(terminate) {
            return;
          } else
            timeout = boost::get_system_time() + boost::posix_time::milliseconds(1000);
        }
      }

      queue_ticket_number = frontier_node_q_dequeue_ticket_number;
      frontier_node_q_dequeue_ticket_number++;

      batch_size = std::min(frontier_node_q.size(), MULTIREAD_REQ_MAX);
      for(int i = 0; i < batch_size; i++) {
        read_q[i] = frontier_node_q.front();
        frontier_node_q.pop();
      }
    }
    
    for(int i = 0; i < batch_size; i++) {
      objects[i].construct( graph_tableid, 
                            (char*)&read_q[i].get<0>(), 
                            sizeof(uint64_t),
                            &values[i] );
      requests[i] = objects[i].get();
    }

    try {
      client.multiRead(requests, batch_size);
    } catch(RAMCloud::ClientException& e) {
      continue;
    }
    
    while(true) {
      std::lock_guard<SpinLock> guard(frontier_edge_q_lock);
      if((frontier_edge_q.size() >= FRONTIER_EDGE_Q_SIZE_MAX) || (frontier_edge_q_enqueue_ticket_number != queue_ticket_number))
        continue;
      
      stat_time_frontier_edge_q_enqueue_start = Cycles::rdtsc(); 
      for(int i = 0; i < batch_size; i++) {
        if(values[i].get()) {

          buf_len = values[i].get()->getTotalLength();
          frontier_edge_q.push(boost::tuple<string,uint64_t>(string(static_cast<const char*>(values[i].get()->getRange(0, buf_len)), buf_len), read_q[i].get<1>()));

          stat_nodes_read_acc++;
          stat_bytes_read_acc += buf_len;
        }
      }

      stat_max_frontier_edge_q_size = std::max(stat_max_frontier_edge_q_size, frontier_edge_q.size());
      frontier_edge_q_enqueue_ticket_number++;
      stat_time_frontier_edge_q_enqueue_acc += Cycles::rdtsc() - stat_time_frontier_edge_q_enqueue_start; 
      
      break; 
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
  
  uint64_t node;
  uint64_t distance;
  size_t batch_size;
  boost::tuple<uint64_t,uint64_t> write_q[MULTIWRITE_REQ_MAX];    
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
          if(terminate) {
            writer_terminated = true;
            return;
          } else
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
                            (char*)&write_q[i].get<0>(), 
                            sizeof(uint64_t),
                            (char*)&write_q[i].get<1>(),
                            sizeof(uint64_t) );
      requests[i] = objects[i].get();
    }

    stat_time_write_start = Cycles::rdtsc();
    try {
      client.multiWrite(requests, batch_size);
    } catch(RAMCloud::ClientException& e) {
      fprintf(stderr, "RAMCloud exception: %s\n", e.str().c_str());
      return;
    }
    stat_time_write_acc += Cycles::rdtsc() - stat_time_write_start;
    stat_nodes_write_acc += batch_size;
  }
}

void writer_sidekick() {
  uint64_t dist_tableid;
  RamCloud client(COORDINATOR_LOCATION);
 
  dist_tableid = client.getTableId(DIST_TABLE_NAME);
  
  uint64_t node;
  uint64_t distance;
  size_t batch_size;
  boost::tuple<uint64_t,uint64_t> write_q[MULTIWRITE_REQ_MAX];    
  MultiWriteObject* requests[MULTIWRITE_REQ_MAX];
  while(true) {
    Tub<MultiWriteObject> objects[MULTIWRITE_REQ_MAX];

    {
      boost::system_time timeout = boost::get_system_time() + boost::posix_time::milliseconds(1000);
      boost::unique_lock<boost::mutex> lock(node_distance_q_mutex);

      while(node_distance_q.empty()) {
        if(!node_distance_q_condvar.timed_wait(lock, timeout)) {
          if(terminate) {
            return;
          } else
            timeout = boost::get_system_time() + boost::posix_time::milliseconds(1000);
        }
      }
      
      batch_size = std::min(node_distance_q.size(), MULTIWRITE_REQ_MAX);
      for(int i = 0; i < batch_size; i++) {
        write_q[i] = node_distance_q.front();
        node_distance_q.pop();
      }
    }

    for(int i = 0; i < batch_size; i++) {
      objects[i].construct( dist_tableid, 
                            (char*)&write_q[i].get<0>(), 
                            sizeof(uint64_t),
                            (char*)&write_q[i].get<1>(),
                            sizeof(uint64_t) );
      requests[i] = objects[i].get();
    }

    try {
      client.multiWrite(requests, batch_size);
    } catch(RAMCloud::ClientException& e) {
      fprintf(stderr, "RAMCloud exception: %s\n", e.str().c_str());
      return;
    }
    stat_nodes_write_acc += batch_size;
  }
}


int main(int argc, char* argv[]) {
  GOOGLE_PROTOBUF_VERIFY_VERSION;

  boost::dynamic_bitset<> seen_list(boost::lexical_cast<boost::dynamic_bitset<>::size_type>(argv[2]));

  uint64_t source = boost::lexical_cast<uint64_t>(argv[1]);
  
  boost::thread reader_thread(reader);
  boost::thread reader_sidekick_1_thread(reader_sidekick);
  boost::thread reader_sidekick_2_thread(reader_sidekick);
  //boost::thread reader_sidekick_3_thread(reader_sidekick);
  boost::thread writer_thread(writer);
  boost::thread writer_sidekick_1_thread(writer_sidekick);
  //boost::thread writer_sidekick_2_thread(writer_sidekick);
  boost::thread reporter_thread(reporter);

  seen_list.set(boost::lexical_cast<boost::dynamic_bitset<>::size_type>(source));

  stat_time_alg_start = Cycles::rdtsc();
  {
    boost::lock_guard<boost::mutex> lock(node_distance_q_mutex);
    node_distance_q.push(boost::tuple<uint64_t,uint64_t>(source, 0));
    node_distance_q_condvar.notify_all();
  }

  {
    boost::lock_guard<boost::mutex> lock(frontier_node_q_mutex);
    frontier_node_q.push(boost::tuple<uint64_t,uint64_t>(source, 0));
    frontier_node_q_condvar.notify_all();
  }

  uint64_t node_dist;
  AdjacencyList adj_list_pb;
  uint64_t* adj_list_intarray;
  size_t batch_size;
  boost::tuple<string,uint64_t> edge_batch_q[EDGE_BATCH_MAX];  
  while(true) {
    stat_time_alg_acc += Cycles::rdtsc() - stat_time_alg_start;
    stat_time_alg_start = Cycles::rdtsc();
  
    const uint64_t timeout = Cycles::rdtsc() + Cycles::fromSeconds(1);

    stat_time_frontier_edge_q_access_start = Cycles::rdtsc();
    while(true) { 
      std::lock_guard<SpinLock> guard(frontier_edge_q_lock);
      if(frontier_edge_q.empty()) {
        if(Cycles::rdtsc() > timeout) {
          terminate = true;
          stat_time_thread_join_start = Cycles::rdtsc();
          reader_thread.join();
          reader_sidekick_1_thread.join();
          reader_sidekick_2_thread.join();
          writer_thread.join();
          writer_sidekick_1_thread.join();
          reporter_thread.join();
          stat_time_thread_join_acc += Cycles::rdtsc() - stat_time_thread_join_start;
          report_stats();
          return 0;
        }
        continue;
      }   
       
      batch_size = std::min(frontier_edge_q.size(), EDGE_BATCH_MAX);
      for(int i = 0; i < batch_size; i++) {
        edge_batch_q[i] = frontier_edge_q.front();
        frontier_edge_q.pop();
      }      
 
      break;
    }
    stat_time_frontier_edge_q_access_acc += Cycles::rdtsc() - stat_time_frontier_edge_q_access_start;
  
    stat_time_edge_trav_start = Cycles::rdtsc();    
    {
      stat_time_misc3_start = Cycles::rdtsc();
      boost::lock_guard<boost::mutex> ndq_lock(node_distance_q_mutex);
      boost::lock_guard<boost::mutex> fnq_lock(frontier_node_q_mutex);
      stat_time_misc3_acc += Cycles::rdtsc() - stat_time_misc3_start;      

      stat_time_misc4_start = Cycles::rdtsc();
      for(int i = 0; i < batch_size; i++) {
        stat_time_misc5_start = Cycles::rdtsc();
        adj_list_intarray = (uint64_t*)edge_batch_q[i].get<0>().c_str();
        node_dist = edge_batch_q[i].get<1>();
        stat_time_misc5_acc += Cycles::rdtsc() - stat_time_misc5_start;        

        stat_time_misc6_start = Cycles::rdtsc();
        for(int j = 0; j < adj_list_intarray[0]; j++) {
          uint64_t neighbor = adj_list_intarray[j+1];
          if(!seen_list[neighbor]) {
            seen_list.set(neighbor);
            
            node_distance_q.push(boost::tuple<uint64_t,uint64_t>(neighbor, node_dist+1)); 
            frontier_node_q.push(boost::tuple<uint64_t,uint64_t>(neighbor, node_dist+1));
          }
        }
        stat_time_misc6_acc += Cycles::rdtsc() - stat_time_misc6_start;
      }
      stat_time_misc4_acc += Cycles::rdtsc() - stat_time_misc4_start;

      stat_time_misc7_start = Cycles::rdtsc();
      stat_max_node_distance_q_size = std::max(stat_max_node_distance_q_size, node_distance_q.size());
      stat_max_frontier_node_q_size = std::max(stat_max_frontier_node_q_size, frontier_node_q.size());
      node_distance_q_condvar.notify_all();
      frontier_node_q_condvar.notify_all(); 
      stat_time_misc7_acc += Cycles::rdtsc() - stat_time_misc7_start;
    }
    stat_time_edge_trav_acc += Cycles::rdtsc() - stat_time_edge_trav_start; 
  }
  
  return 0;
}
