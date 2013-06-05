#include <boost/lexical_cast.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/thread.hpp>
#include <iostream>
#include <sstream>
#include <queue>
#include <set>
#include "Cycles.h"
#include "ShortMacros.h"
#include "RamCloud.h"
#include "sandy.h"
#include "bfs.h"

using namespace RAMCloud;

/// Global program termination bit
bool terminate = false;

uint64_t stat_time_alg_start;
uint64_t stat_time_alg_acc = 0;
uint64_t stat_time_read_start;
uint64_t stat_time_read_acc = 0;
uint64_t stat_time_write_start;
uint64_t stat_time_write_acc = 0;
uint64_t stat_time_edge_trav_start;
uint64_t stat_time_edge_trav_acc = 0;
uint64_t stat_bytes_read_acc = 0;
uint64_t stat_nodes_read_acc = 0;
uint64_t stat_nodes_write_acc = 0;

uint64_t stat_time_pop_start;
uint64_t stat_time_pop_acc = 0;
uint64_t stat_time_boostsplit_start;
uint64_t stat_time_boostsplit_acc = 0;
uint64_t stat_time_neighbor_trav_start;
uint64_t stat_time_neighbor_trav_acc = 0;
uint64_t stat_time_seenlistinsert_start;
uint64_t stat_time_seenlistinsert_acc = 0;
uint64_t stat_time_push_start;
uint64_t stat_time_push_acc = 0;

uint64_t stat_time_reporter_start = 0;

/**
 * Report program statistics
 * Called just before program termination.
 */
void report_stats() {
  LOG(NOTICE, "Total time for algorithm execution:  %f seconds", Cycles::toSeconds(stat_time_alg_acc));
  LOG(NOTICE, "Total time for popping from queue:     %f seconds", Cycles::toSeconds(stat_time_pop_acc));
  LOG(NOTICE, "Total time for reading graph edges:    %f seconds", Cycles::toSeconds(stat_time_read_acc));
  LOG(NOTICE, "Total time for boostsplit:             %f seconds", Cycles::toSeconds(stat_time_boostsplit_acc));
  LOG(NOTICE, "Total time for edge traversal:         %f seconds", Cycles::toSeconds(stat_time_edge_trav_acc));
  LOG(NOTICE, "Total time for neighbor traversal:       %f seconds", Cycles::toSeconds(stat_time_neighbor_trav_acc));
  LOG(NOTICE, "Total time for writing node distances:     %f seconds", Cycles::toSeconds(stat_time_write_acc));
  LOG(NOTICE, "Total time for pushing into queue:         %f seconds", Cycles::toSeconds(stat_time_push_acc));
  LOG(NOTICE, "Total time for insertion into seen list:   %f seconds", Cycles::toSeconds(stat_time_seenlistinsert_acc));

  LOG(NOTICE, "Average processing time per node:  %lu nanoseconds", Cycles::toNanoseconds(stat_time_alg_acc/stat_nodes_read_acc));
  LOG(NOTICE, "Average queue pop time per node:     %lu nanoseconds", Cycles::toNanoseconds(stat_time_pop_acc/stat_nodes_read_acc));
  LOG(NOTICE, "Average time per node read:          %lu nanoseconds", Cycles::toNanoseconds(stat_time_read_acc/stat_nodes_read_acc));
  LOG(NOTICE, "Average time for boostsplit:         %lu nanoseconds", Cycles::toNanoseconds(stat_time_boostsplit_acc/stat_nodes_read_acc));
  LOG(NOTICE, "Average time for edge traversal:     %lu nanoseconds", Cycles::toNanoseconds(stat_time_edge_trav_acc/stat_nodes_read_acc));
  LOG(NOTICE, "Average time for a seen list check:    %lu nanoseconds", Cycles::toNanoseconds((stat_time_neighbor_trav_acc - stat_time_write_acc - stat_time_push_acc - stat_time_seenlistinsert_acc)/(5*stat_nodes_write_acc)));
  LOG(NOTICE, "Average time for distance write:         %lu nanoseconds", Cycles::toNanoseconds(stat_time_write_acc / stat_nodes_write_acc));
  LOG(NOTICE, "Average time for push into queue:        %lu nanoseconds", Cycles::toNanoseconds(stat_time_push_acc/stat_nodes_write_acc));
  LOG(NOTICE, "Average time for seen list insert:       %lu nanoseconds", Cycles::toNanoseconds(stat_time_seenlistinsert_acc/stat_nodes_write_acc));

  LOG(NOTICE, "Total bytes read from ramcloud: %lu bytes", stat_bytes_read_acc);
  LOG(NOTICE, "Total nodes read from ramcloud: %lu nodes", stat_nodes_read_acc);
  LOG(NOTICE, "Average bytes per node: %lu bytes/node", stat_bytes_read_acc/stat_nodes_read_acc);
}

/**
 * Main function of the reporter thread
 * Reports some stats in fixed intervals of time.
 */
void reporter() {
  stat_time_reporter_start = Cycles::rdtsc();
  while(true) {
    sleep(REPORTER_NAP_TIME);
    LOG(NOTICE, "-------------------- Reporter Output Start --------------------");
    report_stats();
    LOG(NOTICE, "Reporter time: %f seconds", Cycles::toSeconds(Cycles::rdtsc() - stat_time_reporter_start));
    LOG(NOTICE, "-------------------- Reporter Output Stop --------------------");
    if(terminate)
      return;
  }
}
 
int main(int argc, char* argv[]) {
  uint64_t graph_tableid;
  uint64_t dist_tableid;
  RamCloud client(COORDINATOR_LOCATION);
  std::queue<pair<string,int64_t>> node_queue;
  std::set<string> seen_list;
  string source(argv[1]);

  boost::thread reporter_thread(reporter);

  graph_tableid = client.getTableId(GRAPH_TABLE_NAME);
  dist_tableid = client.getTableId(DIST_TABLE_NAME);

  try {
    client.write( dist_tableid, 
                  source.c_str(), 
                  source.length(), 
                  (boost::lexical_cast<string>(0)).c_str() );
  } catch (RAMCloud::ClientException& e) {
    fprintf(stderr, "RAMCloud exception: %s\n", e.str().c_str());
    return 1;
  }

  node_queue.push(pair<string, int64_t>(source, 0));
  seen_list.insert(source);

  string node;
  int64_t dist;
  Buffer rc_read_buf;
  string rc_read_str;
  uint32_t buf_len;
  std::vector<string> edge_list;

  stat_time_alg_start = Cycles::rdtsc();
  while( !node_queue.empty() ) {
    stat_time_pop_start = Cycles::rdtsc();
    node = node_queue.front().first;
    dist = node_queue.front().second;
    node_queue.pop();
    stat_time_pop_acc += Cycles::rdtsc() - stat_time_pop_start;

    stat_time_read_start = Cycles::rdtsc();
    try {
      client.read(  graph_tableid,
                    node.c_str(),
                    node.length(),
                    &rc_read_buf );
    } catch (RAMCloud::ClientException& e) {
      continue;
    }
    stat_time_read_acc += Cycles::rdtsc() - stat_time_read_start;
    stat_nodes_read_acc++;
  
    buf_len = rc_read_buf.getTotalLength();
    rc_read_str = string(static_cast<const char*>(rc_read_buf.getRange(0, buf_len)), buf_len);
    
    stat_time_boostsplit_start = Cycles::rdtsc(); 
    boost::split(edge_list, rc_read_str, boost::is_any_of(" "));
    stat_time_boostsplit_acc += Cycles::rdtsc() - stat_time_boostsplit_start;

    stat_bytes_read_acc += buf_len;

    stat_time_edge_trav_start = Cycles::rdtsc();
    for(  std::vector<string>::iterator it = edge_list.begin(); 
          it != edge_list.end();
          ++it ) {
      stat_time_neighbor_trav_start = Cycles::rdtsc();
      if( seen_list.count(*it) == 0 ) {
        stat_time_write_start = Cycles::rdtsc();
        try {
          client.write( dist_tableid, 
                        (*it).c_str(), 
                        (*it).length(), 
                        (boost::lexical_cast<string>(dist+1)).c_str() );
        } catch (RAMCloud::ClientException& e) {
          fprintf(stderr, "RAMCloud exception: %s\n", e.str().c_str());
          return 1;
        }
        stat_time_write_acc += Cycles::rdtsc() - stat_time_write_start;
        stat_nodes_write_acc++;
    
        stat_time_push_start = Cycles::rdtsc();    
        node_queue.push(pair<string, int64_t>((*it), dist+1));
        stat_time_push_acc += Cycles::rdtsc() - stat_time_push_start;

        stat_time_seenlistinsert_start = Cycles::rdtsc();
        seen_list.insert(*it);
        stat_time_seenlistinsert_acc += Cycles::rdtsc() - stat_time_seenlistinsert_start;
      }
      stat_time_neighbor_trav_acc += Cycles::rdtsc() - stat_time_neighbor_trav_start;
    }
    stat_time_edge_trav_acc += Cycles::rdtsc() - stat_time_edge_trav_start;
  }
  stat_time_alg_acc += Cycles::rdtsc() - stat_time_alg_start;
 
  terminate = true;
  reporter_thread.join();
 
  return 0;
}
