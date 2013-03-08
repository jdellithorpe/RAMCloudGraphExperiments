#include <boost/lexical_cast.hpp>
#include <boost/algorithm/string.hpp>
#include <iostream>
#include <sstream>
#include <queue>
#include <set>
#include "Cycles.h"
#include "ShortMacros.h"
#include "RamCloud.h"
#include "sandy.h"

using namespace RAMCloud;

int main(int argc, char* argv[]) {
  uint64_t graph_tableid;
  uint64_t dist_tableid;
  RamCloud client(COORDINATOR_LOCATION);
  std::queue<pair<string,int64_t>> node_queue;
  std::set<string> seen_list;
  string source(argv[1]);
  uint64_t stat_time_alg_start;
  uint64_t stat_time_read_start;
  uint64_t stat_time_read_acc;
  uint64_t stat_time_write_start;
  uint64_t stat_time_write_acc;
  uint64_t stat_bytes_read_acc;
  uint64_t stat_nodes_read_acc;
  uint64_t stat_nodes_write_acc;

  client.dropTable(DIST_TABLE_NAME);
  client.createTable(DIST_TABLE_NAME);

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

  stat_time_read_acc = 0;
  stat_time_write_acc = 0;
  stat_bytes_read_acc = 0;
  stat_nodes_read_acc = 0;
  stat_nodes_write_acc = 0;

  stat_time_alg_start = Cycles::rdtsc();
  while( !node_queue.empty() ) {
    node = node_queue.front().first;
    dist = node_queue.front().second;
    node_queue.pop();

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

    stat_bytes_read_acc += buf_len;

    rc_read_str = string(static_cast<const char*>(rc_read_buf.getRange(0, buf_len)), buf_len);
    boost::split(edge_list, rc_read_str, boost::is_any_of(" "));
    
    for(  std::vector<string>::iterator it = edge_list.begin(); 
          it != edge_list.end();
          ++it ) {
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
        
        node_queue.push(pair<string, int64_t>((*it), dist+1));
        seen_list.insert(*it);
      }
    }
  }
  LOG(NOTICE, "Total time for algorithm execution: %f seconds", Cycles::toSeconds(Cycles::rdtsc() - stat_time_alg_start));
  LOG(NOTICE, "Total time for reading graph edges: %f seconds", Cycles::toSeconds(stat_time_read_acc));
  LOG(NOTICE, "Total time for writing node distances: %f seconds", Cycles::toSeconds(stat_time_write_acc));
  LOG(NOTICE, "Total bytes read from ramcloud: %lu bytes", stat_bytes_read_acc);
  LOG(NOTICE, "Total nodes read from ramcloud: %lu nodes", stat_nodes_read_acc);
  LOG(NOTICE, "Average bytes per node: %lu bytes/node", stat_bytes_read_acc/stat_nodes_read_acc);
  LOG(NOTICE, "Average time per node read: %lu microseconds", Cycles::toMicroseconds(stat_time_read_acc / stat_nodes_read_acc));
  LOG(NOTICE, "Average time for distance write: %lu microseconds", Cycles::toMicroseconds(stat_time_write_acc / stat_nodes_write_acc));

  return 0;
}
