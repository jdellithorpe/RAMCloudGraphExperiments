#include <iostream>
#include <sstream>
#include <queue>
#include <set>
#include "RamCloud.h"

#define GRAPH_TABLE_NAME "graph"
#define COORDINATOR_LOCATION "infrc:host=192.168.1.101,port=12246"

using namespace RAMCloud;

int main(int argc, char* argv[]) { 
  uint64_t graph_tableid;

  RamCloud client(COORDINATOR_LOCATION);
 
  graph_tableid = client.getTableId(GRAPH_TABLE_NAME);

  std::queue<pair<string,int>> queue;
  std::set<string> seen_list;

  Buffer buffer;
  string u(argv[1]);
  uint32_t dist_to_u = 0;
  queue.push(pair<string, int>(u, dist_to_u)); 
  seen_list.insert(u);
  while( !queue.empty() ) {
    u = queue.front().first;
    dist_to_u = queue.front().second;
    std::stringstream ss;
    ss << dist_to_u;
    string dist_to_u_str = ss.str();
    queue.pop();

    try {
      client.read(graph_tableid, u.c_str(), u.length(), &buffer);
      
      string edge_list(static_cast<const char*>(buffer.getRange(0, buffer.getTotalLength())), buffer.getTotalLength());
    
      std::istringstream iss(edge_list);
    
      while(iss) {
        string v;
        iss >> v;
        if( v != "-1" ) {
          //std::cout << v << "\n";
          if( seen_list.count(v) == 0 ) {
            queue.push(pair<string, int>(v, dist_to_u + 1));
            seen_list.insert(v);
          }
        } else {
          //std::cout << "writing: " << dist_to_u_str << "\n";
          client.write(graph_tableid, u.c_str(), u.length(), edge_list.append(" " + dist_to_u_str).c_str());
          break;
        }
      }
    } catch (RAMCloud::ClientException& e) {
      //fprintf(stderr, "RAMCloud exception: %s\n", e.str().c_str());
      client.write(graph_tableid, u.c_str(), u.length(), dist_to_u_str.c_str());
    } catch (RAMCloud::Exception& e) {
      //fprintf(stderr, "RAMCloud exception: %s\n", e.str().c_str());
      client.write(graph_tableid, u.c_str(), u.length(), dist_to_u_str.c_str());
    }
  }

  // finally, drop the table
  //client.dropTable(GRAPH_TABLE_NAME);

  return 0;
}
