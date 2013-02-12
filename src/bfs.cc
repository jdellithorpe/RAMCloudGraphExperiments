#include <iostream>
#include "RamCloud.h"

#define GRAPH_TABLE_NAME "graph"
#define COORDINATOR_LOCATION "fast+udp:host=127.0.0.1,port=12246"

using namespace RAMCloud;

int main(int argc, char* argv[]) {
  RamCloud client(COORDINATOR_LOCATION);
  
  client.createTable(GRAPH_TABLE_NAME);
  uint64_t table_id;
  table_id = client.getTableId(GRAPH_TABLE_NAME);

  std::cout << "got table id: " << table_id << "\n";

  client.dropTable(GRAPH_TABLE_NAME);

  return 0;
}
