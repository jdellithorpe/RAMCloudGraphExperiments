#include <iostream>
#include "RamCloud.h"
#include "sandy.h"
#include "adjacency_list.pb.h"

using namespace RAMCloud;
using namespace Sandy::ProtoBuf;

int main(int argc, char* argv[]) {
  GOOGLE_PROTOBUF_VERIFY_VERSION;

  uint64_t tableid;
  string key;
  AdjacencyList adj_list_pb;
  RamCloud client(COORDINATOR_LOCATION);

  tableid = client.getTableId(argv[1]);
  key = argv[2];

  Buffer rc_read_buf;
  uint64_t buf_len;
  try {
    client.read(  tableid,
                  key.c_str(),
                  key.length(),
                  &rc_read_buf );
  } catch (RAMCloud::ClientException& e) {
    fprintf(stderr, "RAMCloud exception: %s\n", e.str().c_str());
    return 1;
  }

  buf_len = rc_read_buf.getTotalLength();
  adj_list_pb.ParseFromArray(rc_read_buf.getRange(0, buf_len), buf_len);

  std::cout << adj_list_pb.DebugString();

  return 0;
}
