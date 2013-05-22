#include <boost/algorithm/string.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/filesystem.hpp>
#include <boost/program_options.hpp>
#include <iostream>
#include <fstream>
#include <map>
#include "RamCloud.h"
#include "sandy.h"
#include "adjacency_list.pb.h"

using namespace RAMCloud;
using namespace Sandy::ProtoBuf;

enum Command {  cmd_undefined, 
                cmd_rd,
                cmd_wr,
                cmd_ct,
                cmd_dt };

int main(int argc, char* argv[]) {
  GOOGLE_PROTOBUF_VERIFY_VERSION;

  // Build mapping from command string to command number.
  std::map<std::string, Command> cmd_map;
  cmd_map["rd"] = cmd_rd;
  cmd_map["wr"] = cmd_wr;
  cmd_map["ct"] = cmd_ct;
  cmd_map["dt"] = cmd_dt;

  // Parse command line arguments.
  namespace fs = boost::filesystem;
  namespace po = boost::program_options;

  string prog_name = fs::basename(argv[0]);

  po::options_description desc("Allowed options");

  bool verbose = false;
  string cmd;
  string table_name;
  uint64_t server_span;
  string key;
  string value;
  string format;

  desc.add_options()
        ("cmd", po::value<string>(&cmd)->required(), "command to run")
        ("help,h", "produce help message")
        ("verbose,v", "print additional messages")
        ("table_name,t", po::value<string>(&table_name)->default_value("DefaultTable"), "name of table (default 'DefaultTable')")
        ("server_span,s", po::value<uint64_t>(&server_span)->default_value(1), "server span (default 1)")
        ("key,k", po::value<string>(&key)->default_value("DefaultKey"), "table key (default 'DefaultKey')")
        ("value,v", po::value<string>(&value)->default_value("DefaultValue"), "table value (default 'DefaultValue')")
        ("format,f", po::value<string>(&format)->default_value("DefaultFormat"), "ramcloud object format (default 'DefaultFormat')");

  po::positional_options_description positionalOptions;
  positionalOptions.add("cmd", 1);

  po::variables_map vm;

  try {
    po::store(po::command_line_parser(argc, argv).options(desc).positional(positionalOptions).run(), vm);

    po::notify(vm);
  } catch(po::required_option& e) {
    std::cout << "ERROR: " << e.what() << std::endl << std::endl;
    std::cout << desc << "\n";
    return -1;
  } catch(po::error& e) {
    std::cout << "ERROR: " << e.what() << std::endl << std::endl;
    std::cout << desc << "\n";
    return -1;
  }
  
  RamCloud client(COORDINATOR_LOCATION);
  uint64_t table_id;
  uint64_t buf_len;
  Buffer read_buf;
  AdjacencyList adj_list_pb; 

  switch(cmd_map[cmd]) {
  case cmd_rd:
    std::cout << "Executing read command...\n";
    
    table_id = client.getTableId(table_name.c_str());
  
    if(format == "v3") {
      uint64_t key_int = boost::lexical_cast<uint64_t>(key);
      try {
        client.read(  table_id,
                      (char*)&key_int,
                      sizeof(uint64_t),
                      &read_buf );
      } catch (RAMCloud::ClientException& e) {
        fprintf(stderr, "RAMCloud exception: %s\n", e.str().c_str());
        return 1;
      }
      buf_len = read_buf.getTotalLength();
      adj_list_pb.ParseFromArray(read_buf.getRange(0, buf_len), buf_len);
      std::cout << adj_list_pb.DebugString();      
    } else if(format == "v2") {
      try {
        client.read(  table_id,
                      key.c_str(),
                      key.length(),
                      &read_buf );
      } catch (RAMCloud::ClientException& e) {
        fprintf(stderr, "RAMCloud exception: %s\n", e.str().c_str());
        return 1;
      }
      buf_len = read_buf.getTotalLength();
      adj_list_pb.ParseFromArray(read_buf.getRange(0, buf_len), buf_len);
      std::cout << adj_list_pb.DebugString();
    } else if(format == "v1") {
      try {
        client.read(  table_id,
                      key.c_str(),
                      key.length(),
                      &read_buf );
      } catch (RAMCloud::ClientException& e) {
        fprintf(stderr, "RAMCloud exception: %s\n", e.str().c_str());
        return 1;
      }
      buf_len = read_buf.getTotalLength();
      std::cout << string(static_cast<const char*>(read_buf.getRange(0, buf_len)), buf_len) << "\n";
    } else {
      std::cout << "Oops! Format " << format << " not implemented yet!\n";
    }
    break;
  case cmd_wr:
    std::cout << "Executing write command...\n";
    std::cout << "Oops! Command not yet implemented, sorry!\n";
    break;
  case cmd_ct:
    std::cout << "Executing create table command...\n";
    client.createTable(table_name.c_str(), server_span);
    std::cout << "Created table with the following properties:\n";
    std::cout << "\tTable Name:\t" << table_name << "\n";
    std::cout << "\tServer Span:\t" << server_span << "\n";
    break;
  case cmd_dt:
    std::cout << "Executing drop table command...\n";
    client.dropTable(table_name.c_str());
    std::cout << "Dropped table '" << table_name << "'\n";
    break;
  default:
    std::cout << "'" << cmd << "' is not a valid command\n";
    break;
  }

  return 0;
}
