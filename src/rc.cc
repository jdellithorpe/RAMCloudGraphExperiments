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

  desc.add_options()
        ("cmd", po::value<string>(&cmd)->required(), "command to run")
        ("help", "produce help message")
        ("verbose", "print additional messages")
        ("table_name", po::value<string>(&table_name)->default_value("DefaultTable"), "name of table")
        ("server_span", po::value<uint64_t>(&server_span)->default_value(1), "server span");

  po::positional_options_description positionalOptions;
  positionalOptions.add("cmd", 1);
  positionalOptions.add("table_name", 1);
  positionalOptions.add("server_span", 1);

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

  switch(cmd_map[cmd]) {
  case cmd_rd:
    std::cout << "Executing read command...\n";
    std::cout << "Oops! Command not yet implemented, sorry!\n";
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
