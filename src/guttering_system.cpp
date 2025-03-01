#include "guttering_system.h"

#include <string>
#include <unistd.h> //sysconf
#include <fstream>

uint32_t GutteringSystem::page_size;
uint32_t GutteringSystem::buffer_size;
uint32_t GutteringSystem::fanout;
uint32_t GutteringSystem::queue_factor;
uint32_t GutteringSystem::num_flushers;
float GutteringSystem::gutter_factor;

void GutteringSystem::configure_system() {
  // some default values
  uint32_t buffer_exp  = 20;
  uint16_t branch      = 64;
  int queue_f          = 8;
  int page_factor      = 1;
  int n_fushers        = 1;
  float gutter_f       = 1;

  // parse the configuration file
  std::string line;
  std::ifstream conf("./buffering.conf");
  if (conf.is_open()) {
    while(getline(conf, line)) {
      if (line[0] == '#' || line[0] == '\n') continue;
      else if(line.substr(0, line.find('=')) == "buffer_exp") {
        buffer_exp  = std::stoi(line.substr(line.find('=') + 1));
        if (buffer_exp > 30 || buffer_exp < 10) {
          printf("WARNING: buffer_exp out of bounds [10,30] using default(20)\n");
          buffer_exp = 20;
        }
      }
      else if(line.substr(0, line.find('=')) == "branch") {
        branch = std::stoi(line.substr(line.find('=') + 1));
        if (branch > 2048 || branch < 2) {
          printf("WARNING: branch out of bounds [2,2048] using default(64)\n");
          branch = 64;
        }
      }
      else if(line.substr(0, line.find('=')) == "queue_factor") {
        queue_f = std::stoi(line.substr(line.find('=') + 1));
        if (queue_f > 1024 || queue_f < 1) {
          printf("WARNING: queue_factor out of bounds [1,1024] using default(8)\n");
          queue_f = 2;
        }
      }
      else if(line.substr(0, line.find('=')) == "page_factor") {
        page_factor = std::stoi(line.substr(line.find('=') + 1));
        if (page_factor > 50 || page_factor < 1) {
          printf("WARNING: page_factor out of bounds [1,50] using default(1)\n");
          page_factor = 1;
        }
      }
      else if(line.substr(0, line.find('=')) == "num_threads") {
        n_fushers = std::stoi(line.substr(line.find('=') + 1));
        if (n_fushers > 20 || n_fushers < 1) {
          printf("WARNING: num_threads out of bounds [1,20] using default(1)\n");
          n_fushers = 1;
        }
      }
      else if(line.substr(0, line.find('=')) == "gutter_factor") {
        gutter_f = std::stof(line.substr(line.find('=') + 1));
        if (gutter_f < 1 && gutter_f > -1) {
          printf("WARNING: gutter_factor must be outside of range -1 < x < 1 using default(1)\n");
          gutter_f = 1;
        }
      }
    }
  } else {
    printf("WARNING: Could not open buffering configuration file! Using default setttings.\n");
  }
  buffer_size = 1 << buffer_exp;
  fanout = branch;
  queue_factor = queue_f;
  num_flushers = n_fushers;
  page_size = page_factor * sysconf(_SC_PAGE_SIZE); // works on POSIX systems (alternative is boost)
  // Windows may need https://docs.microsoft.com/en-us/windows/win32/api/sysinfoapi/nf-sysinfoapi-getnativesysteminfo?redirectedfrom=MSDN

  gutter_factor = gutter_f;
  if (gutter_factor < 0)
    gutter_factor = 1 / (-1 * gutter_factor); // gutter factor reduces size if negative
}
