#include "common-macros.h"

#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>

// FIXME: Does not handle error conditions.
class CsvHelper {
  public:
    CsvHelper(const string& file) : pos(0) {
      const char *fname = file.c_str();
      
      struct stat statbuf;    
      stat(fname, &statbuf);
      fsize = statbuf.st_size;

      fd = open(fname, O_RDONLY);
      if (fd == -1) {
        throw new RawException("csv.open");
      }
  
      buf = (char*) mmap(NULL, fsize, PROT_READ, MAP_PRIVATE | MAP_POPULATE, fd, 0);
      if (buf == MAP_FAILED) {
        throw new RawException("csv.mmap");
      }
    }
  
    ~CsvHelper() {
      // FIXME: unmmap
      close(fd);
    }
  
    void skip() {
      while (pos < fsize && buf[pos] != ';' && buf[pos] != '\n') {
        pos++;
      }
      pos++;
    }

    int readAsInt() {
      int start = pos;
      skip();
      return atoi(buf + start);      
    }
    
    int eof() {
      return (pos >= fsize);
    }

  private:
    off_t fsize;
    int fd;
    char *buf;
    off_t pos;
};



#define CSV_INIT(id,file)                 CsvHelper csv_##id(file);
#define CSV_OUTER_LOOP_BEGIN(id)          while (!csv_##id.eof()) {
#define CSV_FIELD_SKIP(id)                  csv_##id.skip();
#define CSV_FIELD_READ_AS_INT(id,field)     int field = csv_##id.readAsInt();           
#define CSV_OUTER_LOOP_END()              }
// FIXME: Add CSV_DESTROY(file)