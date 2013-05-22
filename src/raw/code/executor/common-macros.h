#ifndef COMMON_MACROS_H_
#define COMMON_MACROS_H_

#include <string>
using std::string;

class RawException {
  public:
    RawException(const string& message) : _message(message) { }
    const string& message() { return _message; }
  private:
    string _message;
};

#endif /* COMMON_MACROS_H_ */