#include "supersonic-macros.h"
#include "root-macros.h"
#include "csv-macros.h"

#include <iostream>
using std::cout;
using std::endl;

int
main(int argc, char *argv[])
{
  //try {
  $MAIN$
  //} catch (RawException *e) {
  //  cout << "Exception occurred: " << e->message() << endl;
  //}

  return 0;
}
