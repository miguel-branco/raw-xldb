#include "common-macros.h"

#include <string>
using std::string;
#include <vector>
using std::vector;

#include <TApplication.h>
#include <TChain.h>

class RootHelper {
  public:
    RootHelper() {
      chain = new TChain("physics", "");
    }

    void addFileOrFail(const char* fname) {
      if (!chain->AddFile(fname)) {
        throw new RawException("root.chain->AddFile");
      }
    }

  TChain* chain;
};

#define ROOT_INIT()                                 TApplication App(argv[0], &argc, argv);
#define ROOT_FILE_ADD(rootId,file)                  RootHelper rootId;                                                                      \
                                                    rootId.addFileOrFail(file);
#define ROOT_FIELD_SET(rootId,field,type)           TBranch* branch_ ## rootId ## field;                                                    \
                                                    type rootId ## field;                                                                   \
                                                    rootId.chain->SetBranchAddress(#field, &rootId ## field, &branch_ ## rootId ## field);
#define ROOT_VECTOR_SET(rootId,field,type)          TBranch* branch_ ## rootId ## field;                                                    \
                                                    vector<type>* rootId ## field;                                                          \
                                                    rootId ## field = 0;                                                                    \
                                                    rootId.chain->SetBranchAddress(#field, &rootId ## field, &branch_ ## rootId ## field);
#define ROOT_OUTER_LOOP_BEGIN(rootId,id)            Long64_t rootId ## nentries = rootId.chain->GetEntries();                               \
                                                    for (Long64_t id = 0; id < rootId ## nentries; id++) {                      
#define ROOT_ROW_GET(rootId,id)                       rootId.chain->LoadTree(id);
#define ROOT_FIELD_GET(rootId,id,field)               branch_ ## rootId ## field->GetEntry(id);
#define ROOT_VECTOR_LOOP_BEGIN(rootId,id,field)       for (int id = 0; id < rootId ## field->size(); id++) {
#define ROOT_VECTOR_GET(rootId,id,type,field,loopId)    type rootId ## id = rootId ## field->at(loopId);
#define ROOT_VECTOR_LOOP_END()                        }                                                
#define ROOT_OUTER_LOOP_END()                       }
